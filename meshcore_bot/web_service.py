from __future__ import annotations

import asyncio
import copy
import hmac
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from .config import AppConfig, load_config, load_raw_config, save_raw_config
from .database import BotDatabase
from .probe_service import probe_wakeup_socket_path


MANUAL_WEB_PROBE_REASON = "manual web fetch"
MANUAL_WEB_PROBE_MAX_PER_WINDOW = 2
MANUAL_WEB_PROBE_WINDOW_SECS = 3600.0
MANUAL_WEB_PROBE_MIN_SUCCESS_COOLDOWN_SECS = 60.0
STATE_CACHE_IDLE_TTL_SECS = 300.0
STATE_CACHE_ACTIVE_TTL_SECS = 15.0
STATE_SHARED_CACHE_IDLE_TTL_SECS = 60
STATE_SHARED_CACHE_ACTIVE_TTL_SECS = 15
MANAGEMENT_CACHE_IDLE_TTL_SECS = 900.0
MANAGEMENT_CACHE_ACTIVE_TTL_SECS = 60.0
MANAGEMENT_SHARED_CACHE_IDLE_TTL_SECS = 300
MANAGEMENT_SHARED_CACHE_ACTIVE_TTL_SECS = 60
STATE_CACHE_STALE_WHILE_REVALIDATE_SECS = 300
ADMIN_COOKIE_NAME = "meshcore_admin"
ADMIN_COOKIE_MAX_AGE_SECS = 12 * 60 * 60
ADMIN_NODE_LIMIT = 300
ADMIN_PROBE_JOB_LIMIT = 120
ADMIN_PACKET_LOG_LIMIT = 80
ADMIN_LOG_FILE_LIMIT = 6
ADMIN_LOG_TAIL_LINES = 160


class ProbeJobCreatePayload(BaseModel):
    repeater_id: int


class AdminLoginPayload(BaseModel):
  username: str
  password: str


class AdminCleanupPayload(BaseModel):
  failed_older_than_hours: float = 24.0


@dataclass(slots=True)
class _StateSnapshot:
    payload_bytes: bytes
    etag: str
    generated_monotonic: float
    ttl_secs: float
    cache_control: str


class _StateSnapshotCache:
    def __init__(
        self,
        *,
        payload_builder: Callable[[BotDatabase], dict[str, object]],
        ttl_builder: Callable[[dict[str, object]], float],
        cache_control_builder: Callable[[float], str],
    ) -> None:
        self._snapshot: _StateSnapshot | None = None
        self._lock = asyncio.Lock()
        self._payload_builder = payload_builder
        self._ttl_builder = ttl_builder
        self._cache_control_builder = cache_control_builder

    def invalidate(self) -> None:
        self._snapshot = None

    async def get_snapshot(self, database: BotDatabase) -> _StateSnapshot:
        snapshot = self._snapshot
        if self._is_fresh(snapshot):
            return snapshot

        async with self._lock:
            snapshot = self._snapshot
            if self._is_fresh(snapshot):
                return snapshot

            payload = self._payload_builder(database)
            payload_bytes = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
            ttl_secs = self._ttl_builder(payload)
            self._snapshot = _StateSnapshot(
                payload_bytes=payload_bytes,
                etag=f'"{hashlib.sha256(payload_bytes).hexdigest()}"',
                generated_monotonic=time.monotonic(),
                ttl_secs=ttl_secs,
                cache_control=self._cache_control_builder(ttl_secs),
            )
            return self._snapshot

    def _is_fresh(self, snapshot: _StateSnapshot | None) -> bool:
        if snapshot is None:
            return False
        return (time.monotonic() - snapshot.generated_monotonic) < snapshot.ttl_secs


def _build_state_payload(database: BotDatabase) -> dict[str, object]:
    return {
        "overview": database.snapshot_overview(),
        "nodes": database.list_repeaters_for_web(),
        "probe_jobs": database.list_probe_jobs(limit=100),
    }


def _build_management_payload(database: BotDatabase, *, include_historical: bool = False) -> dict[str, object]:
    probe_jobs = database.list_probe_jobs(limit=100)
    payload: dict[str, object] = {
        "has_active_probe_jobs": _probe_jobs_have_active_entries(probe_jobs),
        "map_links": database.latest_repeater_neighbor_links(limit_repeaters=128),
        "route_hints": database.repeater_route_hints(limit_repeaters=128),
    }
    if include_historical:
        payload["historical_links"] = database.repeater_historical_neighbor_links(limit_repeaters=128)
    return payload


def _probe_jobs_have_active_entries(probe_jobs: object) -> bool:
    if not isinstance(probe_jobs, list):
        return False
    for job in probe_jobs:
        if not isinstance(job, dict):
            continue
        if str(job.get("status") or "") in {"pending", "running"}:
            return True
    return False


def _state_cache_ttl_secs(payload: dict[str, object]) -> float:
    return STATE_CACHE_ACTIVE_TTL_SECS if _probe_jobs_have_active_entries(payload.get("probe_jobs")) else STATE_CACHE_IDLE_TTL_SECS


def _management_cache_ttl_secs(payload: dict[str, object]) -> float:
    if bool(payload.get("has_active_probe_jobs")):
        return MANAGEMENT_CACHE_ACTIVE_TTL_SECS
    return MANAGEMENT_CACHE_IDLE_TTL_SECS


def _snapshot_cache_control(
    ttl_secs: float,
    *,
    active_ttl_secs: float,
    shared_cache_active_ttl_secs: int,
    shared_cache_idle_ttl_secs: int,
) -> str:
    shared_ttl = shared_cache_active_ttl_secs if ttl_secs <= active_ttl_secs else shared_cache_idle_ttl_secs
    return (
        "public, max-age=0, must-revalidate, "
        f"s-maxage={shared_ttl}, stale-while-revalidate={STATE_CACHE_STALE_WHILE_REVALIDATE_SECS}"
    )


def _state_cache_control(ttl_secs: float) -> str:
    return _snapshot_cache_control(
        ttl_secs,
        active_ttl_secs=STATE_CACHE_ACTIVE_TTL_SECS,
        shared_cache_active_ttl_secs=STATE_SHARED_CACHE_ACTIVE_TTL_SECS,
        shared_cache_idle_ttl_secs=STATE_SHARED_CACHE_IDLE_TTL_SECS,
    )


def _management_cache_control(ttl_secs: float) -> str:
    return _snapshot_cache_control(
        ttl_secs,
        active_ttl_secs=MANAGEMENT_CACHE_ACTIVE_TTL_SECS,
        shared_cache_active_ttl_secs=MANAGEMENT_SHARED_CACHE_ACTIVE_TTL_SECS,
        shared_cache_idle_ttl_secs=MANAGEMENT_SHARED_CACHE_IDLE_TTL_SECS,
    )


def _etag_matches(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match:
        return False
    for candidate in if_none_match.split(","):
        normalized = candidate.strip()
        if normalized == "*":
            return True
        if normalized.startswith("W/"):
            normalized = normalized[2:].strip()
        if normalized == etag:
            return True
    return False


def _enabled_endpoint_names(config: AppConfig) -> list[str]:
    return [endpoint.name for endpoint in config.endpoints if endpoint.enabled]


def _resolve_manual_probe_endpoint_name(
    *,
    config: AppConfig,
    database: BotDatabase,
    repeater_id: int,
    repeater_name: str | None,
) -> str | None:
    normalized_repeater_name = str(repeater_name or "").strip()
    if normalized_repeater_name:
        for endpoint in config.endpoints:
            if not endpoint.enabled:
                continue
            if str(endpoint.local_node_name or "").strip() == normalized_repeater_name:
                return endpoint.name

    enabled_names = _enabled_endpoint_names(config)
    if not enabled_names:
        return None

    candidate_names = database.recommended_repeater_endpoint_names(
        repeater_id=repeater_id,
        endpoint_names=enabled_names,
    )
    for candidate_name in candidate_names:
        if candidate_name in enabled_names:
            return candidate_name

    return enabled_names[0]


def _active_probe_job_for_repeater(database: BotDatabase, *, repeater_id: int) -> dict[str, object] | None:
    jobs = database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=8)
    active_jobs = [job for job in jobs if str(job.get("status") or "") in {"pending", "running"}]
    if not active_jobs:
        return None
    active_jobs.sort(key=lambda job: (str(job.get("scheduled_at") or ""), int(job.get("id") or 0)))
    return active_jobs[0]


async def _notify_probe_worker(config: AppConfig) -> bool:
    socket_path = probe_wakeup_socket_path(config)
    try:
        _, writer = await asyncio.open_unix_connection(str(socket_path))
    except OSError:
        return False
    writer.write(b"wake\n")
    try:
        await writer.drain()
    except OSError:
        writer.close()
        return False
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        return False
    return True


def _parse_iso_datetime(value: object) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _manual_probe_schedule_at(database: BotDatabase, *, config: AppConfig) -> str | None:
    spacing_secs = max(
        float(config.probe.advert_probe_min_interval_secs),
        MANUAL_WEB_PROBE_MIN_SUCCESS_COOLDOWN_SECS,
    )
    if spacing_secs <= 0:
        return None

    latest_reference_at: datetime | None = None
    for job in database.list_probe_jobs(limit=256):
        reason = str(job.get("reason") or "")
        if not reason.startswith("manual "):
            continue
        status = str(job.get("status") or "")
        if status == "pending":
            reference_at = _parse_iso_datetime(job.get("scheduled_at"))
        elif status == "running":
            reference_at = _parse_iso_datetime(job.get("started_at")) or _parse_iso_datetime(job.get("scheduled_at"))
        else:
            reference_at = (
                _parse_iso_datetime(job.get("finished_at"))
                or _parse_iso_datetime(job.get("started_at"))
                or _parse_iso_datetime(job.get("scheduled_at"))
            )
        if reference_at is None:
            continue
        if latest_reference_at is None or reference_at > latest_reference_at:
            latest_reference_at = reference_at

    if latest_reference_at is None:
        return None

    scheduled_at = latest_reference_at + timedelta(seconds=spacing_secs)
    now = datetime.now(tz=UTC)
    if scheduled_at <= now:
        return None
    return scheduled_at.isoformat()


def _current_admin_username(config: AppConfig) -> str:
    return config.web.admin_username.strip() or "admin"


def _admin_cookie_value(config: AppConfig) -> str:
    token_seed = f"{_current_admin_username(config)}\0{config.web.admin_password}\0{config.service.name}\0admin"
    return hashlib.sha256(token_seed.encode("utf-8")).hexdigest()


def _request_uses_https(request: Request) -> bool:
    forwarded_proto = str(request.headers.get("x-forwarded-proto", "")).split(",", 1)[0].strip().lower()
    return forwarded_proto == "https" or request.url.scheme == "https"


def _is_admin_authenticated(request: Request, config: AppConfig) -> bool:
    cookie = request.cookies.get(ADMIN_COOKIE_NAME)
    if not cookie:
      return False
    return hmac.compare_digest(cookie, _admin_cookie_value(config))


def _require_admin(request: Request, config: AppConfig) -> None:
    if not _is_admin_authenticated(request, config):
      raise HTTPException(status_code=401, detail="admin auth required")


def _set_admin_cookie(response: JSONResponse, request: Request, config: AppConfig) -> None:
    response.set_cookie(
      key=ADMIN_COOKIE_NAME,
      value=_admin_cookie_value(config),
      max_age=ADMIN_COOKIE_MAX_AGE_SECS,
      httponly=True,
      samesite="lax",
      secure=_request_uses_https(request),
      path="/",
    )


def _clear_admin_cookie(response: JSONResponse) -> None:
    response.delete_cookie(key=ADMIN_COOKIE_NAME, path="/", httponly=True, samesite="lax")


def _admin_session_payload(config: AppConfig) -> dict[str, object]:
    return {"authenticated": True, "username": _current_admin_username(config)}


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
      return value
    if isinstance(value, (int, float)):
      return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_required_text(value: object, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
      raise ValueError(f"missing {field_name}")
    return normalized


def _normalize_optional_text(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _normalize_int(value: object, field_name: str) -> int:
    try:
      return int(value)
    except (TypeError, ValueError) as exc:
      raise ValueError(f"invalid integer for {field_name}") from exc


def _normalize_float(value: object, field_name: str) -> float:
    try:
      return float(value)
    except (TypeError, ValueError) as exc:
      raise ValueError(f"invalid number for {field_name}") from exc


def _normalize_string_list(value: object) -> list[str]:
    if value is None:
      return []
    if isinstance(value, list):
      raw_items = value
    else:
      raw_items = str(value).replace("\n", ",").split(",")
    return [str(item).strip() for item in raw_items if str(item).strip()]


def _section_dict(raw_config: dict[str, object], section_name: str) -> dict[str, object]:
    existing = raw_config.get(section_name)
    if existing is None:
      existing = {}
      raw_config[section_name] = existing
    if not isinstance(existing, dict):
      raise ValueError(f"invalid config section: {section_name}")
    return existing


def _normalize_endpoint_config_entry(entry: object) -> dict[str, object]:
    if not isinstance(entry, dict):
      raise ValueError("endpoint entry must be an object")
    normalized: dict[str, object] = {
      "name": _normalize_required_text(entry.get("name"), "endpoint.name"),
      "raw_host": _normalize_required_text(entry.get("raw_host"), "endpoint.raw_host"),
      "raw_port": _normalize_int(entry.get("raw_port", 5002), "endpoint.raw_port"),
      "enabled": _normalize_bool(entry.get("enabled", True)),
    }
    console_port = entry.get("console_port")
    if console_port not in (None, ""):
      normalized["console_port"] = _normalize_int(console_port, "endpoint.console_port")
    local_node_name = _normalize_optional_text(entry.get("local_node_name"))
    if local_node_name:
      normalized["local_node_name"] = local_node_name
    console_mirror_host = _normalize_optional_text(entry.get("console_mirror_host"))
    if console_mirror_host:
      normalized["console_mirror_host"] = console_mirror_host
    console_mirror_port = entry.get("console_mirror_port")
    if console_mirror_port not in (None, ""):
      normalized["console_mirror_port"] = _normalize_int(console_mirror_port, "endpoint.console_mirror_port")
    return normalized


def _apply_admin_config_payload(raw_config: dict[str, object], payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
      raise ValueError("invalid config payload")

    updated = copy.deepcopy(raw_config)

    if isinstance(payload.get("service"), dict):
      service = _section_dict(updated, "service")
      service_payload = payload["service"]
      if "name" in service_payload:
        service["name"] = _normalize_required_text(service_payload.get("name"), "service.name")
      if "log_level" in service_payload:
        service["log_level"] = _normalize_required_text(service_payload.get("log_level"), "service.log_level")

    if isinstance(payload.get("web"), dict):
      web = _section_dict(updated, "web")
      web_payload = payload["web"]
      if "host" in web_payload:
        web["host"] = _normalize_required_text(web_payload.get("host"), "web.host")
      if "port" in web_payload:
        web["port"] = _normalize_int(web_payload.get("port"), "web.port")
      if "admin_username" in web_payload:
        web["admin_username"] = _normalize_required_text(web_payload.get("admin_username"), "web.admin_username")
      if "admin_password" in web_payload:
        web["admin_password"] = str(web_payload.get("admin_password") or "")

    if isinstance(payload.get("bot"), dict):
      bot = _section_dict(updated, "bot")
      bot_payload = payload["bot"]
      if "enabled" in bot_payload:
        bot["enabled"] = _normalize_bool(bot_payload.get("enabled"))
      if "sender_name" in bot_payload:
        bot["sender_name"] = str(bot_payload.get("sender_name") or "").strip()
      if "reply_endpoint_name" in bot_payload:
        reply_endpoint_name = _normalize_optional_text(bot_payload.get("reply_endpoint_name"))
        if reply_endpoint_name is None:
          bot.pop("reply_endpoint_name", None)
        else:
          bot["reply_endpoint_name"] = reply_endpoint_name
      if "channels" in bot_payload:
        bot["channels"] = _normalize_string_list(bot_payload.get("channels"))
      if "enabled_commands" in bot_payload:
        bot["enabled_commands"] = _normalize_string_list(bot_payload.get("enabled_commands"))
      if "min_response_delay_secs" in bot_payload:
        bot["min_response_delay_secs"] = _normalize_float(
          bot_payload.get("min_response_delay_secs"), "bot.min_response_delay_secs"
        )
      if "response_attempts" in bot_payload:
        bot["response_attempts"] = _normalize_int(bot_payload.get("response_attempts"), "bot.response_attempts")
      if "response_attempts_max" in bot_payload:
        bot["response_attempts_max"] = _normalize_int(
          bot_payload.get("response_attempts_max"), "bot.response_attempts_max"
        )
      if "quiet_window_secs" in bot_payload:
        bot["quiet_window_secs"] = _normalize_float(bot_payload.get("quiet_window_secs"), "bot.quiet_window_secs")
      if "command_dedup_ttl_secs" in bot_payload:
        bot["command_dedup_ttl_secs"] = _normalize_float(
          bot_payload.get("command_dedup_ttl_secs"), "bot.command_dedup_ttl_secs"
        )
      if "include_test_signal" in bot_payload:
        bot["include_test_signal"] = _normalize_bool(bot_payload.get("include_test_signal"))

    if isinstance(payload.get("probe"), dict):
      probe = _section_dict(updated, "probe")
      probe_payload = payload["probe"]
      string_fields = {
        "admin_password",
        "guest_password",
        "default_guest_password",
        "pre_login_advert_name",
      }
      float_fields = {
        "poll_interval_secs",
        "request_timeout_secs",
        "route_freshness_secs",
        "scheduled_reprobe_interval_secs",
        "scheduled_reprobe_seen_within_secs",
        "night_failed_retry_interval_secs",
        "advert_probe_min_interval_secs",
        "advert_reprobe_failure_cooldown_secs",
      }
      int_fields = {
        "scheduled_reprobe_max_batch",
        "night_failed_retry_max_batch",
      }
      for field_name in string_fields:
        if field_name in probe_payload:
          probe[field_name] = str(probe_payload.get(field_name) or "")
      for field_name in float_fields:
        if field_name in probe_payload:
          probe[field_name] = _normalize_float(probe_payload.get(field_name), f"probe.{field_name}")
      for field_name in int_fields:
        if field_name in probe_payload:
          probe[field_name] = _normalize_int(probe_payload.get(field_name), f"probe.{field_name}")

    if isinstance(payload.get("gateway"), dict):
      gateway = _section_dict(updated, "gateway")
      gateway_payload = payload["gateway"]
      for field_name in {"traffic_watchdog_secs", "close_timeout_secs", "console_probe_timeout_secs"}:
        if field_name in gateway_payload:
          gateway[field_name] = _normalize_float(gateway_payload.get(field_name), f"gateway.{field_name}")

    if "endpoints" in payload:
      endpoints_payload = payload.get("endpoints")
      if not isinstance(endpoints_payload, list):
        raise ValueError("endpoints must be a list")
      updated["endpoints"] = [_normalize_endpoint_config_entry(item) for item in endpoints_payload]

    return updated


def _save_validated_config(config_path: Path, raw_config: dict[str, object]) -> AppConfig:
    temp_path: Path | None = None
    try:
      with NamedTemporaryFile("w", suffix=".toml", dir=str(config_path.parent), delete=False, encoding="utf-8") as handle:
        temp_path = Path(handle.name)
      save_raw_config(temp_path, raw_config)
      validated_config = load_config(temp_path)
      save_raw_config(config_path, raw_config)
      return validated_config
    finally:
      if temp_path is not None:
        try:
          temp_path.unlink()
        except OSError:
          pass


def _admin_config_payload(config: AppConfig, *, config_path: Path) -> dict[str, object]:
    return {
      "config_path": str(config_path),
      "service": {
        "name": config.service.name,
        "log_level": config.service.log_level,
      },
      "web": {
        "host": config.web.host,
        "port": config.web.port,
        "admin_username": _current_admin_username(config),
        "admin_password": config.web.admin_password,
      },
      "bot": {
        "enabled": config.bot.enabled,
        "sender_name": config.bot.sender_name,
        "reply_endpoint_name": config.bot.reply_endpoint_name or "",
        "channels": list(config.bot.channels),
        "enabled_commands": list(config.bot.enabled_commands),
        "min_response_delay_secs": config.bot.min_response_delay_secs,
        "response_attempts": config.bot.response_attempts,
        "response_attempts_max": config.bot.response_attempts_max,
        "quiet_window_secs": config.bot.quiet_window_secs,
        "command_dedup_ttl_secs": config.bot.command_dedup_ttl_secs,
        "include_test_signal": config.bot.include_test_signal,
      },
      "probe": {
        "admin_password": config.probe.admin_password,
        "guest_password": config.probe.guest_password,
        "default_guest_password": config.probe.default_guest_password,
        "pre_login_advert_name": config.probe.pre_login_advert_name,
        "poll_interval_secs": config.probe.poll_interval_secs,
        "request_timeout_secs": config.probe.request_timeout_secs,
        "route_freshness_secs": config.probe.route_freshness_secs,
        "scheduled_reprobe_interval_secs": config.probe.scheduled_reprobe_interval_secs,
        "scheduled_reprobe_max_batch": config.probe.scheduled_reprobe_max_batch,
        "scheduled_reprobe_seen_within_secs": config.probe.scheduled_reprobe_seen_within_secs,
        "night_failed_retry_interval_secs": config.probe.night_failed_retry_interval_secs,
        "night_failed_retry_max_batch": config.probe.night_failed_retry_max_batch,
        "advert_probe_min_interval_secs": config.probe.advert_probe_min_interval_secs,
        "advert_reprobe_failure_cooldown_secs": config.probe.advert_reprobe_failure_cooldown_secs,
      },
      "gateway": {
        "traffic_watchdog_secs": config.gateway.traffic_watchdog_secs,
        "close_timeout_secs": config.gateway.close_timeout_secs,
        "console_probe_timeout_secs": config.gateway.console_probe_timeout_secs,
      },
      "endpoints": [
        {
          "name": endpoint.name,
          "raw_host": endpoint.raw_host,
          "raw_port": endpoint.raw_port,
          "enabled": endpoint.enabled,
          "console_port": endpoint.console_port,
          "local_node_name": endpoint.local_node_name or "",
          "console_mirror_host": endpoint.console_mirror_host or "",
          "console_mirror_port": endpoint.console_mirror_port,
        }
        for endpoint in config.endpoints
      ],
    }


def _admin_endpoint_rows(database: BotDatabase, config: AppConfig) -> list[dict[str, object]]:
    recent_jobs = database.list_probe_jobs(limit=512)
    rows: list[dict[str, object]] = []
    for endpoint in config.endpoints:
      endpoint_jobs = [job for job in recent_jobs if str(job.get("endpoint_name") or "") == endpoint.name]
      seen_repeaters = database.list_repeaters_seen_on_endpoint(
        endpoint_name=endpoint.name,
        limit=128,
        seen_within_hours=48.0,
      )
      rows.append(
        {
          "name": endpoint.name,
          "raw_host": endpoint.raw_host,
          "raw_port": endpoint.raw_port,
          "enabled": endpoint.enabled,
          "console_port": endpoint.console_port,
          "local_node_name": endpoint.local_node_name,
          "console_mirror_host": endpoint.console_mirror_host,
          "console_mirror_port": endpoint.console_mirror_port,
          "seen_repeater_count": len(seen_repeaters),
          "last_advert_at": seen_repeaters[0].get("advert_observed_at") if seen_repeaters else None,
          "recent_job_count": len(endpoint_jobs),
          "recent_failed_count": sum(1 for job in endpoint_jobs if str(job.get("status") or "") == "failed"),
          "recent_running_count": sum(1 for job in endpoint_jobs if str(job.get("status") or "") == "running"),
          "recent_pending_count": sum(1 for job in endpoint_jobs if str(job.get("status") or "") == "pending"),
          "sample_repeaters": [str(item.get("name") or item.get("pubkey_hex") or "") for item in seen_repeaters[:4]],
        }
      )
    return rows


def _recent_raw_packet_rows(database: BotDatabase, *, limit: int = ADMIN_PACKET_LOG_LIMIT) -> list[dict[str, object]]:
    with database.connect() as connection:
      rows = connection.execute(
        """
        SELECT id,
             endpoint_name,
             observed_at,
             direction,
             transport,
             payload_type,
             route_type,
             remote_pubkey_hex,
             request_tag,
             notes,
             SUBSTR(mesh_packet_hex, 1, 96) AS mesh_packet_prefix,
             SUBSTR(COALESCE(rs232_frame_hex, ''), 1, 96) AS rs232_frame_prefix
        FROM raw_mesh_packets
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
      ).fetchall()
    return [dict(row) for row in rows]


def _tail_log_files(config_path: Path) -> list[dict[str, object]]:
    logs_dir = config_path.parent.parent / "logs"
    if not logs_dir.exists() or not logs_dir.is_dir():
      return []
    try:
      candidates = sorted(
        [path for path in logs_dir.iterdir() if path.is_file()],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
      )[:ADMIN_LOG_FILE_LIMIT]
    except OSError:
      return []

    rows: list[dict[str, object]] = []
    for path in candidates:
      try:
        content = path.read_text(encoding="utf-8", errors="replace")
      except OSError:
        continue
      tail = "\n".join(content.splitlines()[-ADMIN_LOG_TAIL_LINES:])
      rows.append({"name": path.name, "path": str(path), "tail": tail})
    return rows


def _build_admin_dashboard_payload(database: BotDatabase, config: AppConfig, *, config_path: Path) -> dict[str, object]:
    repeaters = database.list_repeaters_for_web()
    recent_jobs = database.list_probe_jobs(limit=ADMIN_PROBE_JOB_LIMIT)
    now = datetime.now(tz=UTC)
    recent_window = now - timedelta(hours=24)

    def is_recent(timestamp_value: object) -> bool:
      parsed = _parse_iso_datetime(timestamp_value)
      return parsed is not None and parsed >= recent_window

    return {
      "config_path": str(config_path),
      "overview": database.snapshot_overview(),
      "summary": {
        "recent_nodes": sum(1 for repeater in repeaters if is_recent(repeater.get("last_advert_at") or repeater.get("last_seen_at"))),
        "nodes_with_data": sum(1 for repeater in repeaters if bool(repeater.get("data_fetch_ok"))),
        "recent_failures": sum(1 for job in recent_jobs if str(job.get("status") or "") == "failed"),
        "active_jobs": sum(1 for job in recent_jobs if str(job.get("status") or "") in {"pending", "running"}),
      },
      "endpoints": _admin_endpoint_rows(database, config),
      "repeaters": repeaters[:ADMIN_NODE_LIMIT],
      "probe_jobs": recent_jobs,
      "recent_failures": [job for job in recent_jobs if str(job.get("status") or "") == "failed"][:40],
      "wakeup_socket": str(probe_wakeup_socket_path(config)),
    }


def _build_admin_logs_payload(database: BotDatabase, *, config_path: Path) -> dict[str, object]:
    recent_jobs = database.list_probe_jobs(limit=ADMIN_PROBE_JOB_LIMIT)
    return {
      "recent_jobs": recent_jobs,
      "recent_failures": [job for job in recent_jobs if str(job.get("status") or "") == "failed"][:80],
      "recent_packets": _recent_raw_packet_rows(database),
      "file_logs": _tail_log_files(config_path),
    }


INDEX_HTML = """<!doctype html>
<html lang=\"pl\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>MeshCore Bot</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\">
  <style>
    :root {
      color-scheme: light;
      --bg: #e8ece7;
      --panel: rgba(248, 250, 248, 0.96);
      --panel-strong: #ffffff;
      --section: rgba(21, 33, 42, 0.045);
      --ink: #15212a;
      --muted: #6a7883;
      --line: rgba(21, 33, 42, 0.1);
      --line-strong: rgba(21, 33, 42, 0.16);
      --green: #2e8b57;
      --blue: #2c71d1;
      --red: #c64a3d;
      --yellow: #cfaa38;
      --orange: #db7d31;
      --unknown: #98a4ad;
      --shadow: 0 20px 48px rgba(21, 33, 42, 0.14);
      --shadow-soft: 0 8px 22px rgba(21, 33, 42, 0.08);
    }
    html, body {
      margin: 0;
      height: 100%;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      font-variant-numeric: tabular-nums;
      -webkit-text-size-adjust: 100%;
    }
    body {
      overflow: hidden;
    }
    #app {
      position: relative;
      width: 100%;
      height: 100%;
      min-height: 100dvh;
      overflow: hidden;
    }
    .admin-link {
      position: fixed;
      top: 16px;
      left: 16px;
      z-index: 1400;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(21, 33, 42, 0.14);
      background: rgba(255, 255, 255, 0.88);
      color: var(--ink);
      text-decoration: none;
      font-size: 0.82rem;
      font-weight: 600;
      box-shadow: var(--shadow-soft);
      backdrop-filter: blur(10px);
    }
    #map {
      position: absolute;
      inset: 0;
      background: #e8eeeb;
    }
    .overlay {
      position: absolute;
      z-index: 1000;
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    #sidebar {
      top: 16px;
      right: 16px;
      bottom: 16px;
      width: min(438px, calc(100vw - 32px));
      border-radius: 24px;
      display: grid;
      grid-template-rows: auto auto 1fr auto;
      overflow: hidden;
      background: rgba(246, 248, 246, 0.98);
      border-color: rgba(21, 33, 42, 0.08);
    }
    .sheet-toggle {
      display: none;
      width: 100%;
      border: 0;
      border-bottom: 1px solid var(--line);
      background: transparent;
      padding: 8px 14px 6px;
      cursor: pointer;
      text-align: center;
      font: inherit;
      color: var(--muted);
    }
    .sheet-toggle span {
      display: inline-block;
      vertical-align: middle;
    }
    .sheet-handle {
      width: 42px;
      height: 5px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.18);
      box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.8);
    }
    .sheet-label {
      display: none;
      margin-left: 8px;
      font-size: 0.72rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    #map-legend {
      left: 16px;
      bottom: 16px;
      border-radius: 14px;
      padding: 10px 12px;
      max-width: 250px;
      font-size: 0.74rem;
      color: var(--muted);
    }
    .summary-strip {
      display: grid;
      gap: 10px;
      padding: 14px 16px 12px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.84), rgba(248, 250, 248, 0.72));
    }
    .summary-shell {
      display: grid;
      gap: 10px;
    }
    .summary-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
    }
    .summary-copy {
      display: grid;
      gap: 4px;
      min-width: 0;
    }
    .summary-copy strong {
      font-size: 0.95rem;
      line-height: 1.1;
      letter-spacing: -0.01em;
    }
    .summary-copy span {
      color: var(--muted);
      font-size: 0.74rem;
      line-height: 1.3;
    }
    .summary-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid rgba(44, 113, 209, 0.12);
      background: rgba(44, 113, 209, 0.1);
      color: var(--ink);
      font-size: 0.7rem;
      font-weight: 700;
      line-height: 1.2;
      text-align: center;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 6px;
    }
    .summary-card {
      display: grid;
      align-content: center;
      min-height: 56px;
      padding: 10px 8px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.88);
      box-shadow: var(--shadow-soft);
      text-align: center;
    }
    .summary-card strong {
      display: block;
      font-size: 0.93rem;
      line-height: 1.1;
    }
    .summary-card span {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.68rem;
      line-height: 1.15;
    }
    .list-shell {
      overflow: auto;
      padding: 12px 14px 16px;
    }
    .list-toolbar {
      display: grid;
      gap: 10px;
      margin: 0 0 12px;
      padding: 14px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 18px;
      background: var(--section);
    }
    .list-toolbar label {
      color: var(--muted);
      font-size: 0.7rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .toolbar-cluster {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: space-between;
    }
    .toolbar-meta {
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: 8px;
      flex-wrap: wrap;
    }
    .toolbar-meta-group {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .toolbar-toggle-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      padding: 8px 12px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--muted);
      font: inherit;
      font-size: 0.74rem;
      font-weight: 600;
      cursor: pointer;
      white-space: nowrap;
    }
    .toolbar-toggle-button.active {
      background: rgba(44, 113, 209, 0.14);
      border-color: rgba(44, 113, 209, 0.16);
      color: var(--ink);
    }
    .toolbar-note {
      padding: 10px 12px;
      border: 1px solid rgba(207, 170, 56, 0.24);
      border-radius: 12px;
      background: rgba(207, 170, 56, 0.12);
      color: #6e5510;
      font-size: 0.74rem;
      line-height: 1.35;
    }
    .toolbar-note strong {
      color: var(--ink);
    }
    .toolbar-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }
    .toolbar-head-main {
      display: grid;
      gap: 6px;
      min-width: 0;
    }
    .toolbar-head-actions {
      display: inline-flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
      flex: 0 0 auto;
    }
    .toolbar-title {
      font-size: 0.98rem;
      line-height: 1.1;
      letter-spacing: -0.01em;
    }
    .toolbar-subtitle {
      color: var(--muted);
      font-size: 0.76rem;
      line-height: 1.32;
    }
    .primary-toggle,
    .secondary-toggle,
    .filter-toggle {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.6);
      flex-wrap: wrap;
    }
    .primary-toggle {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 6px;
      padding: 6px;
      border-radius: 16px;
      background: rgba(21, 33, 42, 0.06);
    }
    .secondary-toggle,
    .filter-toggle {
      margin-bottom: 8px;
    }
    .analysis-tabs {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      width: 100%;
    }
    .analysis-tabs .segmented-button {
      width: 100%;
    }
    .segmented-button {
      border: 0;
      border-radius: 12px;
      background: transparent;
      color: var(--muted);
      padding: 8px 12px;
      font: inherit;
      font-size: 0.77rem;
      font-weight: 600;
      cursor: pointer;
      white-space: nowrap;
    }
    .segmented-button.active {
      background: rgba(44, 113, 209, 0.18);
      color: var(--ink);
      box-shadow: inset 0 0 0 1px rgba(44, 113, 209, 0.16), 0 1px 0 rgba(255, 255, 255, 0.9);
    }
    .segmented-button:disabled,
    .segmented-button.disabled {
      opacity: 0.44;
      color: var(--muted);
      cursor: not-allowed;
      box-shadow: none;
    }
    .mobile-view-toggle {
      display: none;
      align-items: center;
      gap: 4px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.6);
    }
    .view-button {
      border: 0;
      border-radius: 999px;
      background: transparent;
      color: var(--muted);
      padding: 4px 9px;
      font: inherit;
      font-size: 0.7rem;
      cursor: pointer;
    }
    .view-button.active {
      background: rgba(44, 113, 209, 0.14);
      color: var(--ink);
    }
    .sort-select {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--ink);
      padding: 8px 10px;
      font: inherit;
      font-size: 0.82rem;
    }
    .toolbar-search {
      position: relative;
      min-width: min(280px, 100%);
      flex: 1 1 220px;
    }
    .toolbar-search-input {
      width: 100%;
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      color: var(--ink);
      padding: 8px 12px;
      font: inherit;
      font-size: 0.86rem;
    }
    .toolbar-search-input::placeholder {
      color: var(--muted);
    }
    .lang-toggle {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 4px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.82);
    }
    .lang-button {
      border: 0;
      border-radius: 999px;
      background: transparent;
      color: var(--muted);
      padding: 4px 9px;
      font: inherit;
      font-size: 0.7rem;
      cursor: pointer;
    }
    .lang-button.active {
      background: rgba(44, 113, 209, 0.14);
      color: var(--ink);
    }
    .toolbar-head .lang-toggle {
      flex: 0 0 auto;
    }
    .section-heading {
      margin: 10px 2px 6px;
      color: var(--muted);
      font-size: 0.72rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .node-list {
      display: grid;
      gap: 6px;
    }
    .node-row {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: var(--shadow-soft);
      overflow: hidden;
    }
    .node-row.active {
      background: var(--panel-strong);
      border-color: rgba(44, 113, 209, 0.24);
    }
    .node-row-button {
      width: 100%;
      border: 0;
      background: transparent;
      color: inherit;
      padding: 8px 9px;
      display: grid;
      grid-template-columns: auto 1fr auto;
      gap: 8px;
      align-items: center;
      text-align: left;
      cursor: pointer;
      font: inherit;
    }
    .node-row-button:hover {
      background: rgba(255, 255, 255, 0.28);
    }
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.96);
      flex: 0 0 auto;
    }
    .node-main {
      min-width: 0;
    }
    .node-name {
      display: block;
      font-size: 0.92rem;
      line-height: 1.2;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .node-age {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.77rem;
      line-height: 1.1;
    }
    .node-state-tag {
      color: var(--muted);
      font-size: 0.74rem;
      white-space: nowrap;
    }
    .node-expand {
      padding: 0 9px 10px;
      display: grid;
      gap: 10px;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
    }
    .detail-cell {
      padding: 7px 8px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.84);
      font-size: 0.78rem;
      color: var(--muted);
      line-height: 1.22;
    }
    .detail-cell strong {
      display: block;
      color: var(--ink);
      font-size: 0.76rem;
      margin-bottom: 2px;
    }
    .expand-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding-top: 2px;
    }
    .expand-head strong {
      font-size: 0.82rem;
    }
    .ghost-button {
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.5);
      border-radius: 999px;
      color: var(--muted);
      padding: 3px 8px;
      cursor: pointer;
      font: inherit;
      font-size: 0.7rem;
    }
    .probe-queue-card {
      display: grid;
      gap: 8px;
      padding: 9px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.88);
    }
    .probe-queue-controls {
      display: flex;
      align-items: center;
      justify-content: flex-start;
    }
    .probe-submit-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      padding: 8px 12px;
      border: 0;
      border-radius: 12px;
      background: var(--blue);
      color: #fff;
      font: inherit;
      font-size: 0.76rem;
      font-weight: 700;
      cursor: pointer;
      white-space: nowrap;
      box-shadow: var(--shadow-soft);
    }
    .probe-submit-button:disabled {
      opacity: 0.58;
      cursor: wait;
      box-shadow: none;
    }
    .probe-note {
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.28;
    }
    .probe-status-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 4px 9px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.05);
      color: var(--muted);
      font-size: 0.67rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .probe-status-chip.pending,
    .probe-status-chip.busy {
      background: rgba(44, 113, 209, 0.12);
      color: var(--blue);
    }
    .probe-status-chip.cooldown {
      background: rgba(207, 170, 56, 0.16);
      color: #8a6a0d;
    }
    .probe-status-chip.error {
      background: rgba(198, 74, 61, 0.12);
      color: var(--red);
    }
    .neighbor-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.71rem;
    }
    .neighbor-table th,
    .neighbor-table td {
      padding: 5px 4px;
      border-bottom: 1px solid rgba(21, 33, 42, 0.08);
      text-align: left;
      vertical-align: top;
    }
    .neighbor-table th {
      color: var(--muted);
      font-weight: 600;
      font-size: 0.68rem;
    }
    .neighbor-table button {
      border: 0;
      background: transparent;
      padding: 0;
      color: inherit;
      text-align: left;
      cursor: pointer;
      font: inherit;
      line-height: 1.2;
    }
    .neighbor-table tr.active {
      background: rgba(44, 113, 209, 0.08);
    }
    .chart-shell {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.88);
      padding: 8px;
    }
    .chart-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }
    .chart-title {
      font-size: 0.76rem;
      line-height: 1.2;
    }
    .chart-title strong {
      display: block;
      font-size: 0.8rem;
    }
    .chart-meta {
      color: var(--muted);
      font-size: 0.68rem;
      white-space: nowrap;
    }
    #signal-chart {
      width: 100%;
      height: 152px;
      display: block;
    }
    .empty-note {
      color: var(--muted);
      font-size: 0.74rem;
      line-height: 1.3;
      padding: 4px 0 2px;
    }
    .map-warning-note {
      margin: 0 0 10px;
      padding: 10px 12px;
      border: 1px solid rgba(207, 170, 56, 0.32);
      border-radius: 12px;
      background: rgba(207, 170, 56, 0.12);
      color: #6d5313;
      font-size: 0.76rem;
      line-height: 1.4;
    }
    .panel-stack {
      display: grid;
      gap: 10px;
    }
    @media (max-width: 560px) {
      .probe-submit-button {
        width: 100%;
      }
    }
    .panel-section {
      display: grid;
      gap: 8px;
      padding: 10px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 16px;
      background: rgba(21, 33, 42, 0.03);
    }
    .panel-card {
      padding: 10px 11px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
    }
    .panel-card strong {
      display: block;
      font-size: 0.84rem;
      line-height: 1.15;
    }
    .panel-card span {
      display: block;
      margin-top: 4px;
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.3;
    }
    .panel-section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: -2px;
    }
    .panel-section-title {
      font-size: 0.7rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .panel-section-note {
      color: var(--muted);
      font-size: 0.69rem;
      line-height: 1.2;
      text-align: right;
    }
    .answer-strip {
      display: grid;
      gap: 8px;
      padding: 12px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.96);
    }
    .answer-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .answer-title {
      display: grid;
      gap: 3px;
      min-width: 0;
    }
    .answer-title strong {
      display: block;
      font-size: 0.94rem;
      line-height: 1.1;
    }
    .answer-title span {
      display: block;
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.24;
    }
    .answer-kicker {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 4px 9px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.05);
      color: var(--muted);
      font-size: 0.67rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .answer-kicker.alert {
      background: rgba(198, 74, 61, 0.1);
      color: var(--red);
    }
    .answer-metrics {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .answer-stat {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 26px;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      background: rgba(21, 33, 42, 0.04);
      font-size: 0.69rem;
      font-weight: 600;
      letter-spacing: 0.03em;
      white-space: nowrap;
    }
    .answer-stat strong {
      font-size: 0.76rem;
      line-height: 1;
    }
    .answer-state {
      color: var(--ink);
      font-size: 0.84rem;
      line-height: 1.28;
    }
    .answer-state.muted {
      color: var(--muted);
    }
    .relation-grid,
    .route-result-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }
    .route-result-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
      align-items: stretch;
    }
    .relation-card,
    .route-card {
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
    }
    .route-card {
      display: grid;
      align-content: start;
      gap: 8px;
      min-height: 0;
    }
    .relation-card strong,
    .route-card strong {
      display: block;
      font-size: 0.86rem;
      line-height: 1.1;
    }
    .relation-card span,
    .route-card span {
      display: block;
      margin-top: 3px;
      color: var(--muted);
      font-size: 0.68rem;
    }
    .relation-list {
      display: grid;
      gap: 5px;
    }
    .relation-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: start;
      gap: 8px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
    }
    .relation-main {
      min-width: 0;
      display: grid;
      gap: 2px;
    }
    .relation-main strong {
      font-size: 0.84rem;
      line-height: 1.2;
    }
    .relation-main span {
      color: var(--muted);
      font-size: 0.74rem;
      line-height: 1.18;
    }
    .relation-badges {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
      flex: 0 0 auto;
    }
    .direction-chip,
    .stale-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 58px;
      padding: 3px 8px;
      border-radius: 999px;
      font-size: 0.72rem;
      font-weight: 600;
      line-height: 1.2;
    }
    .panel-details {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.88);
      overflow: hidden;
    }
    .panel-details summary {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      cursor: pointer;
      list-style: none;
      padding: 12px 14px;
      font-size: 0.8rem;
      font-weight: 700;
      color: var(--ink);
    }
    .panel-details summary::-webkit-details-marker {
      display: none;
    }
    .panel-details summary::after {
      content: '+';
      color: var(--muted);
      font-size: 0.96rem;
      line-height: 1;
      flex: 0 0 auto;
    }
    .panel-details[open] summary::after {
      content: '-';
    }
    .panel-details-body {
      padding: 0 14px 14px;
      display: grid;
      gap: 10px;
    }
    .direction-chip {
      background: rgba(46, 139, 87, 0.12);
      color: var(--ink);
    }
    .stale-chip {
      background: rgba(198, 74, 61, 0.12);
      color: var(--red);
    }
    .route-controls {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      align-items: end;
    }
    .route-control-bar {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      align-items: stretch;
    }
    .route-endpoint-stack {
      display: grid;
      gap: 8px;
    }
    .route-picker-note {
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.22;
      text-align: left;
    }
    .route-picker-note strong {
      color: var(--ink);
      font-size: 0.74rem;
    }
    .route-destination-list {
      display: grid;
      gap: 6px;
    }
    .route-destination-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: center;
      gap: 8px;
      padding: 9px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      color: var(--ink);
      cursor: pointer;
      text-align: left;
      font: inherit;
    }
    .route-destination-item.active {
      border-color: rgba(207, 170, 56, 0.28);
      box-shadow: inset 0 0 0 1px rgba(207, 170, 56, 0.18), var(--shadow-soft);
    }
    .route-destination-main {
      min-width: 0;
      display: grid;
      gap: 2px;
    }
    .route-destination-main strong {
      display: block;
      font-size: 0.8rem;
      line-height: 1.2;
      word-break: break-word;
    }
    .route-destination-main span {
      display: block;
      color: var(--muted);
      font-size: 0.68rem;
      line-height: 1.18;
    }
    .route-destination-action {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(44, 113, 209, 0.1);
      color: var(--blue);
      font-size: 0.67rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .route-destination-item.active .route-destination-action {
      background: rgba(207, 170, 56, 0.16);
      color: #9c7b13;
    }
    .route-destination-empty {
      padding: 10px 12px;
      border: 1px dashed rgba(21, 33, 42, 0.14);
      border-radius: 12px;
      background: rgba(21, 33, 42, 0.03);
    }
    .route-destination-empty strong {
      display: block;
      font-size: 0.8rem;
      line-height: 1.2;
    }
    .route-destination-empty span {
      display: block;
      margin-top: 4px;
      color: var(--muted);
      font-size: 0.7rem;
      line-height: 1.2;
    }
    .route-hint-shell {
      display: grid;
      gap: 8px;
    }
    .route-hint-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .route-hint-chip {
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.06);
      color: #41505c;
      font-size: 0.67rem;
      font-weight: 700;
    }
    .route-hint-path {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }
    .route-hint-step {
      display: inline-flex;
      align-items: center;
      padding: 5px 10px;
      border-radius: 999px;
      background: rgba(44, 113, 209, 0.1);
      color: #173b61;
      font-size: 0.72rem;
      font-weight: 700;
    }
    .route-hint-step.uncertain {
      background: rgba(207, 170, 56, 0.16);
      color: #7a5600;
    }
    .route-hint-arrow {
      color: #7d8992;
      font-size: 0.72rem;
      font-weight: 700;
    }
    .route-hint-note {
      color: #55636f;
      font-size: 0.76rem;
      line-height: 1.35;
    }
    .route-endpoint {
      display: grid;
      grid-template-columns: 34px minmax(0, 1fr);
      align-items: center;
      gap: 6px;
      padding: 10px 12px;
      min-height: 0;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
      text-align: left;
      cursor: pointer;
      font: inherit;
      color: var(--ink);
    }
    .route-endpoint.active {
      border-color: rgba(44, 113, 209, 0.24);
      box-shadow: inset 0 0 0 1px rgba(44, 113, 209, 0.16), var(--shadow-soft);
    }
    .route-endpoint.route-endpoint-target.active {
      border-color: rgba(207, 170, 56, 0.28);
      box-shadow: inset 0 0 0 1px rgba(207, 170, 56, 0.18), var(--shadow-soft);
    }
    .route-endpoint-label {
      grid-row: 1 / span 2;
      grid-column: 1;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 34px;
      height: 34px;
      border-radius: 999px;
      background: rgba(44, 113, 209, 0.08);
      color: var(--muted);
      font-size: 0.67rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .route-endpoint-target .route-endpoint-label {
      background: rgba(207, 170, 56, 0.12);
    }
    .route-endpoint-name {
      grid-column: 2;
      display: block;
      font-size: 0.96rem;
      line-height: 1.15;
      word-break: break-word;
    }
    .route-actions {
      display: flex;
      justify-content: flex-start;
    }
    .route-endpoint-clear {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 34px;
      padding: 5px 10px;
      border: 1px solid rgba(21, 33, 42, 0.1);
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.04);
      color: var(--muted);
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      cursor: pointer;
      font: inherit;
    }
    .route-endpoint-clear:hover {
      color: var(--ink);
      border-color: rgba(21, 33, 42, 0.16);
      background: rgba(21, 33, 42, 0.06);
    }
    .field-stack {
      display: grid;
      gap: 5px;
    }
    .field-stack label {
      color: var(--muted);
      font-size: 0.72rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .route-select {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.96);
      color: var(--ink);
      padding: 8px 10px;
      font: inherit;
      font-size: 0.84rem;
    }
    .route-status-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }
    .route-status-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 88px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.68rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .route-status-badge.ok {
      background: rgba(46, 139, 87, 0.14);
      color: var(--green);
    }
    .route-status-badge.no {
      background: rgba(198, 74, 61, 0.12);
      color: var(--red);
    }
    .route-meta {
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.2;
    }
    .route-card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }
    .route-direction-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 54px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.67rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      background: rgba(21, 33, 42, 0.06);
      color: var(--ink);
    }
    .route-direction-chip.forward {
      background: rgba(44, 113, 209, 0.12);
      color: var(--blue);
    }
    .route-direction-chip.backward {
      background: rgba(207, 170, 56, 0.16);
      color: #9c7b13;
    }
    .route-path {
      display: grid;
      gap: 6px;
      align-content: start;
      font-size: 0.74rem;
      margin-top: 0;
      justify-items: center;
    }
    .route-hop-row {
      width: 100%;
      display: flex;
      justify-content: center;
      position: relative;
    }
    .route-hop-row + .route-hop-row::before {
      content: '';
      position: absolute;
      top: -7px;
      left: 50%;
      width: 1px;
      height: 8px;
      background: rgba(21, 33, 42, 0.16);
      transform: translateX(-50%);
    }
    .route-step {
      padding: 5px 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.96);
      min-width: 0;
      text-align: center;
    }
    .route-empty {
      display: grid;
      gap: 4px;
      align-content: center;
      min-height: 92px;
      text-align: left;
    }
    .route-empty strong {
      font-size: 0.8rem;
    }
    .route-empty span {
      margin-top: 0;
      font-size: 0.71rem;
    }
    .compact-note {
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.28;
    }
    .compact-note strong {
      display: block;
      margin-bottom: 2px;
      color: var(--ink);
      font-size: 0.78rem;
      line-height: 1.2;
    }
    .mobile-map-stack {
      display: grid;
      gap: 8px;
    }
    .mobile-analysis-tabs {
      display: none;
    }
    .mobile-summary-card {
      display: grid;
      gap: 8px;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.95);
    }
    .mobile-overview-card {
      display: grid;
      gap: 8px;
      padding: 10px;
      margin-bottom: 8px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.95);
      box-shadow: var(--shadow-soft);
    }
    .mobile-overview-head {
      display: grid;
      gap: 6px;
    }
    .mobile-overview-copy {
      display: grid;
      gap: 3px;
    }
    .mobile-overview-copy strong {
      font-size: 0.84rem;
      line-height: 1.15;
    }
    .mobile-overview-copy span {
      color: var(--muted);
      font-size: 0.71rem;
      line-height: 1.24;
    }
    .mobile-overview-status {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: fit-content;
      min-height: 24px;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(44, 113, 209, 0.1);
      color: var(--ink);
      font-size: 0.68rem;
      font-weight: 700;
      line-height: 1.2;
    }
    .mobile-overview-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
    }
    .mobile-overview-metric {
      padding: 8px 7px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(248, 250, 248, 0.92);
      text-align: center;
    }
    .mobile-overview-metric strong {
      display: block;
      font-size: 0.81rem;
      line-height: 1.1;
    }
    .mobile-overview-metric span {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.65rem;
      line-height: 1.2;
    }
    .mobile-summary-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 8px;
    }
    .mobile-summary-title {
      display: grid;
      gap: 3px;
      min-width: 0;
    }
    .mobile-summary-title strong {
      font-size: 0.84rem;
      line-height: 1.15;
    }
    .mobile-summary-title span {
      color: var(--muted);
      font-size: 0.71rem;
      line-height: 1.2;
    }
    .mobile-summary-count {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.05);
      color: var(--ink);
      font-size: 0.68rem;
      font-weight: 700;
      white-space: nowrap;
    }
    .mobile-relation-list {
      display: grid;
      gap: 5px;
    }
    .mobile-relation-button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: inherit;
      padding: 8px 10px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: start;
      text-align: left;
      cursor: pointer;
      font: inherit;
    }
    .mobile-relation-button.active {
      border-color: rgba(44, 113, 209, 0.24);
      background: rgba(255, 255, 255, 0.98);
    }
    .mobile-relation-main {
      min-width: 0;
      display: grid;
      gap: 2px;
    }
    .mobile-relation-main strong {
      font-size: 0.78rem;
      line-height: 1.2;
    }
    .mobile-relation-main span {
      color: var(--muted);
      font-size: 0.68rem;
      line-height: 1.18;
    }
    .mobile-relation-meta {
      display: grid;
      gap: 4px;
      justify-items: end;
    }
    .legend-group + .legend-group {
      margin-top: 9px;
    }
    .legend-title {
      display: block;
      margin-bottom: 4px;
      color: var(--ink);
      font-size: 0.73rem;
    }
    .legend-row {
      display: flex;
      align-items: center;
      gap: 6px;
      margin-top: 4px;
    }
    .legend-node,
    .legend-line {
      flex: 0 0 auto;
    }
    .legend-node {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.96);
    }
    .legend-line {
      width: 18px;
      height: 0;
      border-top-width: 2px;
      border-top-style: solid;
    }
    .legend-arrow {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      border-radius: 999px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      background: rgba(255, 255, 255, 0.82);
      color: var(--ink);
      font-size: 0.8rem;
      line-height: 1;
    }
    .legend-line.dashed {
      border-top-style: dashed;
    }
    .leaflet-control-attribution {
      opacity: 0.7;
    }
    .node-label-icon,
    .link-label-icon,
    .line-arrow-icon {
      background: transparent;
      border: 0;
      transform: translate(-50%, -50%);
    }
    .line-arrow-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 19px;
      height: 19px;
      border-radius: 999px;
      border: 1px solid rgba(21, 33, 42, 0.1);
      background: rgba(255, 255, 255, 0.9);
      box-shadow: 0 6px 14px rgba(21, 33, 42, 0.12);
      color: var(--ink);
      font-size: 13px;
      font-weight: 700;
      text-shadow: none;
    }
    .node-label-chip {
      border: 1px solid rgba(21, 33, 42, 0.1);
      border-radius: 10px;
      background: rgba(255, 255, 255, 0.76);
      box-shadow: 0 8px 18px rgba(21, 33, 42, 0.08);
      color: var(--ink);
      padding: 5px 8px;
      white-space: nowrap;
      font-size: 0.72rem;
      line-height: 1.2;
      pointer-events: none;
    }
    .node-label-chip strong {
      font-size: 0.74rem;
      font-weight: 600;
    }
    .node-label-chip .label-meta {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.68rem;
    }
    .signal-label-chip {
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.74);
      box-shadow: 0 8px 18px rgba(21, 33, 42, 0.06);
      color: var(--ink);
      padding: 4px 8px;
      font-family: 'SFMono-Regular', ui-monospace, monospace;
      font-size: 0.66rem;
      line-height: 1.2;
      text-align: center;
      white-space: nowrap;
      pointer-events: none;
    }
    .node-label-chip.focused {
      border-color: rgba(21, 33, 42, 0.18);
      background: rgba(255, 255, 255, 0.92);
      box-shadow: 0 10px 24px rgba(21, 33, 42, 0.14);
    }
    .node-label-chip.active-peer {
      background: rgba(255, 255, 255, 0.86);
    }
    .signal-label-chip strong,
    .signal-label-chip span {
      display: block;
    }
    @media (max-width: 860px) {
      body {
        overflow: auto;
      }
      #app {
        display: flex;
        flex-direction: column;
        height: auto;
        min-height: 100dvh;
        overflow: visible;
        gap: 10px;
        padding-bottom: max(12px, env(safe-area-inset-bottom));
      }
      #map {
        position: relative;
        inset: auto;
        order: 1;
        flex: 0 0 clamp(180px, 26dvh, 240px);
        min-height: clamp(180px, 26dvh, 240px);
      }
      #sidebar {
        position: relative;
        order: 3;
        left: auto;
        right: auto;
        top: auto;
        bottom: auto;
        width: auto;
        max-height: none;
        margin: 0 12px 0;
        border-radius: 20px;
        background: rgba(248, 250, 248, 0.98);
      }
      #map-legend {
        position: relative;
        order: 2;
        left: auto;
        right: auto;
        top: auto;
        bottom: auto;
        max-width: none;
        margin: 0 12px;
        padding: 10px 12px;
        border-radius: 16px;
        font-size: 0.7rem;
      }
      .summary-strip {
        padding: 14px 12px 10px;
      }
      .summary-grid {
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 8px;
      }
      .summary-head {
        flex-direction: column;
        align-items: flex-start;
      }
      .summary-card {
        padding: 8px 4px;
      }
      .summary-card strong {
        font-size: 0.78rem;
      }
      .summary-card span {
        font-size: 0.6rem;
      }
      .list-shell {
        padding: 12px 12px 18px;
      }
      .list-toolbar {
        gap: 8px;
        padding: 12px;
      }
      .toolbar-cluster {
        justify-content: space-between;
      }
      .mobile-view-toggle {
        display: inline-flex;
      }
      .mobile-analysis-tabs {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }
      .primary-toggle,
      .secondary-toggle,
      .filter-toggle {
        width: 100%;
        justify-content: flex-start;
      }
      .primary-toggle {
        grid-template-columns: 1fr 1fr;
      }
      .toolbar-meta {
        flex-direction: column;
        align-items: stretch;
      }
      .toolbar-search {
        min-width: 0;
        width: 100%;
      }
      .toolbar-head {
        align-items: stretch;
      }
      .toolbar-head-actions {
        justify-content: space-between;
      }
      .toolbar-meta-group {
        justify-content: space-between;
      }
      .sort-select {
        min-height: 38px;
        font-size: 0.94rem;
        padding: 7px 12px;
      }
      .lang-button {
        min-height: 34px;
        padding: 5px 12px;
        font-size: 0.82rem;
      }
      .node-row-button {
        gap: 10px;
        padding: 11px 11px;
      }
      .node-name {
        white-space: normal;
        overflow: visible;
        text-overflow: clip;
        font-size: 0.82rem;
      }
      .node-age {
        font-size: 0.78rem;
      }
      .node-state-tag {
        align-self: start;
        font-size: 0.76rem;
      }
      .node-expand {
        padding: 0 11px 12px;
      }
      .detail-grid {
        grid-template-columns: 1fr;
      }
      .detail-cell {
        font-size: 0.76rem;
      }
      .expand-head {
        align-items: flex-start;
        flex-direction: column;
      }
      .neighbor-table {
        display: block;
        overflow-x: auto;
        white-space: nowrap;
      }
      .relation-grid,
      .route-result-grid,
      .route-control-bar,
      .route-controls {
        grid-template-columns: 1fr;
      }
      .relation-item {
        grid-template-columns: 1fr;
      }
      .relation-badges {
        justify-content: flex-start;
      }
      .chart-head {
        align-items: flex-start;
        flex-direction: column;
      }
      .chart-meta {
        white-space: normal;
      }
      .legend-group + .legend-group {
        margin-top: 12px;
      }
      #map-legend .legend-row {
        display: grid;
        grid-template-columns: auto 1fr;
        align-items: center;
        column-gap: 8px;
      }
      .leaflet-left .leaflet-control {
        margin-left: 10px;
      }
      .leaflet-top .leaflet-control {
        margin-top: 10px;
      }
    }
    @media (max-width: 860px) and (orientation: portrait) {
      #app {
        display: block;
        height: 100dvh;
        min-height: 100dvh;
        overflow: hidden;
        padding-bottom: 0;
      }
      #map {
        position: absolute;
        inset: 0;
        display: block;
        min-height: auto;
        height: auto;
      }
      #sidebar {
        position: absolute;
        left: 10px;
        right: 10px;
        top: auto;
        bottom: max(10px, env(safe-area-inset-bottom));
        width: auto;
        height: min(34dvh, 280px);
        max-height: min(34dvh, 280px);
        margin: 0;
        overflow: hidden;
        border-radius: 18px;
        z-index: 1200;
        transition: height 180ms ease, max-height 180ms ease, transform 180ms ease;
      }
      #sidebar.sheet-collapsed {
        height: min(18dvh, 148px);
        max-height: min(18dvh, 148px);
      }
      #sidebar.sheet-expanded {
        height: min(74dvh, 640px);
        max-height: min(74dvh, 640px);
      }
      .sheet-toggle {
        display: block;
      }
      .sheet-label {
        display: inline-block;
      }
      #map-legend {
        display: none;
      }
      .summary-strip {
        display: none;
      }
      .list-shell {
        padding: 8px 10px 10px;
        overflow: auto;
        overscroll-behavior: contain;
      }
      .list-toolbar {
        margin: 0 0 8px;
      }
      .section-heading {
        margin-top: 6px;
      }
    }
    @media (max-width: 520px) {
      #map {
        flex-basis: clamp(150px, 22dvh, 200px);
        min-height: clamp(150px, 22dvh, 200px);
      }
      #sidebar {
        margin: 0 10px 0;
      }
      #map-legend {
        margin: 0 10px;
        font-size: 0.66rem;
      }
      .summary-strip {
        padding: 12px 10px 8px;
      }
      .list-shell {
        padding: 10px 10px 16px;
      }
    }
  </style>
</head>
<body>
  <a class=\"admin-link\" href=\"/admin\">Admin</a>
  <div id=\"app\">
    <div id=\"map\"></div>
    <div id=\"map-legend\" class=\"overlay\"></div>
    <aside id=\"sidebar\" class=\"overlay\">
      <button id=\"sheet-toggle\" class=\"sheet-toggle\" type=\"button\" aria-expanded=\"false\"><span class=\"sheet-handle\"></span><span class=\"sheet-label\"></span></button>
      <section class=\"summary-strip\">
        <div id=\"summary\" class=\"summary-shell\"></div>
      </section>
      <section class=\"list-shell\">
        <div id=\"node-sections\"></div>
      </section>
    </aside>
  </div>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    const ACTIVE_THRESHOLD_MS = 24 * 60 * 60 * 1000;
    const LINK_STALE_SECONDS = 6 * 60 * 60;
    const LOW_ZOOM_LABEL_THRESHOLD = 10;
    const HIGH_ZOOM_LABEL_THRESHOLD = 12;
    const MAX_COLLISION_LABELS = 18;
    const TRANSLATIONS = {
      pl: {
        unknown: 'brak',
        legendRepeaters: 'Repeatery',
        legendLinks: 'Połączenia',
        legendDataAvailable: 'dane dostępne',
        legendKnownNoData: 'znany / bez pobranych danych',
        legendInactive: 'nieaktywny > 24h',
        legendStrong: 'mocne',
        legendMedium: 'średnie',
        legendWeak: 'słabe',
        legendVeryWeak: 'bardzo słabe',
        legendDashed: 'stare dane',
        legendArrow: 'kierunek',
        summaryKnown: 'znane',
        summaryNew: 'nowe 24h',
        summaryWithData: 'z danymi',
        summaryPending: 'bez danych',
        summaryInactive: 'nieaktywne',
        archivedToggle: '>24h',
        archivedToggleCount: (count) => `>24h ${count}`,
        archivedAutoFallback: 'Brak aktywnych punktów z ostatnich 24 godzin. Pokazuję archiwalne, żeby mapa nie była pusta.',
        answerSelectedRepeater: 'Wybrany punkt',
        mobileMapTitle: 'Połączenia na mapie',
        mobileMapEmpty: 'Wybierz punkt, aby pokazać relacje na mapie.',
        mobileMapVisible: 'widoczne',
        mobileMapListTitle: 'Najbliższe relacje',
        mobileMapNoRows: 'Brak relacji dla tego trybu.',
        mobileMapPickRepeater: 'Wybierz punkt i kierunek relacji.',
        mobileMapDirectionOut: 'Na mapie: Widzę',
        mobileMapDirectionIn: 'Na mapie: Mnie widzą',
        mobileAnalysisWidze: 'Widzę',
        mobileAnalysisWidza: 'Mnie widzą',
        mobileAnalysisMutual: 'Obie strony',
        mobileAnalysisRoute: 'Trasa',
        connectivityStateOut: (count) => `${count} bezpośrednich relacji wychodzących.`,
        connectivityStateIn: (count) => `${count} punktów widzi ten punkt.`,
        connectivityStateMutual: (count) => `${count} relacji wzajemnych.`,
        connectivityStateNoOwnData: 'Ten punkt nie ma jeszcze własnych danych sąsiedztwa. Możemy pokazać tylko kto go widzi.',
        connectivityStateNoVisible: 'Brak relacji dla bieżącego widoku.',
        routeStateIdle: 'Wybierz start i cel. Wynik pokażemy od razu w obu kierunkach.',
        routeStatePickTarget: 'Wybierz cel, a pokażemy wynik trasy i oba kierunki.',
        routeStatePickSource: 'Wybierz start, aby policzyć trasę do wybranego celu.',
        routeStateReady: 'Pokazujemy oba kierunki niezależnie, jeśli istnieją.',
        routeStateSameNode: 'A i B muszą wskazywać różne punkty.',
        routeResultsTitle: 'Wynik trasy',
        routeReachabilityTitle: 'Sugestie celów z A',
        routeReachabilityIdle: 'Wybierz punkt startowy, a pokażemy dokąd można dojść.',
        routeReachabilitySummary: (count) => `Z A można dojść do ${count} punktów.`,
        routeReachabilityEmpty: 'Dla wybranego startu nie ma jeszcze znanych celów osiągalnych jednokierunkowo.',
        routeReachabilityFreshShort: 'świeże',
        routeReachabilityStaleShort: 'stare',
        routeReachabilityAction: 'Ustaw jako cel',
        routeClearTarget: 'Usuń cel',
        routeProbePathTitle: 'Zapamiętana ścieżka do celu',
        routeProbePathSaved: 'z ostatniego udanego pobrania',
        routeProbePathAdvert: 'z ostatniego ogłoszenia',
        routeProbePathNoStored: 'Brak zapamiętanej bezpośredniej ścieżki do tego celu.',
        routeProbePathFallback: 'Ostatnie pobranie mogło przejść trasą rozgłoszeniową albo odpowiedź nie zwróciła ścieżki do ponownego użycia.',
        routeProbePathObserved: 'zapisano',
        routeProbePathSource: 'źródło',
        routeProbePathEndpoint: 'cel',
        routeProbePathBot: 'BOT',
        routeProbePathTarget: 'B',
        routeProbePathUnknownHop: (prefix) => `hop ${prefix}`,
        routeProbePathAmbiguousHop: (prefix, count) => `${prefix} (${count} możliwe)`,
        routeHistoricalRoute: 'historyczna trasa',
        routeHistoricalLinks: 'historyczne linki',
        routeHistoryFallback: 'Bieżące linki nie dają przejścia, ale w historii jest starsza trasa.',
        statusData: 'gotowe',
        statusNoData: 'brak danych',
        statusInactive: 'nieaktywny',
        probeFailedAfterData: 'nieudane po zapisaniu danych',
        probeDataSaved: 'dane zapisane',
        probePending: 'czeka na dane',
        signalMissing: 'sygnał: b/d',
        distanceMissing: 'dyst: -',
        distancePrefix: 'dyst',
        lastAdvertLabel: 'ostatnio widziany',
        lastDataLabel: 'dane sąsiedztwa',
        chartHistory: 'historia',
        chartLatest: 'ostatnio',
        chartSNRHistory: 'historia SNR',
        chartNow: 'teraz',
        emptySelectRepeater: 'Wybierz punkt, aby zobaczyć jego bezpośrednie połączenia.',
        emptySelectNeighbor: 'Wybierz sąsiada, aby zobaczyć historię sygnału.',
        emptyNoNeighborLinks: 'Dla tego punktu nie ma jeszcze zapisanych połączeń sąsiedzkich.',
        emptyNoOtherRepeaters: 'Brak innych punktów.',
        emptyNoSearchResults: 'Brak punktów pasujących do filtra.',
        inspection: 'Szczegóły punktu',
        clearFocus: 'Wyczyść wybór',
        probeQueueTitle: 'Pobierz dane',
        probeQueueAction: 'Dodaj',
        probeQueueBusy: 'Dodaję...',
        probeQueueQueued: 'dodano',
        probeQueuePending: 'czeka',
        probeQueueRunning: 'trwa',
        probeQueueCooldown: 'limit',
        probeQueueError: 'błąd',
        probeQueueHintQueuedNow: 'Dodano jako następne.',
        probeQueueHintQueuedAt: (when) => `Dodano, nie wcześniej niż ${when}.`,
        probeQueueHintPendingNow: 'Już czeka.',
        probeQueueHintPendingAt: (when) => `Czeka, nie wcześniej niż ${when}.`,
        probeQueueHintRunning: 'Pobranie trwa.',
        probeQueueHintCooldown: 'Pominięto przez limit lub cooldown.',
        probeQueueHintError: 'Nie udało się dodać.',
        role: 'Rola',
        firstSeen: 'Pierwsze wykrycie',
        firstSeenLabel: 'pierwsze wykrycie',
        lastAdvert: 'Ostatnio widziany',
        lastData: 'Dane sąsiedztwa',
        lastSuccessfulProbe: 'Ostatnie udane pobranie',
        lastProbeResult: 'Wynik ostatniej próby',
        lastProbeAttempt: 'Ostatnia próba',
        directNeighbors: 'Bezpośrednie połączenia',
        mapNodePositionMissing: 'Mapa nie narysuje połączeń od tego punktu, bo nie ma on poprawnej pozycji GPS.',
        mapNeighborPositionsMissing: (count) => `Mapa pomija ${count} połącze${count === 1 ? 'nie' : count < 5 ? 'nia' : 'ń'} do sąsiadów bez poprawnej pozycji GPS.`,
        neighbor: 'Sąsiad',
        lastSeen: 'Ostatnio widziany',
        signal: 'Sygnał',
        distance: 'Dystans',
        selectedRepeater: 'Wybrany punkt',
        otherRepeaters: 'Pozostałe punkty',
        repeaters: 'Punkty',
        sortLabel: 'Sortowanie',
        searchLabel: 'Szukaj punktu',
        searchPlaceholder: 'nazwa, prefix, hex (min. 2 znaki)',
        sortLastAdvert: 'ostatnio widziany',
        sortLastData: 'ostatnie dane',
        sortAlphabetical: 'alfabetycznie',
        viewMap: 'Mapa',
        viewList: 'Lista',
        viewLabel: 'Widok',
        panelMap: 'Mapa',
        panelNew: 'Nowe',
        panelConnectivity: 'Łączność',
        panelRoute: 'Trasy',
        panelAnalysis: 'Analiza',
        focusRepeater: 'Wybór',
        relationModeOut: 'Widzę',
        relationModeIn: 'Mnie widzą',
        relationModeMutual: 'Obie strony',
        relationFilterAll: 'Wszystkie',
        relationFilterTwoWay: 'Obie strony',
        relationFilterOut: 'Widzę',
        relationFilterIn: 'Mnie widzą',
        relationDirectOut: 'bezpośrednio widzę',
        relationDirectIn: 'bezpośrednio widzą',
        relationNodeSees: (name) => `${name} widzi`,
        relationNodeSeenBy: (name) => `${name} widziany przez`,
        relationNodeMutual: (name) => `${name} w obie strony`,
        connectivityHint: 'Wybierz punkt.',
        connectivitySelect: 'Punkt',
        connectivityVisible: 'Widoczne relacje',
        connectivityCountShort: 'rel.',
        connectivityNoRows: 'Brak relacji dla wybranego widoku.',
        connectivitySummaryTitle: 'Podsumowanie',
        connectivityVisibleTitle: 'Widoczne relacje',
        connectivityFilterHint: 'W warstwie porównania pokazuj tylko jeden typ.',
        connectivitySummaryOut: 'widzę',
        connectivitySummaryIn: 'widzą',
        connectivitySummaryMutual: 'wzajemne',
        connectivitySummaryOneWay: 'jednokierunkowe',
        connectivityTablePeer: 'Punkt',
        connectivityTableType: 'Typ',
        connectivityTableOut: 'A->B',
        connectivityTableIn: 'B->A',
        connectivityTableAge: 'Ostatnio',
        connectivityTableSignal: 'SNR',
        relationTypeOut: 'ode mnie',
        relationTypeIn: 'do mnie',
        relationTypeMutual: 'obie strony',
        staleShort: 'stare',
        routeSource: 'Start',
        routeTarget: 'Cel',
        routeSwap: 'Zamień',
        routeForward: 'A->B',
        routeBackward: 'B->A',
        routePickHint: 'Wybierz na mapie',
        routeSelectedA: 'A',
        routeSelectedB: 'B',
        routeUnset: 'nie ustawiono',
        routeStatusYes: 'trasa dostępna',
        routeStatusNo: 'brak trasy',
        routeNoSelection: 'Ustaw A i B.',
        routeSameNode: 'Start i cel muszą być różne.',
        routeNoPath: 'Brak trasy.',
        routeHopCount: 'hopów',
        routeUsesStale: 'użyto starych linków',
        routeFreshOnly: 'świeże linki',
        languageLabel: 'Język',
        sheetExpand: 'Rozwiń',
        sheetCollapse: 'Zwin',
        toolbarMapTitle: 'Mapa sieci',
        toolbarMapSubtitle: 'Wybierz punkt z mapy lub listy, aby zobaczyć jego bezpośrednie połączenia.',
        toolbarNewTitle: 'Nowe punkty',
        toolbarNewSubtitle: 'Punkty wykryte po raz pierwszy w ostatnich 24 godzinach.',
        toolbarConnectivityTitle: 'Łączność',
        toolbarConnectivitySubtitle: 'Sprawdź, kto widzi wybrany punkt i kogo widzi on.',
        toolbarRouteTitle: 'Trasy',
        toolbarRouteSubtitle: 'Wybierz start i cel. Najpierw pokażemy wynik, potem szczegóły.',
        newRepeaters: 'Nowe punkty 24h',
        emptyNoNewRepeaters: 'Brak nowych punktów wykrytych w ostatnich 24 godzinach.',
        routeTapTarget: 'Wybierz na mapie start albo cel.',
        routeTapTargetSource: 'Kliknij punkt na mapie, aby ustawić start.',
        routeTapTargetTarget: 'Kliknij punkt na mapie, aby ustawić cel.',
        routeTapTargetReady: 'Kliknij punkt na mapie, aby zmienić start albo cel.',
        roleDefault: 'Repeater',
        kindSignal: 'sygnał',
        noDataShort: 'b/d',
        loadingSignalHistory: 'Ładowanie historii sygnału...',
        storedSamples: (count) => `Dla tego połączenia zapisano na razie ${count} prób${count === 1 ? 'kę' : count < 5 ? 'ki' : 'ek'}. Wykres pojawi się po zebraniu co najmniej 2 próbek.`,
        agoSeconds: (count) => `${count}s temu`,
        agoMinutes: (count) => `${count} min temu`,
        agoHours: (count) => `${count} h temu`,
        agoDays: (count) => `${count} d temu`,
      },
      en: {
        unknown: 'unknown',
        legendRepeaters: 'Repeaters',
        legendLinks: 'Links',
        legendDataAvailable: 'data available',
        legendKnownNoData: 'known / no data fetched',
        legendInactive: 'inactive > 24h',
        legendStrong: 'strong',
        legendMedium: 'medium',
        legendWeak: 'weak',
        legendVeryWeak: 'very weak',
        legendDashed: 'stale data',
        legendArrow: 'direction',
        summaryKnown: 'known',
        summaryNew: 'new 24h',
        summaryWithData: 'with data',
        summaryPending: 'no data',
        summaryInactive: 'inactive',
        archivedToggle: '>24h',
        archivedToggleCount: (count) => `>24h ${count}`,
        archivedAutoFallback: 'No active nodes were seen in the last 24 hours. Showing archived ones so the map does not stay empty.',
        answerSelectedRepeater: 'Selected node',
        mobileMapTitle: 'Links on map',
        mobileMapEmpty: 'Select a node to show relations on the map.',
        mobileMapVisible: 'visible',
        mobileMapListTitle: 'Closest relations',
        mobileMapNoRows: 'No relations for this mode.',
        mobileMapPickRepeater: 'Select a node and relation direction.',
        mobileMapDirectionOut: 'Map: Out',
        mobileMapDirectionIn: 'Map: Seen by',
        mobileAnalysisWidze: 'Out',
        mobileAnalysisWidza: 'Seen by',
        mobileAnalysisMutual: 'Mutual',
        mobileAnalysisRoute: 'Route',
        connectivityStateOut: (count) => `${count} direct outgoing relations.`,
        connectivityStateIn: (count) => `${count} nodes can see this node.`,
        connectivityStateMutual: (count) => `${count} mutual relations.`,
        connectivityStateNoOwnData: 'This node has no own neighbor snapshot yet. We can only show who can see it.',
        connectivityStateNoVisible: 'No relations match the current view.',
        routeStateIdle: 'Set source and target. We will show both directions immediately.',
        routeStatePickTarget: 'Pick a target to calculate both directions.',
        routeStatePickSource: 'Pick a source to calculate the route to the selected target.',
        routeStateReady: 'Both directions are shown independently when available.',
        routeStateSameNode: 'A and B must point to different nodes.',
        routeResultsTitle: 'Route result',
        routeReachabilityTitle: 'Suggested targets from A',
        routeReachabilityIdle: 'Set a source and we will show which targets are reachable one-way.',
        routeReachabilitySummary: (count) => `${count} reachable destination${count === 1 ? '' : 's'} from A.`,
        routeReachabilityEmpty: 'No known one-way destinations are reachable from the selected A yet.',
        routeReachabilityFreshShort: 'fresh',
        routeReachabilityStaleShort: 'stale',
        routeReachabilityAction: 'Set as target',
        routeClearTarget: 'Clear target',
        routeProbePathTitle: 'Remembered path to target',
        routeProbePathSaved: 'from successful fetch',
        routeProbePathAdvert: 'from latest advert',
        routeProbePathNoStored: 'No remembered direct path to this target is stored yet.',
        routeProbePathFallback: 'The latest fetch may have used flood routing or the response did not return a reusable path.',
        routeProbePathObserved: 'stored',
        routeProbePathSource: 'source',
        routeProbePathEndpoint: 'target',
        routeProbePathBot: 'BOT',
        routeProbePathTarget: 'B',
        routeProbePathUnknownHop: (prefix) => `hop ${prefix}`,
        routeProbePathAmbiguousHop: (prefix, count) => `${prefix} (${count} matches)`,
        routeHistoricalRoute: 'historical route',
        routeHistoricalLinks: 'historical links',
        routeHistoryFallback: 'Current links no longer provide a route, but an older route still exists in history.',
        statusData: 'ready',
        statusNoData: 'no data',
        statusInactive: 'inactive',
        probeFailedAfterData: 'failed after data snapshot',
        probeDataSaved: 'data saved',
        probePending: 'waiting for data',
        signalMissing: 'signal: n/a',
        distanceMissing: 'dist: -',
        distancePrefix: 'dist',
        lastAdvertLabel: 'last seen',
        lastDataLabel: 'neighbor data',
        chartHistory: 'history',
        chartLatest: 'latest',
        chartSNRHistory: 'SNR history',
        chartNow: 'now',
        emptySelectRepeater: 'Select a node to inspect its direct links.',
        emptySelectNeighbor: 'Select a neighbor row to inspect signal history.',
        emptyNoNeighborLinks: 'No stored neighbor links are available yet for this node.',
        emptyNoOtherRepeaters: 'No other nodes available.',
        emptyNoSearchResults: 'No nodes match the current filter.',
        inspection: 'Node details',
        clearFocus: 'Clear selection',
        probeQueueTitle: 'Fetch data',
        probeQueueAction: 'Queue',
        probeQueueBusy: 'Queuing...',
        probeQueueQueued: 'queued',
        probeQueuePending: 'pending',
        probeQueueRunning: 'running',
        probeQueueCooldown: 'limit',
        probeQueueError: 'error',
        probeQueueHintQueuedNow: 'Queued as next.',
        probeQueueHintQueuedAt: (when) => `Queued, not before ${when}.`,
        probeQueueHintPendingNow: 'Already pending.',
        probeQueueHintPendingAt: (when) => `Pending, not before ${when}.`,
        probeQueueHintRunning: 'Fetch already running.',
        probeQueueHintCooldown: 'Skipped by cooldown or queue limit.',
        probeQueueHintError: 'Unable to queue the job.',
        role: 'Role',
        firstSeen: 'First seen',
        firstSeenLabel: 'first seen',
        lastAdvert: 'Last seen',
        lastData: 'Neighbor data',
        lastSuccessfulProbe: 'Last successful fetch',
        lastProbeResult: 'Last probe result',
        lastProbeAttempt: 'Last probe attempt',
        directNeighbors: 'Direct links',
        mapNodePositionMissing: 'The map cannot draw links from this node because it has no valid GPS position.',
        mapNeighborPositionsMissing: (count) => `The map skips ${count} link${count === 1 ? '' : 's'} to neighbors without a valid GPS position.`,
        neighbor: 'Neighbor',
        lastSeen: 'Last seen',
        signal: 'Signal',
        distance: 'Distance',
        selectedRepeater: 'Selected node',
        otherRepeaters: 'Other nodes',
        repeaters: 'Nodes',
        sortLabel: 'Sort',
        searchLabel: 'Find node',
        searchPlaceholder: 'name, prefix, hex (min. 2 chars)',
        sortLastAdvert: 'last seen',
        sortLastData: 'last data fetch',
        sortAlphabetical: 'alphabetical',
        viewMap: 'Map',
        viewList: 'List',
        viewLabel: 'View',
        panelMap: 'Map',
        panelNew: 'New',
        panelConnectivity: 'Connectivity',
        panelRoute: 'Routes',
        panelAnalysis: 'Analysis',
        focusRepeater: 'Focus',
        relationModeOut: 'Out',
        relationModeIn: 'Seen by',
        relationModeMutual: 'Mutual',
        relationFilterAll: 'All',
        relationFilterTwoWay: 'Mutual',
        relationFilterOut: 'Out',
        relationFilterIn: 'In',
        relationDirectOut: 'directly seen',
        relationDirectIn: 'directly seeing me',
        relationNodeSees: (name) => `${name} sees`,
        relationNodeSeenBy: (name) => `${name} seen by`,
        relationNodeMutual: (name) => `${name} mutual`,
        connectivityHint: 'Select a node.',
        connectivitySelect: 'Node',
        connectivityVisible: 'Visible relations',
        connectivityCountShort: 'rel.',
        connectivityNoRows: 'No relations match the current view.',
        connectivitySummaryTitle: 'Summary',
        connectivityVisibleTitle: 'Visible relations',
        connectivityFilterHint: 'Show one relation type at a time in compare mode.',
        connectivitySummaryOut: 'outgoing',
        connectivitySummaryIn: 'incoming',
        connectivitySummaryMutual: 'mutual',
        connectivitySummaryOneWay: 'one-way',
        connectivityTablePeer: 'Node',
        connectivityTableType: 'Type',
        connectivityTableOut: 'A->B',
        connectivityTableIn: 'B->A',
        connectivityTableAge: 'Last seen',
        connectivityTableSignal: 'SNR',
        relationTypeOut: 'from me',
        relationTypeIn: 'to me',
        relationTypeMutual: 'mutual',
        staleShort: 'stale',
        routeSource: 'Source',
        routeTarget: 'Target',
        routeSwap: 'Swap',
        routeForward: 'A->B',
        routeBackward: 'B->A',
        routePickHint: 'Pick on map',
        routeSelectedA: 'A',
        routeSelectedB: 'B',
        routeUnset: 'not set',
        routeStatusYes: 'route available',
        routeStatusNo: 'no route',
        routeNoSelection: 'Set A and B.',
        routeSameNode: 'Source and target must be different.',
        routeNoPath: 'No route available.',
        routeHopCount: 'hops',
        routeUsesStale: 'stale links used',
        routeFreshOnly: 'fresh links',
        languageLabel: 'Language',
        sheetExpand: 'Expand',
        sheetCollapse: 'Collapse',
        toolbarMapTitle: 'Network map',
        toolbarMapSubtitle: 'Select a node on the map or from the list to inspect direct links.',
        toolbarNewTitle: 'New nodes',
        toolbarNewSubtitle: 'Nodes first seen within the last 24 hours.',
        toolbarConnectivityTitle: 'Connectivity',
        toolbarConnectivitySubtitle: 'Check who can see the selected node and who it can see.',
        toolbarRouteTitle: 'Routes',
        toolbarRouteSubtitle: 'Pick source and target. We show the answer first, then the details.',
        newRepeaters: 'New nodes 24h',
        emptyNoNewRepeaters: 'No completely new nodes were first seen in the last 24 hours.',
        routeTapTarget: 'Pick source or target on the map.',
        routeTapTargetSource: 'Click a node on the map to set the source.',
        routeTapTargetTarget: 'Click a node on the map to set the target.',
        routeTapTargetReady: 'Click a node on the map to change source or target.',
        roleDefault: 'Repeater',
        kindSignal: 'signal',
        noDataShort: 'n/a',
        loadingSignalHistory: 'Loading signal history...',
        storedSamples: (count) => `Only ${count} stored sample${count === 1 ? '' : 's'} for this link so far. The history chart appears after at least 2 samples.`,
        agoSeconds: (count) => `${count}s ago`,
        agoMinutes: (count) => `${count}m ago`,
        agoHours: (count) => `${count}h ago`,
        agoDays: (count) => `${count}d ago`,
      },
    };
    const map = L.map('map', { zoomControl: true, preferCanvas: true }).setView([53.43, 14.55], 8);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
      subdomains: 'abcd',
      maxZoom: 20,
      attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
    }).addTo(map);
    const markersLayer = L.layerGroup().addTo(map);
    const halosLayer = L.layerGroup().addTo(map);
    const linksLayer = L.layerGroup().addTo(map);
    const labelsLayer = L.layerGroup().addTo(map);
    const linkLabelsLayer = L.layerGroup().addTo(map);
    let latestState = null;
    let latestManagement = null;
    let signalHistoryByNode = {};
    let selectedSourceId = null;
    let selectedNeighborId = null;
    let hoveredNodeId = null;
    let nodeSortMode = 'last_advert';
    let nodeSearchQuery = '';
    let currentLanguage = localStorage.getItem('meshcoreDashboardLanguage') || 'pl';
    let currentPanel = localStorage.getItem('meshcoreDashboardPanel') || 'map';
    let connectivityDirection = localStorage.getItem('meshcoreDashboardConnectivityDirection') || 'out';
    let connectivityFilter = '2way';
    let showArchived = localStorage.getItem('meshcoreDashboardShowArchived') === 'true';
    let routeSourceId = null;
    let routeTargetId = null;
    let routeActiveEndpoint = 'source';
    let hasFitBounds = false;
    let pendingRefreshState = null;
    let refreshTimerId = null;
    let refreshInFlight = null;
    let managementRefreshInFlight = null;
    let latestStateEtag = null;
    let latestManagementEtag = null;
    let latestManagementLoaded = false;
    let latestManagementIncludesHistorical = false;
    let signalHistoryRefreshInFlightByNode = new Map();
    let signalHistoryLoadedNodes = new Set();
    let signalHistoryPendingNodes = new Set();
    let sidebarSheetState = localStorage.getItem('meshcoreDashboardSheetState') || 'collapsed';
    let pendingMapClearSelectionKey = null;
    let pendingMapClearExpiresAt = 0;
    let restoreDoubleClickZoomTimer = null;
    let probeQueueFeedback = null;
    let probeQueueBusyNodeId = null;
    const BLANK_MAP_CLEAR_WINDOW_MS = 900;
    const DOUBLE_CLICK_ZOOM_RESTORE_MS = 260;
    const MIN_NODE_SEARCH_QUERY_LENGTH = 2;
    const IDLE_REFRESH_INTERVAL_MS = 300000;
    const ACTIVE_PROBE_REFRESH_INTERVAL_MS = 15000;
    const ERROR_REFRESH_INTERVAL_MS = 60000;

    function emptyManagementState() {
      return {
        has_active_probe_jobs: false,
        map_links: [],
        route_hints: {},
        historical_links: [],
      };
    }

    function mergeStateWithManagement(state, management = latestManagement) {
      if (!state) return null;
      return {
        ...state,
        management: {
          ...emptyManagementState(),
          ...(management || {}),
        },
      };
    }

    function commitState(state) {
      latestState = mergeStateWithManagement(state);
      return latestState;
    }

    function commitManagement(management, includesHistorical = false) {
      latestManagement = {
        ...emptyManagementState(),
        ...(management || {}),
      };
      latestManagementLoaded = true;
      latestManagementIncludesHistorical = includesHistorical;
      if (latestState) {
        latestState = mergeStateWithManagement(latestState, latestManagement);
      }
      return latestManagement;
    }

    function clearFocusedDataCache() {
      latestManagement = null;
      latestManagementEtag = null;
      latestManagementLoaded = false;
      latestManagementIncludesHistorical = false;
      signalHistoryByNode = {};
      signalHistoryRefreshInFlightByNode = new Map();
      signalHistoryLoadedNodes = new Set();
      signalHistoryPendingNodes = new Set();
      if (latestState) {
        latestState = mergeStateWithManagement(latestState, null);
      }
    }

    function selectedNodeNeedsManagement() {
      return Boolean(selectedSourceId && (currentPanel === 'map' || currentPanel === 'new'));
    }

    function currentPanelNeedsManagement() {
      return currentPanel === 'connectivity' || currentPanel === 'route' || selectedNodeNeedsManagement();
    }

    function selectedHistoryNodeKey(node) {
      if (!node) return null;
      return String(node.identity_hex || '');
    }

    function hasSignalHistoryLoaded(node) {
      const nodeKey = selectedHistoryNodeKey(node);
      if (!nodeKey) return false;
      return signalHistoryLoadedNodes.has(nodeKey);
    }

    function isSignalHistoryLoading(node) {
      const nodeKey = selectedHistoryNodeKey(node);
      if (!nodeKey) return false;
      return signalHistoryPendingNodes.has(nodeKey);
    }

    function strings() {
      return TRANSLATIONS[currentLanguage] || TRANSLATIONS.pl;
    }

    function tr(key) {
      return strings()[key];
    }

    function trFormat(key, value) {
      const entry = tr(key);
      return typeof entry === 'function' ? entry(value) : entry;
    }

    function normalizeSearchText(value) {
      return String(value || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .trim();
    }

    function effectiveNodeSearchQuery() {
      const query = normalizeSearchText(nodeSearchQuery);
      return query.length >= MIN_NODE_SEARCH_QUERY_LENGTH ? query : '';
    }

    function hasActiveNodeSearchQuery() {
      return Boolean(effectiveNodeSearchQuery());
    }

    function autoShowArchived(state) {
      if (currentPanel === 'new' || showArchived) return false;
      const nodes = state?.nodes || [];
      return nodes.length > 0 && nodes.every((node) => isInactive(node));
    }

    function archivedVisible(state) {
      return showArchived || autoShowArchived(state);
    }

    function nodeMatchesSearch(node) {
      const query = effectiveNodeSearchQuery();
      if (!query) return true;
      const haystack = normalizeSearchText(`${node.name || ''} ${node.hash_prefix_hex || ''} ${node.identity_hex || ''}`);
      return haystack.includes(query);
    }

    function isSidebarInteractionActive() {
      const activeElement = document.activeElement;
      if (!activeElement) return false;
      if (!activeElement.closest || !activeElement.closest('#sidebar')) return false;
      const tagName = activeElement.tagName;
      return tagName === 'SELECT' || tagName === 'OPTION' || tagName === 'INPUT' || tagName === 'TEXTAREA';
    }

    function flushPendingRefresh() {
      if (!pendingRefreshState || isSidebarInteractionActive()) return;
      const state = pendingRefreshState;
      pendingRefreshState = null;
      render(state);
    }

    function syncSidebarSheetState() {
      const sidebar = document.getElementById('sidebar');
      const toggle = document.getElementById('sheet-toggle');
      if (!sidebar || !toggle) return;
      if (!isPortraitMobileView()) {
        sidebar.classList.remove('sheet-collapsed', 'sheet-expanded');
        toggle.setAttribute('aria-expanded', 'true');
        const label = toggle.querySelector('.sheet-label');
        if (label) label.textContent = '';
        return;
      }
      sidebar.classList.toggle('sheet-collapsed', sidebarSheetState === 'collapsed');
      sidebar.classList.toggle('sheet-expanded', sidebarSheetState !== 'collapsed');
      toggle.setAttribute('aria-expanded', sidebarSheetState === 'collapsed' ? 'false' : 'true');
      const label = toggle.querySelector('.sheet-label');
      if (label) label.textContent = sidebarSheetState === 'collapsed' ? tr('sheetExpand') : tr('sheetCollapse');
      localStorage.setItem('meshcoreDashboardSheetState', sidebarSheetState);
    }

    function toggleSidebarSheet() {
      sidebarSheetState = sidebarSheetState === 'collapsed' ? 'expanded' : 'collapsed';
      syncSidebarSheetState();
    }

    function setLanguage(language) {
      if (!TRANSLATIONS[language]) return;
      currentLanguage = language;
      localStorage.setItem('meshcoreDashboardLanguage', language);
      document.documentElement.lang = language;
      renderLegend();
      if (latestState) render(latestState);
    }

    function isPortraitMobileView() {
      return window.matchMedia('(max-width: 860px) and (orientation: portrait)').matches;
    }

    function applyMobileView() {
      if (!isPortraitMobileView()) {
        document.body.dataset.mobileView = 'split';
        window.requestAnimationFrame(() => map.invalidateSize(false));
        return;
      }
      const view = currentPanel === 'map' ? 'map' : 'list';
      document.body.dataset.mobileView = view;
      if (view === 'map') {
        window.requestAnimationFrame(() => map.invalidateSize(false));
      }
    }

    function setPanel(panel) {
      if (!['map', 'new', 'connectivity', 'route'].includes(panel)) return;
      resetPendingMapClear();
      currentPanel = panel;
      latestManagementLoaded = false;
      if (panel === 'route' && !routeSourceId && selectedSourceId) {
        routeSourceId = selectedSourceId;
      }
      if (isPortraitMobileView()) {
        sidebarSheetState = panel === 'map' ? 'collapsed' : 'expanded';
      }
      localStorage.setItem('meshcoreDashboardPanel', panel);
      applyMobileView();
      if (latestState) render(latestState);
    }

    function hasOwnNeighborData(node) {
      return Boolean(node?.last_data_at);
    }

    function setConnectivityDirection(direction) {
      if (!['out', 'in', 'mutual'].includes(direction)) return;
      const node = latestState ? selectedConnectivityNode(latestState) : null;
      if ((direction === 'out' || direction === 'mutual') && node && !hasOwnNeighborData(node)) {
        return;
      }
      resetPendingMapClear();
      connectivityDirection = direction;
      localStorage.setItem('meshcoreDashboardConnectivityDirection', direction);
      if (latestState) render(latestState);
    }

    function setShowArchived(value) {
      resetPendingMapClear();
      showArchived = Boolean(value);
      localStorage.setItem('meshcoreDashboardShowArchived', showArchived ? 'true' : 'false');
      if (latestState) render(latestState);
    }

    function renderLegend() {
      const legend = document.getElementById('map-legend');
      legend.innerHTML = `
        <div class="legend-group">
          <span class="legend-title">${tr('legendRepeaters')}</span>
          <div class="legend-row"><span class="legend-node" style="background:#2e8b57"></span><span>${tr('legendDataAvailable')}</span></div>
          <div class="legend-row"><span class="legend-node" style="background:#2c71d1"></span><span>${tr('legendKnownNoData')}</span></div>
          <div class="legend-row"><span class="legend-node" style="background:#c64a3d"></span><span>${tr('legendInactive')}</span></div>
        </div>
        <div class="legend-group">
          <span class="legend-title">${tr('legendLinks')}</span>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#2e8b57"></span><span>${tr('legendStrong')}</span></div>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#cfaa38"></span><span>${tr('legendMedium')}</span></div>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#db7d31"></span><span>${tr('legendWeak')}</span></div>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#c64a3d"></span><span>${tr('legendVeryWeak')}</span></div>
          <div class="legend-row"><span class="legend-arrow">➜</span><span>${tr('legendArrow')}</span></div>
          <div class="legend-row"><span class="legend-line dashed" style="border-top-color:#6a7883"></span><span>${tr('legendDashed')}</span></div>
        </div>
      `;
    }

    function formatWhen(value) {
      if (!value) return tr('unknown');
      return new Date(value).toLocaleString();
    }

    function formatShortWhen(value) {
      if (!value) return tr('unknown');
      return new Date(value).toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      });
    }

    function timeAgo(value) {
      if (!value) return tr('unknown');
      const elapsed = Math.max(0, Date.now() - new Date(value).getTime());
      const seconds = Math.floor(elapsed / 1000);
      if (seconds < 60) return tr('agoSeconds')(seconds);
      if (seconds < 3600) return tr('agoMinutes')(Math.floor(seconds / 60));
      if (seconds < 86400) return tr('agoHours')(Math.floor(seconds / 3600));
      return tr('agoDays')(Math.floor(seconds / 86400));
    }

    function humanizeSeconds(value) {
      if (typeof value !== 'number' || !Number.isFinite(value)) return tr('unknown');
      if (value < 60) return `${Math.round(value)} s`;
      if (value < 3600) {
        const minutes = Math.floor(value / 60);
        const seconds = Math.round(value % 60);
        return seconds ? `${minutes} min ${seconds} s` : `${minutes} min`;
      }
      if (value < 86400) {
        const hours = Math.floor(value / 3600);
        const minutes = Math.floor((value % 3600) / 60);
        return minutes ? `${hours} h ${minutes} min` : `${hours} h`;
      }
      const days = Math.floor(value / 86400);
      const hours = Math.floor((value % 86400) / 3600);
      return hours ? `${days} d ${hours} h` : `${days} d`;
    }

    function isInactive(node) {
      if (!node.last_advert_at) return true;
      return Date.now() - new Date(node.last_advert_at).getTime() > ACTIVE_THRESHOLD_MS;
    }

    function isNewRepeater(node) {
      if (!node?.first_seen_at) return false;
      return Date.now() - new Date(node.first_seen_at).getTime() <= ACTIVE_THRESHOLD_MS;
    }

    function newRepeaterNodes(state) {
      return (state.nodes || []).filter((node) => isNewRepeater(node));
    }

    function nodeState(node) {
      if (isInactive(node)) return 'inactive';
      return node.data_fetch_ok ? 'ok' : 'missing';
    }

    function nodeStateRank(node) {
      const state = nodeState(node);
      if (state === 'ok') return 0;
      if (state === 'missing') return 1;
      return 2;
    }

    function nodeColor(node) {
      const state = nodeState(node);
      if (state === 'ok') return '#2e8b57';
      if (state === 'missing') return '#2c71d1';
      return '#c64a3d';
    }

    function isFiniteCoordinate(latitude, longitude) {
      return Number.isFinite(latitude) && Number.isFinite(longitude) && !(Math.abs(latitude) < 0.01 && Math.abs(longitude) < 0.01);
    }

    function haversineKm(aLat, aLon, bLat, bLon) {
      const toRad = (value) => value * Math.PI / 180;
      const dLat = toRad(bLat - aLat);
      const dLon = toRad(bLon - aLon);
      const sa = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;
      return 6371 * 2 * Math.atan2(Math.sqrt(sa), Math.sqrt(1 - sa));
    }

    function median(values) {
      if (!values.length) return null;
      const sorted = values.slice().sort((left, right) => left - right);
      const middle = Math.floor(sorted.length / 2);
      return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }

    function deriveMapNodes(nodes) {
      const candidates = nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude));
      if (candidates.length <= 2) return candidates;
      const centerLat = median(candidates.map((node) => node.latitude));
      const centerLon = median(candidates.map((node) => node.longitude));
      return candidates.filter((node) => haversineKm(centerLat, centerLon, node.latitude, node.longitude) <= 1200);
    }

    function relevantNodes(state) {
      if (currentPanel === 'new') {
        return newRepeaterNodes(state);
      }
      const nodes = state.nodes || [];
      if (archivedVisible(state)) return nodes;
      return nodes.filter((node) => !isInactive(node));
    }

    function archivedNodeCount(state) {
      if (currentPanel === 'new') return 0;
      return (state.nodes || []).filter((node) => isInactive(node)).length;
    }

    function normalizeVisibleSelections(state) {
      const visibleIds = new Set(relevantNodes(state).map((node) => node.identity_hex));
      if (selectedSourceId && !visibleIds.has(selectedSourceId)) {
        selectedSourceId = null;
        selectedNeighborId = null;
      }
      if (selectedNeighborId && !visibleIds.has(selectedNeighborId)) {
        selectedNeighborId = null;
      }
      if (routeSourceId && !visibleIds.has(routeSourceId)) {
        routeSourceId = null;
      }
      if (routeTargetId && !visibleIds.has(routeTargetId)) {
        routeTargetId = null;
      }
      if (hoveredNodeId && !visibleIds.has(hoveredNodeId)) {
        hoveredNodeId = null;
      }
    }

    function connectivityData(state) {
      const nodes = sortNodes(relevantNodes(state));
      const nodeIndex = new Map(nodes.map((node) => [node.identity_hex, node]));
      const edges = [];
      const pairSet = new Set();
      for (const link of (state.management?.map_links || [])) {
        if (!nodeIndex.has(link.source_identity_hex) || !nodeIndex.has(link.target_identity_hex)) continue;
        if (link.source_identity_hex === link.target_identity_hex) continue;
        const ageSeconds = typeof link.last_heard_seconds === 'number'
          ? link.last_heard_seconds
          : Math.max(0, Math.floor((Date.now() - new Date(link.collected_at).getTime()) / 1000));
        const edge = {
          ...link,
          age_seconds: ageSeconds,
          stale: ageSeconds > LINK_STALE_SECONDS,
          mutual: false,
        };
        pairSet.add(`${edge.source_identity_hex}|${edge.target_identity_hex}`);
        edges.push(edge);
      }
      const historicalEdges = [];
      const historicalPairSet = new Set();
      for (const link of (state.management?.historical_links || [])) {
        if (!nodeIndex.has(link.source_identity_hex) || !nodeIndex.has(link.target_identity_hex)) continue;
        if (link.source_identity_hex === link.target_identity_hex) continue;
        const pairKey = `${link.source_identity_hex}|${link.target_identity_hex}`;
        if (pairSet.has(pairKey) || historicalPairSet.has(pairKey)) continue;
        const ageSeconds = typeof link.last_heard_seconds === 'number'
          ? link.last_heard_seconds
          : Math.max(0, Math.floor((Date.now() - new Date(link.collected_at).getTime()) / 1000));
        historicalEdges.push({
          ...link,
          age_seconds: ageSeconds,
          stale: true,
          historical: true,
          mutual: false,
        });
        historicalPairSet.add(pairKey);
      }
      for (const edge of edges) {
        edge.mutual = pairSet.has(`${edge.target_identity_hex}|${edge.source_identity_hex}`);
      }
      const relationMap = new Map(nodes.map((node) => [node.identity_hex, { outgoing: [], incoming: [], mutual: [], oneWayOutgoing: [], oneWayIncoming: [] }]));
      for (const edge of edges) {
        relationMap.get(edge.source_identity_hex)?.outgoing.push(edge);
        relationMap.get(edge.target_identity_hex)?.incoming.push(edge);
        if (edge.mutual) {
          relationMap.get(edge.source_identity_hex)?.mutual.push(edge);
        } else {
          relationMap.get(edge.source_identity_hex)?.oneWayOutgoing.push(edge);
          relationMap.get(edge.target_identity_hex)?.oneWayIncoming.push(edge);
        }
      }
      return {
        nodes,
        nodeIndex,
        edges,
        historicalEdges,
        relationMap,
        summary: {
          directed: edges.length,
          mutual: edges.filter((edge) => edge.mutual).length / 2,
          oneWay: edges.filter((edge) => !edge.mutual).length,
          stale: edges.filter((edge) => edge.stale).length,
          historical: historicalEdges.length,
        },
      };
    }

    function selectedConnectivityNode(state) {
      const data = connectivityData(state);
      return data.nodeIndex.get(selectedSourceId) || null;
    }

    function relationRows(state, nodeId, filter = null) {
      if (!nodeId) return [];
      const data = connectivityData(state);
      const relations = data.relationMap.get(nodeId);
      if (!relations) return [];
      const peers = new Map();
      for (const edge of relations.outgoing) {
        const row = peers.get(edge.target_identity_hex) || { peerId: edge.target_identity_hex, outEdge: null, inEdge: null };
        row.outEdge = edge;
        peers.set(edge.target_identity_hex, row);
      }
      for (const edge of relations.incoming) {
        const row = peers.get(edge.source_identity_hex) || { peerId: edge.source_identity_hex, outEdge: null, inEdge: null };
        row.inEdge = edge;
        peers.set(edge.source_identity_hex, row);
      }
      return Array.from(peers.values()).map((row) => {
        const peerNode = data.nodeIndex.get(row.peerId);
        const relationType = row.outEdge && row.inEdge ? '2way' : row.outEdge ? 'out' : 'in';
        const freshestAge = Math.min(
          row.outEdge?.age_seconds ?? Number.POSITIVE_INFINITY,
          row.inEdge?.age_seconds ?? Number.POSITIVE_INFINITY,
        );
        return {
          ...row,
          peerName: peerNode?.name || row.peerId.slice(0, 8),
          relationType,
          freshestAge: Number.isFinite(freshestAge) ? freshestAge : null,
          stale: Boolean(row.outEdge?.stale || row.inEdge?.stale),
        };
      }).filter((row) => {
        if (!filter) return true;
        return row.relationType === filter;
      }).sort((left, right) => {
        const typeRank = { '2way': 0, out: 1, in: 2 };
        if (typeRank[left.relationType] !== typeRank[right.relationType]) {
          return typeRank[left.relationType] - typeRank[right.relationType];
        }
        return left.peerName.localeCompare(right.peerName);
      });
    }

    function directRelationRows(state, nodeId, direction) {
      if (!nodeId) return [];
      const data = connectivityData(state);
      const relations = data.relationMap.get(nodeId);
      if (!relations) return [];
      const edges = direction === 'out' ? relations.outgoing : relations.incoming;
      return edges.map((edge) => {
        const peerId = direction === 'out' ? edge.target_identity_hex : edge.source_identity_hex;
        const peerNode = data.nodeIndex.get(peerId);
        return {
          peerName: peerNode?.name || peerId.slice(0, 8),
          relationType: direction,
          stale: Boolean(edge.stale),
          metricText: lineSignalMetric(edge).label,
          ageText: humanizeSeconds(edge.age_seconds),
        };
      }).sort((left, right) => left.peerName.localeCompare(right.peerName));
    }

    function routePath(edges, sourceId, targetId) {
      if (!sourceId || !targetId || sourceId === targetId) return null;
      const adjacency = new Map();
      for (const edge of edges) {
        const bucket = adjacency.get(edge.source_identity_hex) || [];
        bucket.push(edge);
        adjacency.set(edge.source_identity_hex, bucket);
      }
      for (const bucket of adjacency.values()) {
        bucket.sort((left, right) => ((right.snr ?? -999) - (left.snr ?? -999)) || (left.age_seconds - right.age_seconds));
      }
      const queue = [[sourceId]];
      const visited = new Set([sourceId]);
      while (queue.length) {
        const path = queue.shift();
        const current = path[path.length - 1];
        if (current === targetId) return path;
        for (const edge of (adjacency.get(current) || [])) {
          if (visited.has(edge.target_identity_hex)) continue;
          visited.add(edge.target_identity_hex);
          queue.push(path.concat(edge.target_identity_hex));
        }
      }
      return null;
    }

    function buildRouteResult(state, sourceId, targetId) {
      const data = connectivityData(state);
      const freshEdges = data.edges.filter((edge) => !edge.stale);
      const freshPath = routePath(freshEdges, sourceId, targetId);
      const path = freshPath || routePath(data.edges, sourceId, targetId);
      if (!path) {
        return { path: null, usesStale: false };
      }
      return { path, usesStale: !freshPath };
    }

    function buildHistoricalRouteResult(state, sourceId, targetId) {
      const data = connectivityData(state);
      const path = routePath(data.historicalEdges, sourceId, targetId);
      return { path, usesHistorical: Boolean(path) };
    }

    function buildRouteReachability(state, sourceId) {
      const data = connectivityData(state);
      if (!sourceId) {
        return { destinations: [], highlightIds: new Set(), treeEdges: [] };
      }
      const destinations = [];
      const highlightIds = new Set([sourceId]);
      const treeEdges = new Map();
      for (const node of data.nodes) {
        const targetId = node.identity_hex;
        if (!targetId || targetId === sourceId) continue;
        const routeResult = buildRouteResult(state, sourceId, targetId);
        if (!routeResult.path) continue;
        highlightIds.add(targetId);
        destinations.push({
          identityHex: targetId,
          name: node.name || node.hash_prefix_hex || targetId.slice(0, 8),
          hopCount: Math.max(0, routeResult.path.length - 1),
          usesStale: routeResult.usesStale,
        });
        for (let index = 0; index < routeResult.path.length - 1; index += 1) {
          const edgeSourceId = routeResult.path[index];
          const edgeTargetId = routeResult.path[index + 1];
          const edgeKey = `${edgeSourceId}:${edgeTargetId}`;
          const previous = treeEdges.get(edgeKey);
          if (!previous || (previous.usesStale && !routeResult.usesStale)) {
            treeEdges.set(edgeKey, { sourceId: edgeSourceId, targetId: edgeTargetId, usesStale: routeResult.usesStale });
          }
        }
      }
      destinations.sort((left, right) => (left.hopCount - right.hopCount) || (Number(left.usesStale) - Number(right.usesStale)) || left.name.localeCompare(right.name));
      return { destinations, highlightIds, treeEdges: [...treeEdges.values()] };
    }

    function getSelectedNode(state) {
      return (state.nodes || []).find((node) => node.identity_hex === selectedSourceId) || null;
    }

    function routeHintForNode(state, identityHex) {
      if (!identityHex) return null;
      return (state.management?.route_hints || {})[identityHex] || null;
    }

    function decodeHintPath(state, targetId, pathRow) {
      const targetNode = (state.nodes || []).find((node) => node.identity_hex === targetId) || null;
      const normalizedHex = String(pathRow?.path_hex || '').trim().toUpperCase();
      const pathLen = Number(pathRow?.path_len || 0);
      const prefixes = [];
      for (let index = 0; index < normalizedHex.length; index += 2) {
        const prefixHex = normalizedHex.slice(index, index + 2);
        if (prefixHex.length === 2) prefixes.push(prefixHex);
      }
      const steps = prefixes.slice(0, pathLen || prefixes.length).map((prefixHex) => {
        const matches = (state.nodes || []).filter((node) => String(node.identity_hex || '').startsWith(prefixHex));
        if (matches.length === 1) {
          return {
            kind: 'resolved',
            label: matches[0].name || matches[0].hash_prefix_hex || prefixHex,
          };
        }
        if (matches.length > 1) {
          return {
            kind: 'ambiguous',
            label: tr('routeProbePathAmbiguousHop')(prefixHex, matches.length),
          };
        }
        return {
          kind: 'unknown',
          label: trFormat('routeProbePathUnknownHop', prefixHex),
        };
      });
      return { steps, targetNode };
    }

    function getSelectedLinks(state) {
      if (!selectedSourceId) return [];
      return ((state.management?.map_links) || [])
        .filter((link) => link.source_identity_hex === selectedSourceId)
        .sort((left, right) => ((right.snr ?? -999) - (left.snr ?? -999)));
    }

    function getSelectedMapLinks(state) {
      return getSelectedLinks(state)
        .filter((link) => isFiniteCoordinate(link.source_latitude, link.source_longitude))
        .filter((link) => isFiniteCoordinate(link.target_latitude, link.target_longitude));
    }

    function selectedNeighborIds(state) {
      return new Set(getSelectedLinks(state).map((link) => link.target_identity_hex));
    }

    function nodeStateLabel(node) {
      const state = nodeState(node);
      if (state === 'ok') return tr('statusData');
      if (state === 'missing') return tr('statusNoData');
      return tr('statusInactive');
    }

    function compareIsoTimesDesc(leftValue, rightValue) {
      const leftTime = leftValue ? new Date(leftValue).getTime() : 0;
      const rightTime = rightValue ? new Date(rightValue).getTime() : 0;
      return rightTime - leftTime;
    }

    function compareNodeNames(left, right) {
      return (left.name || left.hash_prefix_hex).localeCompare(right.name || right.hash_prefix_hex);
    }

    function sortNodes(nodes) {
      return nodes.slice().sort((left, right) => {
        const rankDiff = nodeStateRank(left) - nodeStateRank(right);
        if (rankDiff !== 0) return rankDiff;

        if (nodeSortMode === 'alphabetical') {
          const nameDiff = compareNodeNames(left, right);
          if (nameDiff !== 0) return nameDiff;
          return compareIsoTimesDesc(left.last_advert_at, right.last_advert_at);
        }

        if (nodeSortMode === 'last_data') {
          const dataDiff = compareIsoTimesDesc(left.last_data_at, right.last_data_at);
          if (dataDiff !== 0) return dataDiff;
          const advertDiff = compareIsoTimesDesc(left.last_advert_at, right.last_advert_at);
          if (advertDiff !== 0) return advertDiff;
          return compareNodeNames(left, right);
        }

        const advertDiff = compareIsoTimesDesc(left.last_advert_at, right.last_advert_at);
        if (advertDiff !== 0) return advertDiff;
        const dataDiff = compareIsoTimesDesc(left.last_data_at, right.last_data_at);
        if (dataDiff !== 0) return dataDiff;
        return compareNodeNames(left, right);
      });
    }

    function listNodes(state) {
      const nodes = sortNodes(relevantNodes(state));
      if (currentPanel === 'connectivity' || currentPanel === 'route') return nodes;
      const filtered = nodes.filter((node) => nodeMatchesSearch(node));
      if (!selectedSourceId) return filtered;
      const selectedNode = nodes.find((node) => node.identity_hex === selectedSourceId);
      if (!selectedNode || filtered.some((node) => node.identity_hex === selectedNode.identity_hex)) {
        return filtered;
      }
      return [selectedNode].concat(filtered);
    }

    function overlayInsets(basePadding) {
      const insets = { top: basePadding, right: basePadding, bottom: basePadding, left: basePadding };
      const mapElement = document.getElementById('map');
      const sidebar = document.getElementById('sidebar');
      if (!mapElement || !sidebar) return insets;

      const mapRect = mapElement.getBoundingClientRect();
      const sidebarRect = sidebar.getBoundingClientRect();
      if (!mapRect.width || !mapRect.height || !sidebarRect.width || !sidebarRect.height) return insets;

      const horizontalMid = mapRect.left + (mapRect.width / 2);
      const verticalMid = mapRect.top + (mapRect.height / 2);
      const overlapRight = Math.max(0, mapRect.right - sidebarRect.left);
      const overlapLeft = Math.max(0, sidebarRect.right - mapRect.left);
      const overlapBottom = Math.max(0, mapRect.bottom - sidebarRect.top);
      const overlapTop = Math.max(0, sidebarRect.bottom - mapRect.top);

      if (sidebarRect.left >= horizontalMid - 40) {
        insets.right += overlapRight;
      } else if (sidebarRect.right <= horizontalMid + 40) {
        insets.left += overlapLeft;
      }

      if (sidebarRect.top >= verticalMid - 40) {
        insets.bottom += overlapBottom;
      } else if (sidebarRect.bottom <= verticalMid + 40) {
        insets.top += overlapTop;
      }

      return insets;
    }

    function offsetLatLngForInsets(latlng, zoom, insets) {
      const projected = map.project(latlng, zoom);
      const shifted = L.point(
        projected.x + ((insets.right - insets.left) / 2),
        projected.y + ((insets.bottom - insets.top) / 2),
      );
      return map.unproject(shifted, zoom);
    }

    function fitInitialBounds(bounds) {
      if (!bounds.length) return;
      const insets = overlayInsets(18);
      map.fitBounds(bounds, {
        paddingTopLeft: [insets.left, insets.top],
        paddingBottomRight: [insets.right, insets.bottom],
        maxZoom: 10,
      });
      hasFitBounds = true;
    }

    function fitSelectedRepeater(selectedNode, visibleNodes) {
      if (!selectedNode || !isFiniteCoordinate(selectedNode.latitude, selectedNode.longitude)) {
        if (visibleNodes.length) fitNodeCollection(visibleNodes, selectedSourceId);
        return;
      }
      const bounds = [[selectedNode.latitude, selectedNode.longitude]];
      for (const node of visibleNodes) {
        if (node.identity_hex === selectedSourceId) continue;
        bounds.push([node.latitude, node.longitude]);
      }
      const insets = overlayInsets(36);
      if (bounds.length > 1) {
        map.flyToBounds(bounds, {
          paddingTopLeft: [insets.left, insets.top],
          paddingBottomRight: [insets.right, insets.bottom],
          maxZoom: 12,
          duration: 0.6,
        });
        return;
      }
      const targetZoom = Math.max(map.getZoom(), 11);
      const centeredTarget = offsetLatLngForInsets([selectedNode.latitude, selectedNode.longitude], targetZoom, insets);
      map.flyTo(centeredTarget, targetZoom, { duration: 0.5 });
    }

    function fitNodeCollection(nodes, focusId = null) {
      const visible = nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude));
      if (!visible.length) return;
      const bounds = visible.map((node) => [node.latitude, node.longitude]);
      if (bounds.length === 1) {
        const targetNode = visible[0];
        const insets = overlayInsets(36);
        const targetZoom = Math.max(map.getZoom(), 11);
        const centeredTarget = offsetLatLngForInsets([targetNode.latitude, targetNode.longitude], targetZoom, insets);
        map.flyTo(centeredTarget, targetZoom, { duration: 0.5 });
        return;
      }
      const insets = overlayInsets(30);
      map.flyToBounds(bounds, {
        paddingTopLeft: [insets.left, insets.top],
        paddingBottomRight: [insets.right, insets.bottom],
        maxZoom: focusId ? 12 : 10,
        duration: 0.6,
      });
    }

    function focusConnectivitySelection(state) {
      const data = connectivityData(state);
      const focusId = selectedSourceId;
      if (!focusId) return;
      const focusNode = data.nodeIndex.get(focusId);
      const canInspectOwnData = hasOwnNeighborData(focusNode);
      let visibleIds = new Set([focusId]);
      if (connectivityDirection === 'out' && canInspectOwnData) {
        for (const edge of data.edges.filter((edge) => edge.source_identity_hex === focusId)) {
          visibleIds.add(edge.target_identity_hex);
        }
      } else if (connectivityDirection === 'in') {
        for (const edge of data.edges.filter((edge) => edge.target_identity_hex === focusId)) {
          visibleIds.add(edge.source_identity_hex);
        }
      } else if (canInspectOwnData) {
        for (const edge of data.edges.filter((edge) => edge.source_identity_hex === focusId && edge.mutual)) {
          visibleIds.add(edge.target_identity_hex);
        }
      }
      fitNodeCollection(data.nodes.filter((node) => visibleIds.has(node.identity_hex)), focusId);
    }

    function focusRouteSelection(state) {
      const data = connectivityData(state);
      const ids = new Set([routeSourceId, routeTargetId].filter(Boolean));
      if (!ids.size) return;
      if (routeSourceId) {
        const reachability = buildRouteReachability(state, routeSourceId);
        for (const destination of reachability.destinations) ids.add(destination.identityHex);
      }
      if (routeSourceId && routeTargetId && routeSourceId !== routeTargetId) {
        const forward = buildRouteResult(state, routeSourceId, routeTargetId);
        const backward = buildRouteResult(state, routeTargetId, routeSourceId);
        for (const identityHex of (forward.path || [])) ids.add(identityHex);
        for (const identityHex of (backward.path || [])) ids.add(identityHex);
      }
      fitNodeCollection(data.nodes.filter((node) => ids.has(node.identity_hex)), routeSourceId || routeTargetId);
    }

    function currentPanelCopy() {
      if (currentPanel === 'connectivity') {
        return { title: tr('toolbarConnectivityTitle'), subtitle: tr('toolbarConnectivitySubtitle') };
      }
      if (currentPanel === 'new') {
        return { title: tr('toolbarNewTitle'), subtitle: tr('toolbarNewSubtitle') };
      }
      if (currentPanel === 'route') {
        return { title: tr('toolbarRouteTitle'), subtitle: tr('toolbarRouteSubtitle') };
      }
      return { title: tr('toolbarMapTitle'), subtitle: tr('toolbarMapSubtitle') };
    }

    function routeSummaryValue(routeResult, historicalRouteResult = null) {
      if (routeResult?.path) return tr('routeStatusYes');
      if (historicalRouteResult?.path) return tr('routeHistoricalRoute');
      return tr('routeStatusNo');
    }

    function renderSummaryMetrics(cards, metricClass) {
      return cards.map((item) => `
        <div class="${metricClass}">
          <strong>${item.value}</strong>
          <span>${item.label}</span>
        </div>
      `).join('');
    }

    function buildPanelSummary(state) {
      const panelCopy = currentPanelCopy();
      const nodes = relevantNodes(state);
      const data = connectivityData(state);
      const selectedNode = selectedSourceId ? data.nodeIndex.get(selectedSourceId) || null : null;
      const defaultCards = [
        { label: currentPanel === 'new' ? tr('summaryNew') : tr('summaryKnown'), value: nodes.length },
        { label: tr('summaryWithData'), value: nodes.filter((node) => !isInactive(node) && node.data_fetch_ok).length },
        { label: tr('summaryPending'), value: nodes.filter((node) => !isInactive(node) && !node.data_fetch_ok).length },
        { label: tr('summaryInactive'), value: nodes.filter((node) => isInactive(node)).length },
      ];
      if (currentPanel === 'connectivity') {
        const node = selectedConnectivityNode(state);
        if (!node) {
          return { ...panelCopy, status: '', cards: defaultCards };
        }
        const relations = data.relationMap.get(node.identity_hex) || { outgoing: [], incoming: [] };
        return {
          title: panelCopy.title,
          subtitle: `${tr('selectedRepeater')}: ${node.name}`,
          status: connectivityModeLabel(node),
          cards: [
            { label: tr('connectivityVisible'), value: connectivityVisibleRows(state, node.identity_hex).length },
            { label: tr('connectivitySummaryOut'), value: relations.outgoing.length },
            { label: tr('connectivitySummaryIn'), value: relations.incoming.length },
            { label: tr('connectivitySummaryMutual'), value: relationRows(state, node.identity_hex, '2way').length },
          ],
        };
      }
      if (currentPanel === 'route') {
        const sourceNode = routeSourceId ? data.nodeIndex.get(routeSourceId) || null : null;
        const targetNode = routeTargetId ? data.nodeIndex.get(routeTargetId) || null : null;
        const hasRoutePair = Boolean(sourceNode && targetNode && routeSourceId !== routeTargetId);
        const reachability = sourceNode ? buildRouteReachability(state, routeSourceId) : null;
        const forward = hasRoutePair ? buildRouteResult(state, routeSourceId, routeTargetId) : null;
        const backward = hasRoutePair ? buildRouteResult(state, routeTargetId, routeSourceId) : null;
        const historicalForward = forward?.path || !hasRoutePair
          ? null
          : buildHistoricalRouteResult(state, routeSourceId, routeTargetId);
        const historicalBackward = backward?.path || !hasRoutePair
          ? null
          : buildHistoricalRouteResult(state, routeTargetId, routeSourceId);
        let subtitle = panelCopy.subtitle;
        let status = '';
        if (sourceNode && targetNode) {
          subtitle = `${tr('routeSource')}: ${sourceNode.name} | ${tr('routeTarget')}: ${targetNode.name}`;
          status = `${tr('routeForward')} / ${tr('routeBackward')}`;
        } else if (sourceNode) {
          subtitle = `${tr('routeSource')}: ${sourceNode.name}`;
          status = tr('routeStatePickTarget');
        } else if (targetNode) {
          subtitle = `${tr('routeTarget')}: ${targetNode.name}`;
          status = tr('routeStatePickSource');
        }
        return {
          title: panelCopy.title,
          subtitle,
          status,
          cards: [
            { label: tr('routeSelectedA'), value: sourceNode ? tr('statusData') : '-' },
            { label: tr('routeSelectedB'), value: targetNode ? tr('statusData') : '-' },
            hasRoutePair
              ? { label: tr('routeForward'), value: routeSummaryValue(forward, historicalForward) }
              : { label: tr('routeReachabilityFreshShort'), value: reachability ? reachability.destinations.length : '-' },
            hasRoutePair
              ? { label: tr('routeBackward'), value: routeSummaryValue(backward, historicalBackward) }
              : { label: tr('routeReachabilityStaleShort'), value: reachability ? reachability.destinations.filter((destination) => destination.usesStale).length : '-' },
          ],
        };
      }
      return {
        title: panelCopy.title,
        subtitle: selectedNode ? `${tr('selectedRepeater')}: ${selectedNode.name}` : panelCopy.subtitle,
        status: selectedNode ? nodeStateLabel(selectedNode) : '',
        cards: defaultCards,
      };
    }

    function renderSummary(state) {
      const summary = buildPanelSummary(state);
      document.getElementById('summary').innerHTML = `
        <div class="summary-head">
          <div class="summary-copy">
            <strong>${summary.title}</strong>
            <span>${summary.subtitle}</span>
          </div>
          ${summary.status ? `<span class="summary-badge">${summary.status}</span>` : ''}
        </div>
        <div class="summary-grid">${renderSummaryMetrics(summary.cards, 'summary-card')}</div>
      `;
    }

    function renderPrimaryTabs() {
      return `
        <div class="primary-toggle" role="group" aria-label="${tr('viewLabel')}">
          <button type="button" class="segmented-button${currentPanel === 'map' ? ' active' : ''}" data-panel="map">${tr('panelMap')}</button>
          <button type="button" class="segmented-button${currentPanel === 'connectivity' ? ' active' : ''}" data-panel="connectivity">${tr('panelConnectivity')}</button>
          <button type="button" class="segmented-button${currentPanel === 'route' ? ' active' : ''}" data-panel="route">${tr('panelRoute')}</button>
          <button type="button" class="segmented-button${currentPanel === 'new' ? ' active' : ''}" data-panel="new">${tr('panelNew')}</button>
        </div>
      `;
    }

    function renderAnalysisTabs() {
      return '';
    }

    function relationTypeLabel(type) {
      if (type === '2way') return tr('relationTypeMutual');
      if (type === 'out') return tr('relationTypeOut');
      return tr('relationTypeIn');
    }

    function connectivityModeLabel(node) {
      if (connectivityDirection === 'out') return tr('relationModeOut');
      if (connectivityDirection === 'in') return tr('relationModeIn');
      return tr('relationModeMutual');
    }

    function connectivityStateText(node, visibleCount, canInspectOwnData) {
      if (!canInspectOwnData) return tr('connectivityStateNoOwnData');
      if (visibleCount === 0) return tr('connectivityStateNoVisible');
      if (connectivityDirection === 'out') return trFormat('connectivityStateOut', visibleCount);
      if (connectivityDirection === 'in') return trFormat('connectivityStateIn', visibleCount);
      return trFormat('connectivityStateMutual', visibleCount);
    }

    function renderAnswerStrip(title, kicker, stateText, metrics = [], alert = false) {
      return `
        <div class="answer-strip">
          <div class="answer-head">
            <div class="answer-title">
              <strong>${title}</strong>
              <span class="answer-state${alert ? '' : ' muted'}">${stateText}</span>
            </div>
            ${kicker ? `<span class="answer-kicker${alert ? ' alert' : ''}">${kicker}</span>` : ''}
          </div>
          ${metrics.length ? `<div class="answer-metrics">${metrics.map((metric) => `<span class="answer-stat"><strong>${metric.value}</strong><span>${metric.label}</span></span>`).join('')}</div>` : ''}
        </div>
      `;
    }

    function renderExpandablePanel(title, body, open = false) {
      return `
        <details class="panel-details"${open ? ' open' : ''}>
          <summary>${title}</summary>
          <div class="panel-details-body">${body}</div>
        </details>
      `;
    }

    function activeRouteHint() {
      if (routeActiveEndpoint === 'source') return tr('routeTapTargetSource');
      if (routeActiveEndpoint === 'target') return tr('routeTapTargetTarget');
      return tr('routeTapTargetReady');
    }

    function renderMobileOverview(state) {
      const summary = buildPanelSummary(state);
      return `
        <div class="mobile-overview-card">
          <div class="mobile-overview-head">
            <div class="mobile-overview-copy">
              <strong>${summary.title}</strong>
              <span>${summary.subtitle}</span>
            </div>
            ${summary.status ? `<span class="mobile-overview-status">${summary.status}</span>` : ''}
          </div>
          <div class="mobile-overview-grid">${renderSummaryMetrics(summary.cards, 'mobile-overview-metric')}</div>
        </div>
      `;
    }

    function connectivityVisibleRows(state, nodeId) {
      if (!nodeId) return [];
      const node = connectivityData(state).nodeIndex.get(nodeId);
      const canInspectOwnData = hasOwnNeighborData(node);
      if (connectivityDirection === 'out') {
        if (!canInspectOwnData) return [];
        return directRelationRows(state, nodeId, 'out');
      }
      if (connectivityDirection === 'in') {
        return directRelationRows(state, nodeId, 'in');
      }
      if (!canInspectOwnData) return [];
      const filtered = relationRows(state, nodeId, '2way').map((row) => ({
        peerName: row.peerName,
        relationType: row.relationType,
        stale: row.stale,
        metricText: `${tr('connectivityTableOut')}: ${row.outEdge ? lineSignalMetric(row.outEdge).short : '-'}`,
        ageText: row.freshestAge === null ? '-' : humanizeSeconds(row.freshestAge),
        secondaryText: `${tr('connectivityTableIn')}: ${row.inEdge ? lineSignalMetric(row.inEdge).short : '-'}`,
      }));
      return filtered;
    }

    function mobileMapRows(state, nodeId) {
      if (!nodeId) return [];
      const data = connectivityData(state);
      const node = data.nodeIndex.get(nodeId);
      const canInspectOwnData = hasOwnNeighborData(node);
      const edges = connectivityDirection === 'out'
        ? (canInspectOwnData ? data.edges.filter((edge) => edge.source_identity_hex === nodeId) : [])
        : data.edges.filter((edge) => edge.target_identity_hex === nodeId);
      return edges.map((edge) => {
        const peerId = connectivityDirection === 'out' ? edge.target_identity_hex : edge.source_identity_hex;
        const peerNode = data.nodeIndex.get(peerId);
        return {
          peerId,
          peerName: peerNode?.name || peerId.slice(0, 8),
          stale: Boolean(edge.stale),
          metricText: lineSignalMetric(edge).short,
          ageText: humanizeSeconds(edge.age_seconds),
        };
      }).sort((left, right) => left.peerName.localeCompare(right.peerName));
    }

    function renderMobileMapPanel(state) {
      const data = connectivityData(state);
      const node = selectedConnectivityNode(state);
      const nodeOptions = data.nodes.map((candidate) => `<option value="${candidate.identity_hex}">${candidate.name}</option>`).join('');
      const selector = `
        <div class="field-stack">
          <label for="mobile-map-node">${tr('connectivitySelect')}</label>
          <select id="mobile-map-node" class="route-select" data-focus-node="1">
            <option value=""></option>
            ${nodeOptions}
          </select>
        </div>
      `;
      const canInspectOwnData = !node || hasOwnNeighborData(node);
      if (node && !canInspectOwnData && connectivityDirection === 'out') {
        connectivityDirection = 'in';
      }
      const directionButtons = `
        <div class="secondary-toggle" role="group" aria-label="${tr('panelMap')}">
          <button type="button" class="segmented-button${connectivityDirection === 'out' ? ' active' : ''}" data-connectivity-direction="out"${canInspectOwnData ? '' : ' disabled'}>${tr('relationModeOut')}</button>
          <button type="button" class="segmented-button${connectivityDirection === 'in' ? ' active' : ''}" data-connectivity-direction="in">${tr('relationModeIn')}</button>
        </div>
      `;
      if (!node) {
        return `<div class="mobile-map-stack">${selector}${directionButtons}${renderAnswerStrip(tr('mobileMapTitle'), '', tr('mobileMapPickRepeater'))}</div>`;
      }
      const rows = mobileMapRows(state, node.identity_hex);
      const listHtml = rows.length
        ? `<div class="mobile-relation-list">${rows.slice(0, 5).map((row) => `
            <button type="button" class="mobile-relation-button${selectedNeighborId === row.peerId ? ' active' : ''}" data-mobile-peer="${row.peerId}">
              <span class="mobile-relation-main">
                <strong>${row.peerName}</strong>
                <span>${row.metricText}</span>
                <span>${tr('connectivityTableAge')}: ${row.ageText}</span>
              </span>
              <span class="mobile-relation-meta">
                ${row.stale ? `<span class="stale-chip">${tr('staleShort')}</span>` : '<span></span>'}
              </span>
            </button>
          `).join('')}</div>`
        : `<div class="compact-note"><strong>${tr('mobileMapListTitle')}</strong>${tr('mobileMapNoRows')}</div>`;
      const directionLabel = connectivityDirection === 'out' ? tr('mobileMapDirectionOut') : tr('mobileMapDirectionIn');
      return `
        <div class="mobile-map-stack">
          ${selector}
          ${directionButtons}
          <div class="mobile-summary-card">
            <div class="mobile-summary-head">
              <div class="mobile-summary-title">
                <strong>${node.name}</strong>
                <span>${directionLabel}</span>
              </div>
              <span class="mobile-summary-count">${rows.length} ${tr('mobileMapVisible')}</span>
            </div>
            ${listHtml}
          </div>
        </div>
      `;
    }

    function renderRelationList(rows) {
      if (!rows.length) {
        return `<div class="compact-note"><strong>${tr('connectivityVisibleTitle')}</strong>${tr('connectivityNoRows')}</div>`;
      }
      return `
        <div class="relation-list">
          ${rows.map((row) => `
            <div class="relation-item">
              <div class="relation-main">
                <strong>${row.peerName}</strong>
                <span>${row.metricText}</span>
                <span>${tr('connectivityTableAge')}: ${row.ageText}</span>
                ${row.secondaryText ? `<span>${row.secondaryText}</span>` : ''}
              </div>
              <div class="relation-badges">
                <span class="direction-chip">${relationTypeLabel(row.relationType)}</span>
                ${row.stale ? `<span class="stale-chip">${tr('staleShort')}</span>` : ''}
              </div>
            </div>
          `).join('')}
        </div>
      `;
    }

    function renderConnectivityPanel(state) {
      const data = connectivityData(state);
      const node = selectedConnectivityNode(state);
      const nodeOptions = data.nodes.map((candidate) => `<option value="${candidate.identity_hex}">${candidate.name}</option>`).join('');
      const selector = `
        <div class="field-stack">
          <label for="connectivity-node">${tr('connectivitySelect')}</label>
          <select id="connectivity-node" class="route-select" data-focus-node="1">
            <option value=""></option>
            ${nodeOptions}
          </select>
        </div>
      `;
      if (!node) {
        return `<div class="panel-stack"><div class="panel-section">${selector}${renderAnswerStrip(tr('panelConnectivity'), '', tr('connectivityHint'))}</div></div>`;
      }
      const mutualRows = relationRows(state, node.identity_hex, '2way');
      const relations = data.relationMap.get(node.identity_hex) || { outgoing: [], incoming: [], mutual: [], oneWayOutgoing: [], oneWayIncoming: [] };
      const canInspectOwnData = hasOwnNeighborData(node);
      if (!canInspectOwnData && connectivityDirection !== 'in') {
        connectivityDirection = 'in';
      }
      const directionButtons = `
        <div class="secondary-toggle" role="group" aria-label="${tr('panelConnectivity')}">
          <button type="button" class="segmented-button${connectivityDirection === 'out' ? ' active' : ''}" data-connectivity-direction="out"${canInspectOwnData ? '' : ' disabled'}>${tr('relationModeOut')}</button>
          <button type="button" class="segmented-button${connectivityDirection === 'in' ? ' active' : ''}" data-connectivity-direction="in">${tr('relationModeIn')}</button>
          <button type="button" class="segmented-button${connectivityDirection === 'mutual' ? ' active' : ''}" data-connectivity-direction="mutual"${canInspectOwnData ? '' : ' disabled'}>${tr('relationModeMutual')}</button>
        </div>
      `;
      const visibleRows = connectivityVisibleRows(state, node.identity_hex);
      const heroCount = visibleRows.length;
      const summaryMetrics = [
        { value: relations.outgoing.length, label: tr('connectivitySummaryOut') },
        { value: relations.incoming.length, label: tr('connectivitySummaryIn') },
        { value: mutualRows.length, label: tr('connectivitySummaryMutual') },
      ];
      return `
        <div class="panel-stack">
          <div class="panel-section">
            ${selector}
            ${isPortraitMobileView() ? '' : directionButtons}
            ${renderAnswerStrip(node.name, connectivityModeLabel(node), connectivityStateText(node, heroCount, canInspectOwnData), summaryMetrics, !canInspectOwnData)}
          </div>
          <div class="panel-section">
            <div class="panel-section-head"><span class="panel-section-title">${tr('connectivityVisibleTitle')}</span><span class="panel-section-note">${heroCount} ${tr('connectivityCountShort')}</span></div>
            ${renderRelationList(visibleRows)}
          </div>
        </div>
      `;
    }

    function routeSummaryCard(title, routeResult, data, historicalRouteResult = null) {
      const directionClass = title === tr('routeForward') ? 'forward' : 'backward';
      if (!routeResult.path) {
        if (historicalRouteResult?.path) {
          const historicalHtml = historicalRouteResult.path.map((identityHex) => {
            const node = data.nodeIndex.get(identityHex);
            const name = node?.name || identityHex.slice(0, 8);
            return `<div class="route-hop-row"><span class="route-step">${name}</span></div>`;
          }).join('');
          return `
            <div class="route-card">
              <div class="route-card-head"><strong>${title}</strong><span class="route-direction-chip ${directionClass}">${title}</span></div>
              <div class="route-status-row"><span class="route-status-badge no">${tr('routeHistoricalRoute')}</span><span class="route-meta">${Math.max(0, historicalRouteResult.path.length - 1)} ${tr('routeHopCount')}, ${tr('routeHistoricalLinks')}</span></div>
              <div class="route-empty"><strong>${tr('routeHistoryFallback')}</strong></div>
              <div class="route-path">${historicalHtml}</div>
            </div>
          `;
        }
        return `<div class="route-card"><div class="route-card-head"><strong>${title}</strong><span class="route-direction-chip ${directionClass}">${title}</span></div><div class="route-status-row"><span class="route-status-badge no">${tr('routeStatusNo')}</span></div><div class="route-empty"><strong>${tr('routeNoPath')}</strong><span>${tr('routePickHint')}</span></div></div>`;
      }
      const pathHtml = routeResult.path.map((identityHex, index) => {
        const node = data.nodeIndex.get(identityHex);
        const name = node?.name || identityHex.slice(0, 8);
        return `<div class="route-hop-row"><span class="route-step">${name}</span></div>`;
      }).join('');
      return `
        <div class="route-card">
          <div class="route-card-head"><strong>${title}</strong><span class="route-direction-chip ${directionClass}">${title}</span></div>
          <div class="route-status-row"><span class="route-status-badge ok">${tr('routeStatusYes')}</span><span class="route-meta">${Math.max(0, routeResult.path.length - 1)} ${tr('routeHopCount')}${routeResult.usesStale ? `, ${tr('routeUsesStale')}` : `, ${tr('routeFreshOnly')}`}</span></div>
          <div class="route-path">${pathHtml}</div>
        </div>
      `;
    }

    function renderRouteProbePathSection(state) {
      if (!routeTargetId) {
        return '';
      }
      const hint = routeHintForNode(state, routeTargetId);
      const savedPath = hint?.latest_saved_path || null;
      const advertPath = hint?.latest_advert_path || null;
      const chosenPath = savedPath || advertPath;
      const latestProbeRun = hint?.latest_probe_run || null;
      if (!chosenPath) {
        const message = latestProbeRun?.result === 'success'
          ? `${tr('routeProbePathNoStored')} ${tr('routeProbePathFallback')}`
          : tr('routeProbePathNoStored');
        const metrics = latestProbeRun?.endpoint_name
          ? [{ value: latestProbeRun.endpoint_name, label: tr('routeProbePathEndpoint') }]
          : [];
        return `<div class="panel-section">${renderExpandablePanel(tr('routeProbePathTitle'), renderAnswerStrip(tr('routeProbePathTitle'), '', message, metrics, true))}</div>`;
      }
      const decoded = decodeHintPath(state, routeTargetId, chosenPath);
      const pathSteps = [
        `<span class="route-hint-step">${tr('routeProbePathBot')}</span>`,
        ...decoded.steps.map((step) => `<span class="route-hint-arrow">&rarr;</span><span class="route-hint-step${step.kind === 'resolved' ? '' : ' uncertain'}">${step.label}</span>`),
        `<span class="route-hint-arrow">&rarr;</span><span class="route-hint-step">${decoded.targetNode?.name || tr('routeProbePathTarget')}</span>`,
      ].join('');
      const chips = [
        `<span class="route-hint-chip">${savedPath ? tr('routeProbePathSaved') : tr('routeProbePathAdvert')}</span>`,
      ];
      if (chosenPath.source) {
        chips.push(`<span class="route-hint-chip">${tr('routeProbePathSource')}: ${chosenPath.source}</span>`);
      }
      if (chosenPath.endpoint_name) {
        chips.push(`<span class="route-hint-chip">${tr('routeProbePathEndpoint')}: ${chosenPath.endpoint_name}</span>`);
      }
      if (chosenPath.observed_at) {
        chips.push(`<span class="route-hint-chip">${tr('routeProbePathObserved')}: ${formatShortWhen(chosenPath.observed_at)}</span>`);
      }
      const note = latestProbeRun?.result === 'success' && advertPath && !savedPath
        ? tr('routeProbePathFallback')
        : '';
      return `
        <div class="panel-section">
          ${renderExpandablePanel(
            tr('routeProbePathTitle'),
            `${renderAnswerStrip(tr('routeProbePathTitle'), '', savedPath ? tr('routeProbePathSaved') : tr('routeProbePathAdvert'), [{ value: Number(chosenPath.path_len || 0), label: tr('routeHopCount') }])}
            <div class="route-hint-shell">
              <div class="route-hint-meta">${chips.join('')}</div>
              <div class="route-hint-path">${pathSteps}</div>
              ${note ? `<div class="route-hint-note">${note}</div>` : ''}
            </div>`
          )}
        </div>
      `;
    }

    function renderRouteReachabilitySection(state) {
      if (routeTargetId) {
        return '';
      }
      if (!routeSourceId) {
        return `<div class="panel-section">${renderAnswerStrip(tr('routeReachabilityTitle'), '', tr('routeReachabilityIdle'))}</div>`;
      }
      const reachability = buildRouteReachability(state, routeSourceId);
      const freshCount = reachability.destinations.filter((destination) => !destination.usesStale).length;
      const staleCount = reachability.destinations.length - freshCount;
      if (!reachability.destinations.length) {
        return `<div class="panel-section">${renderExpandablePanel(
          tr('routeReachabilityTitle'),
          `${renderAnswerStrip(tr('routeReachabilityTitle'), '', tr('routeReachabilityEmpty'), [{ value: 0, label: tr('routeReachabilityFreshShort') }, { value: 0, label: tr('routeReachabilityStaleShort') }], true)}
          <div class="route-destination-empty"><strong>${tr('routeReachabilityEmpty')}</strong><span>${tr('routePickHint')}</span></div>`
        )}</div>`;
      }
      const destinationHtml = reachability.destinations.map((destination) => `
        <button type="button" class="route-destination-item${routeTargetId === destination.identityHex ? ' active' : ''}" data-route-destination="${destination.identityHex}">
          <span class="route-destination-main">
            <strong>${destination.name}</strong>
            <span>${destination.hopCount} ${tr('routeHopCount')}${destination.usesStale ? `, ${tr('routeUsesStale')}` : `, ${tr('routeFreshOnly')}`}</span>
          </span>
          <span class="route-destination-action">${routeTargetId === destination.identityHex ? tr('routeSelectedB') : tr('routeReachabilityAction')}</span>
        </button>
      `).join('');
      return `<div class="panel-section">${renderExpandablePanel(
        tr('routeReachabilityTitle'),
        `${renderAnswerStrip(tr('routeReachabilityTitle'), '', trFormat('routeReachabilitySummary', reachability.destinations.length), [{ value: freshCount, label: tr('routeReachabilityFreshShort') }, { value: staleCount, label: tr('routeReachabilityStaleShort') }])}
        <div class="route-destination-list">${destinationHtml}</div>`
      )}</div>`;
    }

    function renderRoutePanel(state) {
      const data = connectivityData(state);
      const options = data.nodes.map((node) => `<option value="${node.identity_hex}">${node.name}</option>`).join('');
      let body = '';
      if (routeSourceId && routeTargetId) {
        if (routeSourceId === routeTargetId) {
          body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateSameNode'), [], true)}</div>`;
        } else {
          const forward = buildRouteResult(state, routeSourceId, routeTargetId);
          const backward = buildRouteResult(state, routeTargetId, routeSourceId);
          const historicalForward = forward.path ? null : buildHistoricalRouteResult(state, routeSourceId, routeTargetId);
          const historicalBackward = backward.path ? null : buildHistoricalRouteResult(state, routeTargetId, routeSourceId);
          body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateReady'), [{ value: forward.path ? tr('routeStatusYes') : historicalForward?.path ? tr('routeHistoricalRoute') : '-', label: tr('routeForward') }, { value: backward.path ? tr('routeStatusYes') : historicalBackward?.path ? tr('routeHistoricalRoute') : '-', label: tr('routeBackward') }])}<div class="route-result-grid">${routeSummaryCard(tr('routeForward'), forward, data, historicalForward)}${routeSummaryCard(tr('routeBackward'), backward, data, historicalBackward)}</div></div>`;
        }
      } else if (routeSourceId) {
        body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStatePickTarget'))}</div>`;
      } else if (routeTargetId) {
        body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStatePickSource'))}</div>`;
      } else if (!routeSourceId && !routeTargetId) {
        body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateIdle'))}</div>`;
      }
      body += renderRouteReachabilitySection(state);
      body += renderRouteProbePathSection(state);
      const sourceName = data.nodeIndex.get(routeSourceId)?.name || '-';
      const targetName = data.nodeIndex.get(routeTargetId)?.name || '-';
      return `
        <div class="panel-stack">
          <div class="panel-section">
            <div class="route-picker-note"><strong>${activeRouteHint()}</strong></div>
            <div class="route-control-bar">
              <button type="button" class="route-endpoint${routeActiveEndpoint === 'source' ? ' active' : ''}" data-route-active="source">
                <span class="route-endpoint-label">${tr('routeSelectedA')}</span>
                <strong class="route-endpoint-name">${routeSourceId ? sourceName : tr('routeUnset')}</strong>
              </button>
              <div class="route-endpoint-stack">
                <button type="button" class="route-endpoint route-endpoint-target${routeActiveEndpoint === 'target' ? ' active' : ''}" data-route-active="target">
                  <span class="route-endpoint-label">${tr('routeSelectedB')}</span>
                  <strong class="route-endpoint-name">${routeTargetId ? targetName : tr('routeUnset')}</strong>
                </button>
                ${routeTargetId ? `<div class="route-actions"><button type="button" class="route-endpoint-clear" data-route-clear-target="1">${tr('routeClearTarget')}</button></div>` : ''}
              </div>
            </div>
            <div class="route-controls">
              <div class="field-stack">
                <label for="route-source">${tr('routeSource')}</label>
                <select id="route-source" class="route-select" data-route-source="1">
                  <option value=""></option>
                  ${options}
                </select>
              </div>
              <div></div>
              <div class="field-stack">
                <label for="route-target">${tr('routeTarget')}</label>
                <select id="route-target" class="route-select" data-route-target="1">
                  <option value=""></option>
                  ${options}
                </select>
              </div>
            </div>
          </div>
          ${body}
        </div>
      `;
    }

    function activeMapSelectionKey() {
      if (currentPanel === 'route') return null;
      if (!selectedSourceId && !selectedNeighborId) return null;
      return `${currentPanel}:${selectedSourceId || ''}:${selectedNeighborId || ''}`;
    }

    function resetPendingMapClear() {
      pendingMapClearSelectionKey = null;
      pendingMapClearExpiresAt = 0;
    }

    function armBlankMapClear() {
      const selectionKey = activeMapSelectionKey();
      if (!selectionKey) {
        resetPendingMapClear();
        return false;
      }
      const now = Date.now();
      const shouldClear = pendingMapClearSelectionKey === selectionKey && pendingMapClearExpiresAt > now;
      pendingMapClearSelectionKey = selectionKey;
      pendingMapClearExpiresAt = now + BLANK_MAP_CLEAR_WINDOW_MS;
      return shouldClear;
    }

    function suppressUpcomingDoubleClickZoom() {
      if (!map.doubleClickZoom.enabled()) return;
      map.doubleClickZoom.disable();
      if (restoreDoubleClickZoomTimer !== null) {
        window.clearTimeout(restoreDoubleClickZoomTimer);
      }
      restoreDoubleClickZoomTimer = window.setTimeout(() => {
        map.doubleClickZoom.enable();
        restoreDoubleClickZoomTimer = null;
      }, DOUBLE_CLICK_ZOOM_RESTORE_MS);
    }

    function selectNode(identityHex) {
      resetPendingMapClear();
      if (selectedSourceId === identityHex) {
        clearSelection();
        return;
      }
      selectedSourceId = identityHex;
      selectedNeighborId = null;
      if (!latestState) return;
      if (currentPanel === 'connectivity') {
        render(latestState);
        return;
      }
      const selectedNode = getSelectedNode(latestState);
      const allMapNodes = deriveMapNodes(sortNodes(relevantNodes(latestState)));
      const neighborIds = selectedNeighborIds(latestState);
      const visibleNodes = allMapNodes.filter((node) => node.identity_hex === selectedSourceId || neighborIds.has(node.identity_hex));
      fitSelectedRepeater(selectedNode, visibleNodes);
      render(latestState);
    }

    function clearSelection() {
      resetPendingMapClear();
      selectedSourceId = null;
      selectedNeighborId = null;
      if (!latestState) return;
      render(latestState);
    }

    async function queueProbeJob(repeaterId) {
      if (!latestState) return;
      const numericRepeaterId = Number(repeaterId);
      if (!Number.isFinite(numericRepeaterId)) return;
      const node = (latestState.nodes || []).find((item) => Number(item.id) === numericRepeaterId);
      if (!node) return;

      probeQueueBusyNodeId = node.identity_hex;
      if (latestState) render(latestState);

      try {
        const response = await fetch('/api/probe-jobs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            repeater_id: numericRepeaterId,
          }),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.detail || 'queue failed');
        }
        probeQueueFeedback = {
          identityHex: node.identity_hex,
          status: payload.status || 'error',
          scheduledAt: payload.scheduled_at || null,
        };
        await refresh(true);
        await refreshFocusedDataIfNeeded({ force: true });
      } catch (error) {
        probeQueueFeedback = {
          identityHex: node.identity_hex,
          status: 'error',
          scheduledAt: null,
        };
        if (latestState) render(latestState);
      } finally {
        probeQueueBusyNodeId = null;
        if (latestState) render(latestState);
      }
    }

    function lineSignalMetric(link) {
      if (typeof link.snr === 'number') {
        return { value: link.snr, label: `SNR ${link.snr.toFixed(1)} dB`, short: `SNR ${link.snr.toFixed(1)}`, kind: 'SNR' };
      }
      if (typeof link.rssi === 'number') {
        return { value: link.rssi, label: `RSSI ${link.rssi} dBm`, short: `RSSI ${link.rssi}`, kind: 'RSSI' };
      }
      return { value: null, label: tr('noDataShort'), short: tr('noDataShort'), kind: tr('kindSignal') };
    }

    function describeProbeResult(node) {
      if (node.last_probe_status === 'failed' && node.last_data_at) {
        return tr('probeFailedAfterData');
      }
      if (node.last_probe_status) {
        return node.last_probe_status;
      }
      return node.data_fetch_ok ? tr('probeDataSaved') : tr('probePending');
    }

    function linkLabel(link, sourceNode) {
      const metric = lineSignalMetric(link);
      const distance = neighborDistanceKm(sourceNode, link);
      const metricLine = metric.value !== null ? `${metric.kind}: ${metric.value.toFixed(1)} ${metric.kind === 'RSSI' ? 'dBm' : 'dB'}` : tr('signalMissing');
      const distanceLine = distance !== null ? `${tr('distancePrefix')}: ${distance.toFixed(1)} km` : tr('distanceMissing');
      return `<strong>${metricLine}</strong><span>${distanceLine}</span>`;
    }

    function lineColor(link) {
      const metric = lineSignalMetric(link);
      if (metric.value === null) return '#98a4ad';
      if (metric.value >= 10) return '#2e8b57';
      if (metric.value >= 5) return '#cfaa38';
      if (metric.value >= 0) return '#db7d31';
      return '#c64a3d';
    }

    function markerStyle(node, isolated, selected, neighbor) {
      const color = nodeColor(node);
      if (selected) {
        return { radius: 12, color: '#15212a', weight: 3.6, fillColor: color, fillOpacity: 1, opacity: 1 };
      }
      if (neighbor) {
        return { radius: 7.5, color, weight: 2, fillColor: color, fillOpacity: 0.9, opacity: 0.94 };
      }
      if (isolated) {
        return { radius: 4, color, weight: 1, fillColor: color, fillOpacity: 0.16, opacity: 0.2 };
      }
      return { radius: 5, color, weight: 1.2, fillColor: color, fillOpacity: 0.82, opacity: 0.85 };
    }

    function drawFocusHalo(node, strokeColor, fillColor, outerRadius = 18, innerRadius = 13) {
      if (!node || !isFiniteCoordinate(node.latitude, node.longitude)) return;
      L.circleMarker([node.latitude, node.longitude], {
        radius: outerRadius,
        color: strokeColor,
        weight: 1.4,
        fillColor,
        fillOpacity: 0.06,
        opacity: 0.34,
      }).addTo(halosLayer);
      L.circleMarker([node.latitude, node.longitude], {
        radius: innerRadius,
        color: strokeColor,
        weight: 1.8,
        fillColor,
        fillOpacity: 0.1,
        opacity: 0.52,
      }).addTo(halosLayer);
    }

    function addDirectionalArrow(sourceNode, targetNode, color, ratio = 0.58) {
      if (!sourceNode || !targetNode) return;
      const fromPoint = map.latLngToLayerPoint([sourceNode.latitude, sourceNode.longitude]);
      const toPoint = map.latLngToLayerPoint([targetNode.latitude, targetNode.longitude]);
      const angle = Math.atan2(toPoint.y - fromPoint.y, toPoint.x - fromPoint.x) * (180 / Math.PI);
      const lat = sourceNode.latitude + ((targetNode.latitude - sourceNode.latitude) * ratio);
      const lon = sourceNode.longitude + ((targetNode.longitude - sourceNode.longitude) * ratio);
      L.marker([lat, lon], {
        icon: L.divIcon({ className: 'line-arrow-icon', html: `<span class="line-arrow-chip" style="color:${color}; transform: rotate(${angle}deg)">➜</span>`, iconSize: null }),
        interactive: false,
        zIndexOffset: 1200,
      }).addTo(linksLayer);
    }

    function estimateLabelRect(point, html) {
      const text = html.replace(/<[^>]+>/g, ' ');
      const lines = html.includes('label-meta') ? 2 : 1;
      const width = Math.min(180, Math.max(66, text.trim().length * 5.4));
      const height = lines === 2 ? 38 : 24;
      return {
        left: point.x - (width / 2),
        right: point.x + (width / 2),
        top: point.y - height - 18,
        bottom: point.y - 18,
      };
    }

    function rectsOverlap(left, right) {
      return !(left.right < right.left || left.left > right.right || left.bottom < right.top || left.top > right.bottom);
    }

    function labelHtml(node, zoom, forced, neighborIds) {
      const shortName = node.name || node.hash_prefix_hex;
      const isFocusedNode = node.identity_hex === selectedSourceId || node.identity_hex === routeSourceId || node.identity_hex === routeTargetId;
      const isActivePeer = neighborIds.has(node.identity_hex);
      const chipClass = `node-label-chip${isFocusedNode ? ' focused' : ''}${isActivePeer ? ' active-peer' : ''}`;
      if (selectedNeighborId) {
        if (node.identity_hex !== selectedSourceId && node.identity_hex !== selectedNeighborId) return null;
        return `<div class="${chipClass}"><strong>${shortName}</strong><span class="label-meta">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span></div>`;
      }
      const inspectionNeighbor = Boolean(selectedSourceId) && node.identity_hex !== selectedSourceId && neighborIds.has(node.identity_hex);
      if (inspectionNeighbor && zoom >= HIGH_ZOOM_LABEL_THRESHOLD) {
        return `<div class="${chipClass}"><strong>${shortName}</strong></div>`;
      }
      if (forced && isFocusedNode) {
        return `<div class="${chipClass}"><strong>${shortName}</strong><span class="label-meta">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span></div>`;
      }
      if (forced || zoom >= HIGH_ZOOM_LABEL_THRESHOLD) {
        return `<div class="${chipClass}"><strong>${shortName}</strong></div>`;
      }
      if (zoom >= LOW_ZOOM_LABEL_THRESHOLD && (isFocusedNode || node.identity_hex === hoveredNodeId)) {
        return `<div class="${chipClass}"><strong>${shortName}</strong></div>`;
      }
      return null;
    }

    function labelPriority(node, neighborIds) {
      if (node.identity_hex === selectedSourceId) return 4;
      if (neighborIds.has(node.identity_hex)) return 3;
      if (node.identity_hex === hoveredNodeId) return 2;
      return 1;
    }

    function renderLabels(nodes, neighborIds) {
      labelsLayer.clearLayers();
      const zoom = map.getZoom();
      const candidates = [];
      for (const node of nodes) {
        const forced = node.identity_hex === selectedSourceId
          || node.identity_hex === routeSourceId
          || node.identity_hex === routeTargetId
          || node.identity_hex === hoveredNodeId
          || (selectedNeighborId && node.identity_hex === selectedNeighborId);
        const html = labelHtml(node, zoom, forced, neighborIds);
        if (!html) continue;
        candidates.push({
          node,
          html,
          forced,
          priority: labelPriority(node, neighborIds),
          point: map.latLngToContainerPoint([node.latitude, node.longitude]),
        });
      }
      candidates.sort((left, right) => right.priority - left.priority);
      const occupied = [];
      let count = 0;
      for (const candidate of candidates) {
        const rect = estimateLabelRect(candidate.point, candidate.html);
        const overlaps = occupied.some((item) => rectsOverlap(item, rect));
        if (overlaps && !candidate.forced) continue;
        if (!candidate.forced && count >= MAX_COLLISION_LABELS) continue;
        occupied.push(rect);
        count += 1;
        L.marker([candidate.node.latitude, candidate.node.longitude], {
          icon: L.divIcon({ className: 'node-label-icon', html: candidate.html, iconSize: null }),
          interactive: false,
          zIndexOffset: candidate.priority * 100,
        }).addTo(labelsLayer);
      }
    }

    function renderLinkLabels(selectedLinks, sourceNode) {
      linkLabelsLayer.clearLayers();
      const alwaysVisible = Boolean(selectedSourceId);
      for (const link of selectedLinks) {
        if (selectedNeighborId && link.target_identity_hex !== selectedNeighborId) continue;
        const midpoint = [
          (link.source_latitude + link.target_latitude) / 2,
          (link.source_longitude + link.target_longitude) / 2,
        ];
        L.marker(midpoint, {
          icon: L.divIcon({ className: 'link-label-icon', html: `<div class=\"signal-label-chip\">${linkLabel(link, sourceNode)}</div>`, iconSize: null }),
          interactive: false,
          opacity: alwaysVisible ? 1 : 0,
          zIndexOffset: 2000,
        }).addTo(linkLabelsLayer);
      }
    }

    function neighborDistanceKm(sourceNode, link) {
      if (!sourceNode || !isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude)) return null;
      if (!isFiniteCoordinate(link.target_latitude, link.target_longitude)) return null;
      return haversineKm(sourceNode.latitude, sourceNode.longitude, link.target_latitude, link.target_longitude);
    }

    function selectedHistoryRows(state, node, neighborId) {
      if (!node || !neighborId) return [];
      return (signalHistoryByNode[node.identity_hex] || [])
        .filter((row) => row.target_identity_hex === neighborId || row.target_hash_prefix_hex === neighborId)
        .sort((left, right) => new Date(left.collected_at) - new Date(right.collected_at));
    }

    function probeJobsForNode(state, node) {
      if (!node) return [];
      return (state.probe_jobs || []).filter((job) => job.pubkey_hex === node.identity_hex);
    }

    function nextProbeJobForNode(state, node) {
      const activeJobs = probeJobsForNode(state, node)
        .filter((job) => job.status === 'pending' || job.status === 'running')
        .sort((left, right) => {
          const leftRank = left.status === 'running' ? 0 : 1;
          const rightRank = right.status === 'running' ? 0 : 1;
          if (leftRank !== rightRank) return leftRank - rightRank;
          const leftTime = left.scheduled_at ? new Date(left.scheduled_at).getTime() : 0;
          const rightTime = right.scheduled_at ? new Date(right.scheduled_at).getTime() : 0;
          if (leftTime !== rightTime) return leftTime - rightTime;
          return (left.id || 0) - (right.id || 0);
        });
      return activeJobs[0] || null;
    }

    function probeQueueFeedbackForNode(node) {
      if (!node || !probeQueueFeedback) return null;
      return probeQueueFeedback.identityHex === node.identity_hex ? probeQueueFeedback : null;
    }

    function probeQueueSummary(state, node) {
      if (!node) return null;
      const activeJob = nextProbeJobForNode(state, node);
      if (activeJob) {
        if (activeJob.status === 'running') {
          return {
            chip: tr('probeQueueRunning'),
            chipClass: 'busy',
            note: tr('probeQueueHintRunning'),
          };
        }
        if (activeJob.scheduled_at && new Date(activeJob.scheduled_at).getTime() > Date.now()) {
          return {
            chip: tr('probeQueuePending'),
            chipClass: 'pending',
            note: tr('probeQueueHintPendingAt')(formatShortWhen(activeJob.scheduled_at)),
          };
        }
        return {
          chip: tr('probeQueuePending'),
          chipClass: 'pending',
          note: tr('probeQueueHintPendingNow'),
        };
      }

      if (probeQueueBusyNodeId === node.identity_hex) {
        return {
          chip: tr('probeQueuePending'),
          chipClass: 'busy',
          note: tr('probeQueueBusy'),
        };
      }

      const feedback = probeQueueFeedbackForNode(node);
      if (!feedback) return null;
      if (feedback.status === 'queued') {
        return {
          chip: tr('probeQueueQueued'),
          chipClass: 'pending',
          note: feedback.scheduledAt
            ? tr('probeQueueHintQueuedAt')(formatShortWhen(feedback.scheduledAt))
            : tr('probeQueueHintQueuedNow'),
        };
      }
      if (feedback.status === 'already_pending') {
        return {
          chip: tr('probeQueuePending'),
          chipClass: 'pending',
          note: feedback.scheduledAt
            ? tr('probeQueueHintPendingAt')(formatShortWhen(feedback.scheduledAt))
            : tr('probeQueueHintPendingNow'),
        };
      }
      if (feedback.status === 'cooldown') {
        return {
          chip: tr('probeQueueCooldown'),
          chipClass: 'cooldown',
          note: tr('probeQueueHintCooldown'),
        };
      }
      if (feedback.status === 'error') {
        return {
          chip: tr('probeQueueError'),
          chipClass: 'error',
          note: tr('probeQueueHintError'),
        };
      }
      return null;
    }

    function renderProbeQueueCard(state, node) {
      const summary = probeQueueSummary(state, node);
      const isBusy = probeQueueBusyNodeId === node.identity_hex;
      return `
        <div class=\"probe-queue-card\">
          <div class=\"expand-head\">
            <strong>${tr('probeQueueTitle')}</strong>
            ${summary ? `<span class=\"probe-status-chip ${summary.chipClass}\">${summary.chip}</span>` : ''}
          </div>
          <div class=\"probe-queue-controls\">
            <button type=\"button\" class=\"probe-submit-button\" data-queue-probe=\"${node.id}\" ${isBusy ? 'disabled' : ''}>${isBusy ? tr('probeQueueBusy') : tr('probeQueueAction')}</button>
          </div>
          ${summary?.note ? `<div class=\"probe-note\">${summary.note}</div>` : ''}
        </div>
      `;
    }

    function renderSignalChart(node, neighborLink, historyRows) {
      if (!node) return `<div class=\"empty-note\">${tr('emptySelectRepeater')}</div>`;
      if (!neighborLink) return `<div class=\"empty-note\">${tr('emptySelectNeighbor')}</div>`;
      if (isSignalHistoryLoading(node) && !hasSignalHistoryLoaded(node)) {
        return `<div class=\"empty-note\">${tr('loadingSignalHistory')}</div>`;
      }
      if (historyRows.length < 2) {
        return `
          <div class=\"chart-shell\">
            <div class=\"chart-head\">
              <div class=\"chart-title\"><strong>${neighborLink.target_name}</strong><span>${tr('chartHistory')} ${lineSignalMetric(neighborLink).kind}</span></div>
              <div class=\"chart-meta\">${tr('chartLatest')} ${lineSignalMetric(neighborLink).label}</div>
            </div>
            <div class=\"empty-note\">${tr('storedSamples')(historyRows.length)}</div>
          </div>
        `;
      }
      const values = historyRows.map((row) => row.snr).filter((value) => value !== null && value !== undefined);
      const times = historyRows.map((row) => new Date(row.collected_at).getTime());
      const minValue = Math.min(...values);
      const maxValue = Math.max(...values);
      const minTime = Math.min(...times);
      const maxTime = Math.max(...times);
      const leftPad = 28;
      const topPad = 10;
      const width = 272;
      const height = 110;
      const valueSpan = Math.max(1, maxValue - minValue);
      const timeSpan = Math.max(1, maxTime - minTime);
      const grid = [0, 0.5, 1].map((ratio) => {
        const y = topPad + ratio * height;
        const value = (maxValue - (ratio * valueSpan)).toFixed(1);
        return `<line x1=\"${leftPad}\" y1=\"${y}\" x2=\"${leftPad + width}\" y2=\"${y}\" stroke=\"rgba(21,33,42,0.08)\" stroke-width=\"1\" />` +
          `<text x=\"4\" y=\"${y + 4}\" fill=\"#6a7883\" font-size=\"10\">${value}</text>`;
      }).join('');
      const path = historyRows.map((row, index) => {
        const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
        const y = topPad + ((maxValue - row.snr) / valueSpan) * height;
        return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
      }).join(' ');
      const points = historyRows.map((row) => {
        const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
        const y = topPad + ((maxValue - row.snr) / valueSpan) * height;
        return `<circle cx=\"${x.toFixed(1)}\" cy=\"${y.toFixed(1)}\" r=\"2.2\" fill=\"${lineColor(neighborLink)}\" />`;
      }).join('');
      return `
        <div class=\"chart-shell\">
          <div class=\"chart-head\">
            <div class=\"chart-title\"><strong>${neighborLink.target_name}</strong><span>${tr('chartSNRHistory')}</span></div>
            <div class=\"chart-meta\">${tr('chartLatest')} ${lineSignalMetric(neighborLink).label}</div>
          </div>
          <svg id=\"signal-chart\" viewBox=\"0 0 320 152\" preserveAspectRatio=\"none\">
            ${grid}
            <path d=\"${path}\" fill=\"none\" stroke=\"${lineColor(neighborLink)}\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" />
            ${points}
            <text x=\"${leftPad}\" y=\"144\" fill=\"#6a7883\" font-size=\"10\">${timeAgo(new Date(minTime).toISOString())}</text>
            <text x=\"${leftPad + width - 22}\" y=\"144\" fill=\"#6a7883\" font-size=\"10\">${tr('chartNow')}</text>
          </svg>
        </div>
      `;
    }

    function renderExpandedNode(node, state) {
      const selectedLinks = getSelectedLinks(state);
      if (!selectedLinks.length || (selectedNeighborId && !selectedLinks.some((link) => link.target_identity_hex === selectedNeighborId))) {
        selectedNeighborId = null;
      }
      const selectedLink = selectedLinks.find((link) => link.target_identity_hex === selectedNeighborId) || null;
      const historyRows = selectedHistoryRows(state, node, selectedNeighborId);
      const neighborRows = selectedLinks.length ? `
        <table class=\"neighbor-table\">
          <thead>
            <tr>
              <th>${tr('neighbor')}</th>
              <th>${tr('lastSeen')}</th>
              <th>${tr('signal')}</th>
              <th>${tr('distance')}</th>
            </tr>
          </thead>
          <tbody>
            ${selectedLinks.map((link) => {
              const distance = neighborDistanceKm(node, link);
              const activeClass = link.target_identity_hex === selectedNeighborId ? ' class=\"active\"' : '';
              return `
                <tr${activeClass}>
                  <td><button type=\"button\" data-neighbor=\"${link.target_identity_hex}\">${link.target_name}</button></td>
                  <td>${typeof link.last_heard_seconds === 'number' ? humanizeSeconds(link.last_heard_seconds) : timeAgo(link.collected_at)}</td>
                  <td>${lineSignalMetric(link).label}</td>
                  <td>${distance === null ? '-' : `${distance.toFixed(1)} km`}</td>
                </tr>
              `;
            }).join('')}
          </tbody>
        </table>
      ` : `<div class=\"empty-note\">${tr('emptyNoNeighborLinks')}</div>`;
      return `
        <div class=\"node-expand\">
          <div class=\"expand-head\">
            <strong>${tr('inspection')}</strong>
            <button type=\"button\" class=\"ghost-button\" data-clear-selection=\"1\">${tr('clearFocus')}</button>
          </div>
          <div class=\"detail-grid\">
            <div class=\"detail-cell\"><strong>${tr('role')}</strong>${node.role || tr('roleDefault')}</div>
            <div class=\"detail-cell\"><strong>${tr('firstSeen')}</strong>${formatWhen(node.first_seen_at)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastAdvert')}</strong>${formatWhen(node.last_advert_at)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastData')}</strong>${formatWhen(node.last_data_at)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastSuccessfulProbe')}</strong>${formatWhen(node.last_successful_probe_at)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastProbeResult')}</strong>${describeProbeResult(node)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastProbeAttempt')}</strong>${formatWhen(node.last_probe_at)}</div>
          </div>
          ${renderProbeQueueCard(state, node)}
          <div>
            <div class=\"expand-head\"><strong>${tr('directNeighbors')}</strong><span class=\"node-state-tag\">${selectedLinks.length}</span></div>
            ${neighborRows}
          </div>
          ${renderSignalChart(node, selectedLink, historyRows)}
        </div>
      `;
    }

    function rowHtml(node, state) {
      const primaryAgeLabel = currentPanel === 'new' ? tr('firstSeenLabel') : tr('lastAdvertLabel');
      const primaryAgeValue = currentPanel === 'new' ? node.first_seen_at : node.last_advert_at;
      return `
        <div class=\"node-row${node.identity_hex === selectedSourceId ? ' active' : ''}\">
          <button type=\"button\" class=\"node-row-button\" data-node=\"${node.identity_hex}\">
            <span class=\"status-dot\" style=\"background:${nodeColor(node)}\"></span>
            <span class=\"node-main\">
              <span class=\"node-name\">${node.name || node.hash_prefix_hex}</span>
              <span class=\"node-age\">${primaryAgeLabel}: ${formatShortWhen(primaryAgeValue)}</span>
              ${currentPanel === 'new' ? `<span class=\"node-age\">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span>` : ''}
              <span class=\"node-age\">${tr('lastDataLabel')}: ${formatShortWhen(node.last_data_at)}</span>
            </span>
            <span class=\"node-state-tag\">${nodeStateLabel(node)}</span>
          </button>
          ${node.identity_hex === selectedSourceId && (currentPanel === 'map' || currentPanel === 'new') ? renderExpandedNode(node, state) : ''}
        </div>
      `;
    }

    function renderNodeSections(state) {
      const container = document.getElementById('node-sections');
      const allNodes = sortNodes(relevantNodes(state));
      const nodes = listNodes(state);
      const selectedNode = selectedSourceId ? allNodes.find((node) => node.identity_hex === selectedSourceId) : null;
      const others = nodes.filter((node) => node.identity_hex !== selectedSourceId);
      const panelCopy = currentPanelCopy();
      const panelTitle = panelCopy.title;
      const panelSubtitle = panelCopy.subtitle;
      const archivedCount = archivedNodeCount(state);
      const archivedAutoFallback = autoShowArchived(state);
      let html = '';
      const sortHtml = currentPanel === 'map' && !isPortraitMobileView()
        ? `
            <div class="toolbar-meta-group">
              <label for="sort-mode">${tr('sortLabel')}</label>
              <select id="sort-mode" class="sort-select" data-sort-mode="1">
                <option value="last_advert"${nodeSortMode === 'last_advert' ? ' selected' : ''}>${tr('sortLastAdvert')}</option>
                <option value="last_data"${nodeSortMode === 'last_data' ? ' selected' : ''}>${tr('sortLastData')}</option>
                <option value="alphabetical"${nodeSortMode === 'alphabetical' ? ' selected' : ''}>${tr('sortAlphabetical')}</option>
              </select>
            </div>
          `
        : '';
      const searchHtml = currentPanel === 'map' || currentPanel === 'new'
        ? `
            <div class="toolbar-meta-group toolbar-search">
              <label for="node-search">${tr('searchLabel')}</label>
              <input id="node-search" class="toolbar-search-input" type="search" data-node-search="1" placeholder="${tr('searchPlaceholder')}" />
            </div>
          `
        : '';
      const archivedHtml = currentPanel === 'new'
        ? ''
        : `<button type="button" class="toolbar-toggle-button${showArchived ? ' active' : ''}" data-toggle-archived="1">${archivedCount ? trFormat('archivedToggleCount', archivedCount) : tr('archivedToggle')}</button>`;
      const metaHtml = `${searchHtml}${sortHtml}`;
      const langHtml = `<div class="lang-toggle" role="group" aria-label="${tr('languageLabel')}"><button type="button" class="lang-button" data-global-language="pl">PL</button><button type="button" class="lang-button" data-global-language="en">EN</button></div>`;
      const archivedNoteHtml = archivedAutoFallback
        ? `<div class="toolbar-note"><strong>${tr('archivedToggle')}</strong> ${tr('archivedAutoFallback')}</div>`
        : '';
      html += `
        <div class="list-toolbar">
          <div class="toolbar-head">
            <div class="toolbar-head-main">
              <strong class="toolbar-title">${panelTitle}</strong>
              <span class="toolbar-subtitle">${panelSubtitle}</span>
            </div>
            <div class="toolbar-head-actions">
              ${archivedHtml}
              ${langHtml}
            </div>
          </div>
          ${archivedNoteHtml}
          ${renderPrimaryTabs()}
          <div class="toolbar-meta">
            ${metaHtml}
          </div>
        </div>
      `;
      html += renderAnalysisTabs();
      if (isPortraitMobileView()) {
        html += renderMobileOverview(state);
      }
      if (currentPanel === 'connectivity') {
        html += renderConnectivityPanel(state);
      } else if (currentPanel === 'route') {
        html += renderRoutePanel(state);
      } else if (currentPanel === 'new') {
        if (selectedNode) {
          html += `<div class="section-heading">${tr('selectedRepeater')}</div>`;
          html += `<div class="node-list">${rowHtml(selectedNode, state)}</div>`;
        }
        html += `<div class="section-heading">${tr('newRepeaters')}</div>`;
        html += `<div class="node-list">${others.length ? others.map((node) => rowHtml(node, state)).join('') : `<div class="empty-note">${hasActiveNodeSearchQuery() ? tr('emptyNoSearchResults') : tr('emptyNoNewRepeaters')}</div>`}</div>`;
      } else {
        if (isPortraitMobileView()) {
          html += renderMobileMapPanel(state);
          container.innerHTML = html;
          for (const button of container.querySelectorAll('[data-node]')) {
            button.addEventListener('click', () => selectNode(button.dataset.node));
          }
          for (const button of container.querySelectorAll('[data-panel]')) {
            button.addEventListener('click', () => setPanel(button.dataset.panel));
          }
          for (const button of container.querySelectorAll('[data-connectivity-direction]')) {
            button.addEventListener('click', () => setConnectivityDirection(button.dataset.connectivityDirection));
          }
          for (const select of container.querySelectorAll('[data-focus-node]')) {
            select.value = selectedSourceId || '';
            select.addEventListener('change', () => {
              selectedSourceId = select.value || null;
              selectedNeighborId = null;
              render(latestState);
            });
          }
          for (const button of container.querySelectorAll('[data-toggle-archived]')) {
            button.addEventListener('click', () => setShowArchived(!showArchived));
          }
          for (const input of container.querySelectorAll('[data-node-search]')) {
            input.value = nodeSearchQuery;
            input.addEventListener('input', () => {
              nodeSearchQuery = input.value || '';
              render(latestState);
            });
          }
          for (const button of container.querySelectorAll('[data-queue-probe]')) {
            button.addEventListener('click', () => {
              queueProbeJob(button.dataset.queueProbe);
            });
          }
          for (const button of container.querySelectorAll('[data-mobile-peer]')) {
            button.addEventListener('click', () => {
              selectedNeighborId = selectedNeighborId === button.dataset.mobilePeer ? null : button.dataset.mobilePeer;
              render(latestState);
            });
          }
          for (const button of container.querySelectorAll('[data-global-language]')) {
            button.classList.toggle('active', button.dataset.globalLanguage === currentLanguage);
            button.onclick = () => setLanguage(button.dataset.globalLanguage);
          }
          return;
        }
        if (selectedNode) {
          html += `<div class="section-heading">${tr('selectedRepeater')}</div>`;
          html += `<div class="node-list">${rowHtml(selectedNode, state)}</div>`;
        }
        html += `<div class="section-heading">${selectedNode ? tr('otherRepeaters') : tr('repeaters')}</div>`;
        html += `<div class="node-list">${others.length ? others.map((node) => rowHtml(node, state)).join('') : `<div class="empty-note">${hasActiveNodeSearchQuery() ? tr('emptyNoSearchResults') : tr('emptyNoOtherRepeaters')}</div>`}</div>`;
      }
      container.innerHTML = html;
      for (const button of container.querySelectorAll('[data-node]')) {
        button.addEventListener('click', () => selectNode(button.dataset.node));
      }
      for (const button of container.querySelectorAll('[data-panel]')) {
        button.addEventListener('click', () => setPanel(button.dataset.panel));
      }
      for (const button of container.querySelectorAll('[data-connectivity-direction]')) {
        button.addEventListener('click', () => setConnectivityDirection(button.dataset.connectivityDirection));
      }
      for (const select of container.querySelectorAll('[data-focus-node]')) {
        select.value = selectedSourceId || '';
        select.addEventListener('change', () => {
          selectedSourceId = select.value || null;
          selectedNeighborId = null;
          if (latestState) focusConnectivitySelection(latestState);
          render(latestState);
        });
      }
      for (const select of container.querySelectorAll('[data-sort-mode]')) {
        select.addEventListener('change', () => {
          nodeSortMode = select.value;
          render(latestState);
        });
      }
      for (const input of container.querySelectorAll('[data-node-search]')) {
        input.value = nodeSearchQuery;
        input.addEventListener('input', () => {
          nodeSearchQuery = input.value || '';
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-queue-probe]')) {
        button.addEventListener('click', () => {
          queueProbeJob(button.dataset.queueProbe);
        });
      }
      for (const button of container.querySelectorAll('[data-toggle-archived]')) {
        button.addEventListener('click', () => setShowArchived(!showArchived));
      }
      for (const select of container.querySelectorAll('[data-route-source]')) {
        select.value = routeSourceId || '';
        select.addEventListener('change', () => {
          routeActiveEndpoint = 'source';
          routeSourceId = select.value || null;
          if (latestState) focusRouteSelection(latestState);
          render(latestState);
        });
      }
      for (const select of container.querySelectorAll('[data-route-target]')) {
        select.value = routeTargetId || '';
        select.addEventListener('change', () => {
          routeActiveEndpoint = 'target';
          routeTargetId = select.value || null;
          if (latestState) focusRouteSelection(latestState);
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-route-active]')) {
        button.addEventListener('click', () => {
          routeActiveEndpoint = button.dataset.routeActive === 'target' ? 'target' : 'source';
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-route-destination]')) {
        button.addEventListener('click', () => {
          routeActiveEndpoint = 'target';
          routeTargetId = button.dataset.routeDestination || null;
          if (latestState) focusRouteSelection(latestState);
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-route-clear-target]')) {
        const clearTarget = () => {
          routeActiveEndpoint = 'target';
          routeTargetId = null;
          if (latestState) focusRouteSelection(latestState);
          render(latestState);
        };
        button.addEventListener('click', clearTarget);
      }
      for (const button of container.querySelectorAll('[data-clear-selection]')) {
        button.addEventListener('click', clearSelection);
      }
      for (const button of container.querySelectorAll('[data-neighbor]')) {
        button.addEventListener('click', () => {
          selectedNeighborId = button.dataset.neighbor;
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-mobile-peer]')) {
        button.addEventListener('click', () => {
          selectedNeighborId = selectedNeighborId === button.dataset.mobilePeer ? null : button.dataset.mobilePeer;
          render(latestState);
        });
      }
    }

    function renderMap(state) {
      if (currentPanel === 'map' && isPortraitMobileView()) {
        renderMobileDirectionalMap(state);
        return;
      }
      if (currentPanel === 'connectivity') {
        renderConnectivityMap(state);
        return;
      }
      if (currentPanel === 'route') {
        renderRouteMap(state);
        return;
      }
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const allMapNodes = deriveMapNodes(sortNodes(relevantNodes(state)));
      const neighborIds = selectedNeighborIds(state);
      const selectedLinks = getSelectedMapLinks(state);
      const sourceNode = getSelectedNode(state);
      const nodes = selectedSourceId
        ? allMapNodes.filter((node) => node.identity_hex === selectedSourceId || neighborIds.has(node.identity_hex))
        : (hasActiveNodeSearchQuery() ? allMapNodes.filter((node) => nodeMatchesSearch(node)) : allMapNodes);
      const bounds = [];
      for (const node of nodes) {
        const selected = node.identity_hex === selectedSourceId;
        const neighbor = neighborIds.has(node.identity_hex);
        const isolated = Boolean(selectedNeighborId) && node.identity_hex !== selectedSourceId && node.identity_hex !== selectedNeighborId;
        if (selected) {
          drawFocusHalo(node, nodeColor(node), nodeColor(node), 17, 12);
        }
        const marker = L.circleMarker([node.latitude, node.longitude], markerStyle(node, isolated, selected, neighbor)).addTo(markersLayer);
        marker.on('click', (event) => {
          L.DomEvent.stopPropagation(event);
          selectNode(node.identity_hex);
        });
        marker.on('mouseover', () => {
          hoveredNodeId = node.identity_hex;
          renderLabels(nodes, neighborIds);
        });
        marker.on('mouseout', () => {
          if (hoveredNodeId === node.identity_hex) hoveredNodeId = null;
          renderLabels(nodes, neighborIds);
        });
        if (selected) marker.bringToFront();
        bounds.push([node.latitude, node.longitude]);
      }
      for (const link of selectedLinks) {
        const polyline = L.polyline([
          [link.source_latitude, link.source_longitude],
          [link.target_latitude, link.target_longitude],
        ], {
          color: lineColor(link),
          weight: selectedNeighborId && link.target_identity_hex === selectedNeighborId ? 3.2 : 2,
          opacity: selectedNeighborId && link.target_identity_hex !== selectedNeighborId ? 0.18 : 0.82,
        }).addTo(linksLayer);
        polyline.on('mouseover', () => {
          if (selectedLinks.length > 6) {
            const midpoint = [
              (link.source_latitude + link.target_latitude) / 2,
              (link.source_longitude + link.target_longitude) / 2,
            ];
            const transient = L.marker(midpoint, {
              icon: L.divIcon({ className: 'link-label-icon', html: `<div class=\"signal-label-chip\">${linkLabel(link, sourceNode)}</div>`, iconSize: null }),
              interactive: false,
              zIndexOffset: 2000,
            }).addTo(linkLabelsLayer);
            polyline.once('mouseout', () => linkLabelsLayer.removeLayer(transient));
          }
        });
        polyline.on('click', (event) => {
          L.DomEvent.stopPropagation(event);
          selectedNeighborId = link.target_identity_hex;
          render(latestState);
        });
        bounds.push([link.source_latitude, link.source_longitude]);
        bounds.push([link.target_latitude, link.target_longitude]);
      }
      renderLabels(nodes, neighborIds);
      renderLinkLabels(selectedLinks, sourceNode);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function drawMapNodes(nodeMap, focusId, highlightedIds = new Set()) {
      const bounds = [];
      for (const node of nodeMap) {
        if (!isFiniteCoordinate(node.latitude, node.longitude)) continue;
        const selected = node.identity_hex === focusId;
        const neighbor = highlightedIds.has(node.identity_hex);
        const marker = L.circleMarker([node.latitude, node.longitude], markerStyle(node, false, selected, neighbor)).addTo(markersLayer);
        marker.on('click', (event) => {
          L.DomEvent.stopPropagation(event);
          if (currentPanel === 'route') {
            if (routeActiveEndpoint === 'target') {
              routeTargetId = node.identity_hex;
            } else {
              routeSourceId = node.identity_hex;
            }
            focusRouteSelection(latestState);
          } else {
            selectedSourceId = node.identity_hex;
            if (currentPanel === 'connectivity') {
              focusConnectivitySelection(latestState);
            }
          }
          render(latestState);
        });
        bounds.push([node.latitude, node.longitude]);
      }
      return bounds;
    }

    function renderConnectivityMap(state) {
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const data = connectivityData(state);
      const focusId = selectedSourceId;
      const focusNode = focusId ? data.nodeIndex.get(focusId) : null;
      const canInspectOwnData = hasOwnNeighborData(focusNode);
      let edges = [];
      if (focusId) {
        if (connectivityDirection === 'out' && canInspectOwnData) {
          edges = data.edges.filter((edge) => edge.source_identity_hex === focusId);
        } else if (connectivityDirection === 'in') {
          edges = data.edges.filter((edge) => edge.target_identity_hex === focusId);
        } else if (canInspectOwnData) {
          edges = data.edges.filter((edge) => edge.source_identity_hex === focusId && edge.mutual);
        }
      }
      const highlightedIds = new Set();
      for (const edge of edges) {
        highlightedIds.add(edge.source_identity_hex);
        highlightedIds.add(edge.target_identity_hex);
      }
      const nodes = focusId
        ? data.nodes.filter((node) => highlightedIds.has(node.identity_hex))
        : data.nodes;
      const bounds = drawMapNodes(nodes, focusId, highlightedIds);
      if (focusId) {
        const focusNode = data.nodeIndex.get(focusId);
        drawFocusHalo(focusNode, '#15212a', '#15212a', 19, 14);
      }
      for (const edge of edges) {
        const sourceNode = data.nodeIndex.get(edge.source_identity_hex);
        const targetNode = data.nodeIndex.get(edge.target_identity_hex);
        if (!sourceNode || !targetNode) continue;
        if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
        const color = edge.mutual ? '#2e8b57' : connectivityDirection === 'in' ? '#2c71d1' : '#cfaa38';
        L.polyline([
          [sourceNode.latitude, sourceNode.longitude],
          [targetNode.latitude, targetNode.longitude],
        ], {
          color,
          weight: edge.stale ? 1.5 : 2.6,
          opacity: edge.stale ? 0.4 : 0.84,
          dashArray: edge.stale ? '5 5' : null,
        }).addTo(linksLayer);
        if (connectivityDirection === 'mutual') {
          addDirectionalArrow(sourceNode, targetNode, color, 0.42);
          addDirectionalArrow(targetNode, sourceNode, color, 0.42);
        } else {
          addDirectionalArrow(sourceNode, targetNode, color);
        }
      }
      renderLabels(nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude)), highlightedIds);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function renderMobileDirectionalMap(state) {
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const data = connectivityData(state);
      const focusId = selectedSourceId;
      const focusNode = focusId ? data.nodeIndex.get(focusId) : null;
      const canInspectOwnData = hasOwnNeighborData(focusNode);
      if (focusNode && connectivityDirection === 'out' && !canInspectOwnData) {
        connectivityDirection = 'in';
      }
      const edges = focusId
        ? (connectivityDirection === 'out'
            ? (canInspectOwnData ? data.edges.filter((edge) => edge.source_identity_hex === focusId) : [])
            : data.edges.filter((edge) => edge.target_identity_hex === focusId))
        : [];
      const highlightedIds = new Set(focusId ? [focusId] : []);
      for (const edge of edges) {
        highlightedIds.add(edge.source_identity_hex);
        highlightedIds.add(edge.target_identity_hex);
      }
      const nodes = focusId ? data.nodes.filter((node) => highlightedIds.has(node.identity_hex)) : data.nodes;
      const bounds = drawMapNodes(nodes, focusId, highlightedIds);
      if (focusNode) {
        drawFocusHalo(focusNode, '#15212a', '#15212a', 19, 14);
      }
      for (const edge of edges) {
        const sourceNode = data.nodeIndex.get(edge.source_identity_hex);
        const targetNode = data.nodeIndex.get(edge.target_identity_hex);
        if (!sourceNode || !targetNode) continue;
        if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
        const peerId = connectivityDirection === 'out' ? edge.target_identity_hex : edge.source_identity_hex;
        const isActive = !selectedNeighborId || selectedNeighborId === peerId;
        const color = connectivityDirection === 'in' ? '#2c71d1' : '#cfaa38';
        L.polyline([
          [sourceNode.latitude, sourceNode.longitude],
          [targetNode.latitude, targetNode.longitude],
        ], {
          color,
          weight: isActive ? 3.1 : 1.8,
          opacity: isActive ? 0.88 : 0.22,
          dashArray: edge.stale ? '5 5' : null,
        }).addTo(linksLayer);
        addDirectionalArrow(sourceNode, targetNode, color);
      }
      renderLabels(nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude)), highlightedIds);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function renderRouteMap(state) {
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const data = connectivityData(state);
      const allMapNodes = deriveMapNodes(data.nodes);
      const reachability = routeSourceId && !routeTargetId ? buildRouteReachability(state, routeSourceId) : null;
      const highlightedIds = new Set(reachability?.highlightIds || []);
      for (const identityHex of [routeSourceId, routeTargetId].filter(Boolean)) highlightedIds.add(identityHex);
      const forward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId ? buildRouteResult(state, routeSourceId, routeTargetId) : null;
      const backward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId ? buildRouteResult(state, routeTargetId, routeSourceId) : null;
      const historicalForward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId && !forward?.path
        ? buildHistoricalRouteResult(state, routeSourceId, routeTargetId)
        : null;
      const historicalBackward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId && !backward?.path
        ? buildHistoricalRouteResult(state, routeTargetId, routeSourceId)
        : null;
      const pathIds = new Set(forward?.path || []);
      for (const identityHex of (backward?.path || [])) pathIds.add(identityHex);
      for (const identityHex of (historicalForward?.path || [])) pathIds.add(identityHex);
      for (const identityHex of (historicalBackward?.path || [])) pathIds.add(identityHex);
      for (const identityHex of pathIds) highlightedIds.add(identityHex);
      const bounds = drawMapNodes(allMapNodes, routeSourceId, highlightedIds);
      if (routeSourceId) {
        const sourceNode = data.nodeIndex.get(routeSourceId);
        drawFocusHalo(sourceNode, '#2c71d1', '#2c71d1', 16, 12);
      }
      if (routeTargetId) {
        const targetNode = data.nodeIndex.get(routeTargetId);
        drawFocusHalo(targetNode, '#cfaa38', '#cfaa38', 16, 12);
      }
      const drawReachabilityTree = (reachabilityResult) => {
        if (!reachabilityResult) return;
        for (const edge of reachabilityResult.treeEdges) {
          const sourceNode = data.nodeIndex.get(edge.sourceId);
          const targetNode = data.nodeIndex.get(edge.targetId);
          if (!sourceNode || !targetNode) continue;
          if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
          const color = edge.usesStale ? 'rgba(156, 123, 19, 0.42)' : 'rgba(44, 113, 209, 0.34)';
          L.polyline([
            [sourceNode.latitude, sourceNode.longitude],
            [targetNode.latitude, targetNode.longitude],
          ], {
            color,
            weight: 2,
            opacity: 0.9,
            dashArray: edge.usesStale ? '6 6' : null,
          }).addTo(linksLayer);
          addDirectionalArrow(sourceNode, targetNode, color, 0.56);
        }
      };
      const drawRoute = (routeResult, color, dashArray = null) => {
        if (!routeResult?.path) return;
        for (let index = 0; index < routeResult.path.length - 1; index += 1) {
          const sourceNode = data.nodeIndex.get(routeResult.path[index]);
          const targetNode = data.nodeIndex.get(routeResult.path[index + 1]);
          if (!sourceNode || !targetNode) continue;
          if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
          L.polyline([
            [sourceNode.latitude, sourceNode.longitude],
            [targetNode.latitude, targetNode.longitude],
          ], {
            color,
            weight: 3,
            opacity: 0.9,
            dashArray,
          }).addTo(linksLayer);
          addDirectionalArrow(sourceNode, targetNode, color, 0.54);
        }
      };
      const drawHistoricalContext = (focusIds) => {
        for (const edge of data.historicalEdges) {
          if (!focusIds.has(edge.source_identity_hex) && !focusIds.has(edge.target_identity_hex)) continue;
          const sourceNode = data.nodeIndex.get(edge.source_identity_hex);
          const targetNode = data.nodeIndex.get(edge.target_identity_hex);
          if (!sourceNode || !targetNode) continue;
          if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
          L.polyline([
            [sourceNode.latitude, sourceNode.longitude],
            [targetNode.latitude, targetNode.longitude],
          ], {
            color: 'rgba(122, 97, 0, 0.55)',
            weight: 2,
            opacity: 0.86,
            dashArray: '6 8',
          }).addTo(linksLayer);
          addDirectionalArrow(sourceNode, targetNode, 'rgba(122, 97, 0, 0.55)', 0.48);
        }
      };
      drawReachabilityTree(reachability);
      if (routeSourceId && !routeTargetId) {
        drawHistoricalContext(new Set([routeSourceId]));
      }
      drawRoute(forward, '#2c71d1');
      drawRoute(backward, '#cfaa38');
      drawRoute(historicalForward, 'rgba(44, 113, 209, 0.72)', '7 7');
      drawRoute(historicalBackward, 'rgba(207, 170, 56, 0.78)', '7 7');
      const labelNodes = routeSourceId
        ? allMapNodes.filter((node) => highlightedIds.has(node.identity_hex))
        : allMapNodes;
      renderLabels(labelNodes, highlightedIds);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function render(state) {
      latestState = state;
      normalizeVisibleSelections(state);
      renderLegend();
      renderSummary(state);
      renderNodeSections(state);
      for (const button of document.querySelectorAll('[data-global-language]')) {
        button.classList.toggle('active', button.dataset.globalLanguage === currentLanguage);
        button.onclick = () => setLanguage(button.dataset.globalLanguage);
      }
      syncSidebarSheetState();
      applyMobileView();
      renderMap(state);
      void refreshFocusedDataIfNeeded();
    }

    function hasActiveProbeJobs(state) {
      return Boolean((state?.probe_jobs || []).some((job) => job.status === 'pending' || job.status === 'running'));
    }

    function refreshIntervalMs() {
      if (document.hidden) return null;
      return hasActiveProbeJobs(latestState) ? ACTIVE_PROBE_REFRESH_INTERVAL_MS : IDLE_REFRESH_INTERVAL_MS;
    }

    function scheduleRefresh(delayMs = null) {
      if (refreshTimerId !== null) {
        window.clearTimeout(refreshTimerId);
        refreshTimerId = null;
      }
      const nextDelay = delayMs ?? refreshIntervalMs();
      if (nextDelay === null) return;
      refreshTimerId = window.setTimeout(() => {
        void refresh();
      }, nextDelay);
    }

    async function refresh(force = false) {
      if (refreshInFlight) return refreshInFlight;
      refreshInFlight = (async () => {
        const headers = {};
        if (latestStateEtag && !force) {
          headers['If-None-Match'] = latestStateEtag;
        }
        const response = await fetch('/api/state', {
          headers,
          cache: force ? 'no-store' : 'default',
        });
        if (response.status === 304) {
          return;
        }
        if (!response.ok) {
          throw new Error(`state refresh failed: ${response.status}`);
        }
        latestStateEtag = response.headers.get('etag') || latestStateEtag;
        const state = commitState(await response.json());
        if (isSidebarInteractionActive()) {
          pendingRefreshState = state;
          return;
        }
        render(state);
      })();

      try {
        await refreshInFlight;
      } catch (error) {
        console.error('Dashboard refresh failed', error);
        scheduleRefresh(ERROR_REFRESH_INTERVAL_MS);
      } finally {
        refreshInFlight = null;
        if (refreshTimerId === null) {
          scheduleRefresh();
        }
      }
    }

    async function refreshManagement(force = false) {
      if (!currentPanelNeedsManagement()) {
        return;
      }
      const includeHistorical = currentPanel === 'connectivity';
      if (managementRefreshInFlight) return managementRefreshInFlight;
      managementRefreshInFlight = (async () => {
        const headers = {};
        if (latestManagementEtag && !force && latestManagementIncludesHistorical === includeHistorical) {
          headers['If-None-Match'] = latestManagementEtag;
        }
        const endpoint = includeHistorical ? '/api/management?include_historical=1' : '/api/management';
        const response = await fetch(endpoint, {
          headers,
          cache: force ? 'no-store' : 'default',
        });
        if (response.status === 304) {
          latestManagementLoaded = true;
          latestManagementIncludesHistorical = includeHistorical;
          return;
        }
        if (!response.ok) {
          throw new Error(`management refresh failed: ${response.status}`);
        }
        latestManagementEtag = response.headers.get('etag') || latestManagementEtag;
        commitManagement(await response.json(), includeHistorical);
        if (!latestState) {
          return;
        }
        if (isSidebarInteractionActive()) {
          pendingRefreshState = latestState;
          return;
        }
        render(latestState);
      })();

      try {
        await managementRefreshInFlight;
      } catch (error) {
        console.error('Dashboard management refresh failed', error);
      } finally {
        managementRefreshInFlight = null;
      }
    }

    async function refreshSignalHistory(node, force = false) {
      if (!node) return;
      const nodeKey = selectedHistoryNodeKey(node);
      if (!nodeKey) return;
      if (signalHistoryRefreshInFlightByNode.has(nodeKey)) {
        return signalHistoryRefreshInFlightByNode.get(nodeKey);
      }
      if (signalHistoryLoadedNodes.has(nodeKey) && !force) {
        return;
      }
      signalHistoryPendingNodes.add(nodeKey);
      if (latestState) render(latestState);
      const requestPromise = (async () => {
        const response = await fetch(`/api/repeaters/${encodeURIComponent(node.id)}/signal-history`, {
          cache: force ? 'no-store' : 'default',
        });
        if (!response.ok) {
          throw new Error(`signal history refresh failed: ${response.status}`);
        }
        const payload = await response.json();
        signalHistoryByNode = {
          ...signalHistoryByNode,
          [nodeKey]: Array.isArray(payload.rows) ? payload.rows : [],
        };
        signalHistoryLoadedNodes.add(nodeKey);
      })();
      signalHistoryRefreshInFlightByNode.set(nodeKey, requestPromise);
      try {
        await requestPromise;
      } catch (error) {
        console.error('Dashboard signal history refresh failed', error);
      } finally {
        signalHistoryRefreshInFlightByNode.delete(nodeKey);
        signalHistoryPendingNodes.delete(nodeKey);
        if (latestState && selectedSourceId === node.identity_hex) {
          render(latestState);
        }
      }
    }

    async function refreshFocusedDataIfNeeded(options = {}) {
      const force = Boolean(options.force);
      if (!latestState || document.hidden) {
        return;
      }
      const includeHistorical = currentPanel === 'connectivity';
      const needsManagementRefresh = force || !latestManagementLoaded || (includeHistorical && !latestManagementIncludesHistorical);
      if (currentPanelNeedsManagement() && needsManagementRefresh) {
        await refreshManagement(force);
      }
      const selectedNode = getSelectedNode(latestState);
      if (selectedNodeNeedsManagement() && selectedNeighborId && selectedNode) {
        await refreshSignalHistory(selectedNode, force);
      }
    }

    map.on('click', () => {
      hoveredNodeId = null;
      if (armBlankMapClear()) {
        suppressUpcomingDoubleClickZoom();
        clearSelection();
        return;
      }
      if (latestState) renderMap(latestState);
    });
    map.on('zoomend', () => {
      if (latestState) renderMap(latestState);
    });
    const sheetToggle = document.getElementById('sheet-toggle');
    if (sheetToggle) {
      sheetToggle.addEventListener('click', toggleSidebarSheet);
    }
    window.addEventListener('resize', () => {
      applyMobileView();
      syncSidebarSheetState();
    });
    document.addEventListener('focusin', () => {
      if (!isSidebarInteractionActive()) return;
      pendingRefreshState = null;
    });
    document.addEventListener('focusout', () => {
      window.setTimeout(flushPendingRefresh, 0);
    });
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        if (refreshTimerId !== null) {
          window.clearTimeout(refreshTimerId);
          refreshTimerId = null;
        }
        return;
      }
      void Promise.all([refresh(), refreshFocusedDataIfNeeded()]);
    });

    document.documentElement.lang = currentLanguage;
    applyMobileView();
    renderLegend();
    void refresh(true);
  </script>
</body>
</html>
"""


ADMIN_HTML = """<!doctype html>
<html lang="pl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MeshCore Admin</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #eef1e8;
      --panel: rgba(255, 252, 246, 0.94);
      --panel-strong: #fffdf9;
      --line: rgba(33, 43, 50, 0.12);
      --line-strong: rgba(33, 43, 50, 0.22);
      --ink: #17212b;
      --muted: #68757e;
      --accent: #146356;
      --accent-strong: #0f4e44;
      --warn: #b96918;
      --danger: #b13c2e;
      --shadow: 0 24px 60px rgba(23, 33, 43, 0.12);
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at top left, rgba(20, 99, 86, 0.12), transparent 32%),
        linear-gradient(160deg, #eef1e8 0%, #f6f0e6 48%, #f1f3ef 100%);
      color: var(--ink);
      font-family: Georgia, 'Iowan Old Style', 'Palatino Linotype', serif;
      -webkit-text-size-adjust: 100%;
    }
    body {
      padding: 24px;
    }
    .shell {
      display: grid;
      gap: 18px;
      width: min(1440px, 100%);
      margin: 0 auto;
    }
    .topbar,
    .card,
    .tab-panel {
      border: 1px solid var(--line);
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 22px;
      border-radius: 24px;
    }
    .topbar h1 {
      margin: 0;
      font-size: clamp(1.5rem, 3vw, 2.2rem);
      font-weight: 700;
    }
    .subtle {
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.45;
    }
    .actions,
    .tabs,
    .metrics {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .actions a,
    button,
    .tab-button {
      border: 1px solid var(--line-strong);
      background: var(--panel-strong);
      color: var(--ink);
      border-radius: 999px;
      padding: 10px 15px;
      font: inherit;
      cursor: pointer;
      text-decoration: none;
      transition: transform 0.15s ease, border-color 0.15s ease, background 0.15s ease;
    }
    button:hover,
    .actions a:hover,
    .tab-button:hover {
      transform: translateY(-1px);
      border-color: rgba(20, 99, 86, 0.32);
    }
    .tab-button.is-active,
    .button-primary {
      background: var(--accent);
      color: #f7f6f1;
      border-color: transparent;
    }
    .button-danger {
      color: var(--danger);
      border-color: rgba(177, 60, 46, 0.25);
    }
    .grid {
      display: grid;
      gap: 18px;
    }
    .dashboard-grid {
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }
    .metric {
      padding: 16px 18px;
      border-radius: 20px;
      background: rgba(255, 255, 255, 0.62);
      border: 1px solid rgba(23, 33, 43, 0.08);
    }
    .metric strong {
      display: block;
      font-size: 1.9rem;
      margin-top: 6px;
    }
    .card,
    .tab-panel {
      border-radius: 24px;
      padding: 20px;
    }
    .tab-panel[hidden],
    #dashboard[hidden],
    #loginCard[hidden],
    #logoutButton[hidden] {
      display: none !important;
    }
    .section-title {
      margin: 0 0 6px;
      font-size: 1.18rem;
    }
    .section-head {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    .table-wrap {
      overflow: auto;
      border-radius: 16px;
      border: 1px solid rgba(23, 33, 43, 0.08);
      background: rgba(255, 255, 255, 0.55);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 720px;
    }
    th,
    td {
      padding: 12px 14px;
      border-bottom: 1px solid rgba(23, 33, 43, 0.08);
      text-align: left;
      vertical-align: top;
      font-size: 0.92rem;
    }
    th {
      color: var(--muted);
      font-size: 0.8rem;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }
    .form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }
    label {
      display: grid;
      gap: 6px;
      font-size: 0.86rem;
      color: var(--muted);
    }
    input,
    textarea,
    select {
      width: 100%;
      padding: 10px 12px;
      border-radius: 14px;
      border: 1px solid rgba(23, 33, 43, 0.12);
      background: rgba(255, 255, 255, 0.78);
      color: var(--ink);
      font: inherit;
    }
    textarea {
      min-height: 92px;
      resize: vertical;
    }
    .checkbox-row {
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--ink);
    }
    .checkbox-row input {
      width: auto;
    }
    .endpoint-list {
      display: grid;
      gap: 12px;
    }
    .endpoint-row {
      border: 1px solid rgba(23, 33, 43, 0.1);
      border-radius: 18px;
      padding: 16px;
      background: rgba(255, 255, 255, 0.58);
      display: grid;
      gap: 12px;
    }
    .endpoint-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .inline-note {
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid rgba(185, 105, 24, 0.18);
      background: rgba(185, 105, 24, 0.08);
      color: #6f4510;
      font-size: 0.88rem;
      line-height: 1.45;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 5px 10px;
      border-radius: 999px;
      background: rgba(20, 99, 86, 0.08);
      color: var(--accent-strong);
      font-size: 0.82rem;
    }
    .pill.warn {
      background: rgba(185, 105, 24, 0.1);
      color: #8b4f12;
    }
    .pill.danger {
      background: rgba(177, 60, 46, 0.1);
      color: var(--danger);
    }
    .log-block {
      border-radius: 18px;
      border: 1px solid rgba(23, 33, 43, 0.08);
      background: rgba(20, 28, 34, 0.94);
      color: #d8e5ea;
      padding: 14px;
      overflow: auto;
      white-space: pre-wrap;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.82rem;
      line-height: 1.45;
    }
    #toast {
      position: fixed;
      right: 18px;
      bottom: 18px;
      padding: 12px 16px;
      border-radius: 14px;
      background: rgba(23, 33, 43, 0.92);
      color: #f7f6f1;
      box-shadow: var(--shadow);
      opacity: 0;
      transform: translateY(10px);
      pointer-events: none;
      transition: opacity 0.2s ease, transform 0.2s ease;
    }
    #toast.is-visible {
      opacity: 1;
      transform: translateY(0);
    }
    @media (max-width: 900px) {
      body { padding: 14px; }
      .topbar,
      .card,
      .tab-panel { border-radius: 20px; }
      table { min-width: 580px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div>
        <h1>MeshCore Admin</h1>
        <div class="subtle">Konfiguracja backendów, kolejki probe, logi i stan node bez wychodzenia ze strony.</div>
      </div>
      <div class="actions">
        <span id="sessionBadge" class="pill">Niezalogowany</span>
        <a href="/">Widok publiczny</a>
        <button id="logoutButton" hidden>Wyloguj</button>
      </div>
    </header>

    <section id="loginCard" class="card">
      <div class="section-head">
        <div>
          <h2 class="section-title">Logowanie administratora</h2>
          <div class="subtle">Domyślnie: admin / admin.</div>
        </div>
      </div>
      <form id="loginForm" class="form-grid">
        <label>
          Login
          <input id="loginUsername" type="text" value="admin" autocomplete="username">
        </label>
        <label>
          Hasło
          <input id="loginPassword" type="password" value="admin" autocomplete="current-password">
        </label>
        <div style="display:flex;align-items:flex-end;">
          <button class="button-primary" type="submit">Zaloguj</button>
        </div>
      </form>
    </section>

    <section id="dashboard" hidden>
      <div class="tabs">
        <button class="tab-button is-active" data-tab="status">Status</button>
        <button class="tab-button" data-tab="config">Konfiguracja</button>
        <button class="tab-button" data-tab="logs">Logi</button>
        <button class="tab-button" data-tab="nodes">Node</button>
      </div>

      <section id="tab-status" class="tab-panel">
        <div class="section-head">
          <div>
            <h2 class="section-title">Stan systemu</h2>
            <div id="statusSubtitle" class="subtle"></div>
          </div>
          <div class="actions">
            <button id="refreshButton" type="button">Odśwież</button>
            <button id="wakeupButton" type="button">Wake probe worker</button>
            <button id="cleanupButton" type="button">Czyść stare błędy</button>
          </div>
        </div>
        <div id="metrics" class="metrics"></div>
        <div class="grid dashboard-grid" style="margin-top:16px;">
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Backendy</h3>
            <div class="table-wrap"><table><thead><tr><th>Nazwa</th><th>Adres</th><th>Stan</th><th>Ruch</th><th>Ostatni advert</th></tr></thead><tbody id="endpointStatusBody"></tbody></table></div>
          </div>
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Ostatnie błędy</h3>
            <div class="table-wrap"><table><thead><tr><th>Job</th><th>Endpoint</th><th>Node</th><th>Błąd</th></tr></thead><tbody id="failureBody"></tbody></table></div>
          </div>
        </div>
      </section>

      <section id="tab-config" class="tab-panel" hidden>
        <div class="section-head">
          <div>
            <h2 class="section-title">Konfiguracja</h2>
            <div id="configPath" class="subtle"></div>
          </div>
          <div class="actions">
            <button id="addEndpointButton" type="button">Dodaj backend</button>
            <button id="saveConfigButton" class="button-primary" type="button">Zapisz config</button>
          </div>
        </div>
        <div class="inline-note">Zapis trafia do pliku konfiguracyjnego od razu. Web użyje nowych ustawień natychmiast, ale worker bot/probe może wymagać restartu stacka po zmianach backendów lub haseł.</div>
        <div class="grid" style="margin-top:16px;">
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Serwis i WWW</h3>
            <div class="form-grid">
              <label>Service name<input id="cfg-service-name" type="text"></label>
              <label>Log level<input id="cfg-log-level" type="text"></label>
              <label>Web host<input id="cfg-web-host" type="text"></label>
              <label>Web port<input id="cfg-web-port" type="number"></label>
              <label>Admin login<input id="cfg-admin-username" type="text"></label>
              <label>Admin hasło<input id="cfg-admin-password" type="text"></label>
            </div>
          </div>
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Bot</h3>
            <div class="form-grid">
              <label class="checkbox-row"><input id="cfg-bot-enabled" type="checkbox">Bot aktywny</label>
              <label>Sender name<input id="cfg-bot-sender-name" type="text"></label>
              <label>Reply endpoint<input id="cfg-bot-reply-endpoint" type="text"></label>
              <label>Channels (comma or newline)<textarea id="cfg-bot-channels"></textarea></label>
              <label>Commands (comma or newline)<textarea id="cfg-bot-commands"></textarea></label>
              <label>Min response delay<input id="cfg-bot-min-delay" type="number" step="0.1"></label>
              <label>Response attempts<input id="cfg-bot-attempts" type="number"></label>
              <label>Response attempts max<input id="cfg-bot-attempts-max" type="number"></label>
              <label>Quiet window<input id="cfg-bot-quiet-window" type="number" step="0.1"></label>
              <label>Dedup TTL<input id="cfg-bot-dedup-ttl" type="number" step="0.1"></label>
              <label class="checkbox-row"><input id="cfg-bot-include-test" type="checkbox">Include !test signal</label>
            </div>
          </div>
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Probe</h3>
            <div class="form-grid">
              <label>Admin password<input id="cfg-probe-admin-password" type="text"></label>
              <label>Guest password<input id="cfg-probe-guest-password" type="text"></label>
              <label>Default guest password<input id="cfg-probe-default-guest-password" type="text"></label>
              <label>Pre-login advert name<input id="cfg-probe-pre-login-advert" type="text"></label>
              <label>Poll interval<input id="cfg-probe-poll-interval" type="number" step="0.1"></label>
              <label>Request timeout<input id="cfg-probe-request-timeout" type="number" step="0.1"></label>
              <label>Route freshness<input id="cfg-probe-route-freshness" type="number" step="0.1"></label>
              <label>Scheduled reprobe interval<input id="cfg-probe-scheduled-interval" type="number" step="0.1"></label>
              <label>Scheduled reprobe max batch<input id="cfg-probe-scheduled-batch" type="number"></label>
              <label>Seen within secs<input id="cfg-probe-seen-within" type="number" step="0.1"></label>
              <label>Night retry interval<input id="cfg-probe-night-interval" type="number" step="0.1"></label>
              <label>Night retry max batch<input id="cfg-probe-night-batch" type="number"></label>
              <label>Advert min interval<input id="cfg-probe-advert-min-interval" type="number" step="0.1"></label>
              <label>Advert failure cooldown<input id="cfg-probe-advert-failure-cooldown" type="number" step="0.1"></label>
            </div>
          </div>
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Gateway i backendy</h3>
            <div class="form-grid">
              <label>Traffic watchdog<input id="cfg-gateway-watchdog" type="number" step="0.1"></label>
              <label>Close timeout<input id="cfg-gateway-close-timeout" type="number" step="0.1"></label>
              <label>Console probe timeout<input id="cfg-gateway-console-timeout" type="number" step="0.1"></label>
            </div>
            <div id="endpointList" class="endpoint-list" style="margin-top:16px;"></div>
          </div>
        </div>
      </section>

      <section id="tab-logs" class="tab-panel" hidden>
        <div class="section-head">
          <div>
            <h2 class="section-title">Logi i historia</h2>
            <div class="subtle">Ostatnie probe jobs, surowe pakiety i pliki logów z katalogu logs/.</div>
          </div>
        </div>
        <div class="grid dashboard-grid">
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Probe jobs</h3>
            <div class="table-wrap"><table><thead><tr><th>ID</th><th>Status</th><th>Endpoint</th><th>Node</th><th>Szczegóły</th></tr></thead><tbody id="jobBody"></tbody></table></div>
          </div>
          <div class="card" style="padding:16px;">
            <h3 class="section-title">Raw packets</h3>
            <div class="table-wrap"><table><thead><tr><th>ID</th><th>Endpoint</th><th>Kierunek</th><th>Typ</th><th>Remote</th></tr></thead><tbody id="packetBody"></tbody></table></div>
          </div>
        </div>
        <div id="fileLogs" class="grid" style="margin-top:16px;"></div>
      </section>

      <section id="tab-nodes" class="tab-panel" hidden>
        <div class="section-head">
          <div>
            <h2 class="section-title">Node i repeatery</h2>
            <div class="subtle">Ręczne kolejkowanie probe dla wybranych wpisów.</div>
          </div>
        </div>
        <div class="table-wrap"><table><thead><tr><th>ID</th><th>Nazwa</th><th>Identity</th><th>Last advert</th><th>Last probe</th><th>Data</th><th>Akcja</th></tr></thead><tbody id="nodeBody"></tbody></table></div>
      </section>
    </section>
  </div>

  <div id="toast"></div>

  <script>
    const state = {
      dashboard: null,
      config: null,
      logs: null,
    };

    const $ = (selector) => document.querySelector(selector);
    const escapeHtml = (value) => String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');

    async function api(url, options = {}) {
      const headers = new Headers(options.headers || {});
      if (!headers.has('Content-Type') && options.body !== undefined) {
        headers.set('Content-Type', 'application/json');
      }
      const response = await fetch(url, { ...options, headers });
      const text = await response.text();
      let payload = {};
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch {
          payload = { detail: text };
        }
      }
      if (response.status === 401) {
        showLogin();
      }
      if (!response.ok) {
        throw new Error(payload.detail || 'Request failed');
      }
      return payload;
    }

    function showToast(message, isError = false) {
      const toast = $('#toast');
      toast.textContent = message;
      toast.style.background = isError ? 'rgba(177, 60, 46, 0.94)' : 'rgba(23, 33, 43, 0.92)';
      toast.classList.add('is-visible');
      clearTimeout(showToast.timer);
      showToast.timer = setTimeout(() => toast.classList.remove('is-visible'), 2600);
    }

    function setTab(name) {
      document.querySelectorAll('.tab-button').forEach((button) => {
        button.classList.toggle('is-active', button.dataset.tab === name);
      });
      document.querySelectorAll('.tab-panel').forEach((panel) => {
        panel.hidden = panel.id !== `tab-${name}`;
      });
    }

    function showLogin() {
      $('#loginCard').hidden = false;
      $('#dashboard').hidden = true;
      $('#logoutButton').hidden = true;
      $('#sessionBadge').textContent = 'Niezalogowany';
    }

    function showDashboard(username) {
      $('#loginCard').hidden = true;
      $('#dashboard').hidden = false;
      $('#logoutButton').hidden = false;
      $('#sessionBadge').textContent = `Admin: ${username}`;
    }

    function linesToArray(value) {
      return String(value || '')
        .split(/[,\\n]/)
        .map((item) => item.trim())
        .filter(Boolean);
    }

    function endpointRow(entry = {}) {
      const enabled = entry.enabled !== false;
      return `
        <div class="endpoint-row">
          <div class="endpoint-head">
            <strong>${escapeHtml(entry.name || 'Nowy backend')}</strong>
            <button type="button" class="button-danger" data-action="remove-endpoint">Usuń</button>
          </div>
          <div class="form-grid">
            <label>Nazwa<input data-field="name" type="text" value="${escapeHtml(entry.name || '')}"></label>
            <label>Raw host<input data-field="raw_host" type="text" value="${escapeHtml(entry.raw_host || '')}"></label>
            <label>Raw port<input data-field="raw_port" type="number" value="${escapeHtml(entry.raw_port ?? 5002)}"></label>
            <label>Console port<input data-field="console_port" type="number" value="${escapeHtml(entry.console_port ?? 5001)}"></label>
            <label>Local node name<input data-field="local_node_name" type="text" value="${escapeHtml(entry.local_node_name || '')}"></label>
            <label>Mirror host<input data-field="console_mirror_host" type="text" value="${escapeHtml(entry.console_mirror_host || '')}"></label>
            <label>Mirror port<input data-field="console_mirror_port" type="number" value="${escapeHtml(entry.console_mirror_port ?? '')}"></label>
            <label class="checkbox-row"><input data-field="enabled" type="checkbox" ${enabled ? 'checked' : ''}>Backend aktywny</label>
          </div>
        </div>
      `;
    }

    function renderStatus() {
      const dashboard = state.dashboard;
      if (!dashboard) {
        return;
      }
      $('#statusSubtitle').textContent = `Config: ${dashboard.config_path} | Wake socket: ${dashboard.wakeup_socket}`;
      const metrics = [
        ['Repeaters', dashboard.overview.repeater_count ?? 0],
        ['Packets', dashboard.overview.raw_packet_count ?? 0],
        ['Node z danymi', dashboard.summary.nodes_with_data ?? 0],
        ['Aktywne probe', dashboard.summary.active_jobs ?? 0],
        ['Błędy (ostatnie)', dashboard.summary.recent_failures ?? 0],
      ];
      $('#metrics').innerHTML = metrics.map(([label, value]) => `
        <div class="metric">
          <div class="subtle">${escapeHtml(label)}</div>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `).join('');
      $('#endpointStatusBody').innerHTML = (dashboard.endpoints || []).map((endpoint) => `
        <tr>
          <td><strong>${escapeHtml(endpoint.name)}</strong><br><span class="subtle">${escapeHtml((endpoint.sample_repeaters || []).join(', '))}</span></td>
          <td>${escapeHtml(endpoint.raw_host)}:${escapeHtml(endpoint.raw_port)}</td>
          <td>${endpoint.enabled ? '<span class="pill">enabled</span>' : '<span class="pill danger">disabled</span>'}</td>
          <td>${escapeHtml(endpoint.seen_repeater_count)} node<br><span class="subtle">jobs ${escapeHtml(endpoint.recent_job_count)} | fail ${escapeHtml(endpoint.recent_failed_count)}</span></td>
          <td>${escapeHtml(endpoint.last_advert_at || 'brak')}</td>
        </tr>
      `).join('');
      $('#failureBody').innerHTML = (dashboard.recent_failures || []).map((job) => `
        <tr>
          <td>#${escapeHtml(job.id)}</td>
          <td>${escapeHtml(job.endpoint_name || '')}</td>
          <td>${escapeHtml(job.last_name_from_advert || job.pubkey_hex || '')}</td>
          <td>${escapeHtml(job.last_error || 'brak')}</td>
        </tr>
      `).join('');
    }

    function renderConfig() {
      const config = state.config;
      if (!config) {
        return;
      }
      $('#configPath').textContent = config.config_path;
      $('#cfg-service-name').value = config.service.name || '';
      $('#cfg-log-level').value = config.service.log_level || '';
      $('#cfg-web-host').value = config.web.host || '';
      $('#cfg-web-port').value = config.web.port ?? '';
      $('#cfg-admin-username').value = config.web.admin_username || '';
      $('#cfg-admin-password').value = config.web.admin_password || '';
      $('#cfg-bot-enabled').checked = !!config.bot.enabled;
      $('#cfg-bot-sender-name').value = config.bot.sender_name || '';
      $('#cfg-bot-reply-endpoint').value = config.bot.reply_endpoint_name || '';
      $('#cfg-bot-channels').value = (config.bot.channels || []).join('\\n');
      $('#cfg-bot-commands').value = (config.bot.enabled_commands || []).join('\\n');
      $('#cfg-bot-min-delay').value = config.bot.min_response_delay_secs ?? '';
      $('#cfg-bot-attempts').value = config.bot.response_attempts ?? '';
      $('#cfg-bot-attempts-max').value = config.bot.response_attempts_max ?? '';
      $('#cfg-bot-quiet-window').value = config.bot.quiet_window_secs ?? '';
      $('#cfg-bot-dedup-ttl').value = config.bot.command_dedup_ttl_secs ?? '';
      $('#cfg-bot-include-test').checked = !!config.bot.include_test_signal;
      $('#cfg-probe-admin-password').value = config.probe.admin_password || '';
      $('#cfg-probe-guest-password').value = config.probe.guest_password || '';
      $('#cfg-probe-default-guest-password').value = config.probe.default_guest_password || '';
      $('#cfg-probe-pre-login-advert').value = config.probe.pre_login_advert_name || '';
      $('#cfg-probe-poll-interval').value = config.probe.poll_interval_secs ?? '';
      $('#cfg-probe-request-timeout').value = config.probe.request_timeout_secs ?? '';
      $('#cfg-probe-route-freshness').value = config.probe.route_freshness_secs ?? '';
      $('#cfg-probe-scheduled-interval').value = config.probe.scheduled_reprobe_interval_secs ?? '';
      $('#cfg-probe-scheduled-batch').value = config.probe.scheduled_reprobe_max_batch ?? '';
      $('#cfg-probe-seen-within').value = config.probe.scheduled_reprobe_seen_within_secs ?? '';
      $('#cfg-probe-night-interval').value = config.probe.night_failed_retry_interval_secs ?? '';
      $('#cfg-probe-night-batch').value = config.probe.night_failed_retry_max_batch ?? '';
      $('#cfg-probe-advert-min-interval').value = config.probe.advert_probe_min_interval_secs ?? '';
      $('#cfg-probe-advert-failure-cooldown').value = config.probe.advert_reprobe_failure_cooldown_secs ?? '';
      $('#cfg-gateway-watchdog').value = config.gateway.traffic_watchdog_secs ?? '';
      $('#cfg-gateway-close-timeout').value = config.gateway.close_timeout_secs ?? '';
      $('#cfg-gateway-console-timeout').value = config.gateway.console_probe_timeout_secs ?? '';
      $('#endpointList').innerHTML = (config.endpoints || []).map((entry) => endpointRow(entry)).join('');
    }

    function renderLogs() {
      const logs = state.logs;
      if (!logs) {
        return;
      }
      $('#jobBody').innerHTML = (logs.recent_jobs || []).map((job) => `
        <tr>
          <td>#${escapeHtml(job.id)}</td>
          <td>${escapeHtml(job.status || '')}</td>
          <td>${escapeHtml(job.endpoint_name || '')}</td>
          <td>${escapeHtml(job.last_name_from_advert || job.pubkey_hex || '')}</td>
          <td>${escapeHtml(job.last_error || job.reason || '')}</td>
        </tr>
      `).join('');
      $('#packetBody').innerHTML = (logs.recent_packets || []).map((packet) => `
        <tr>
          <td>#${escapeHtml(packet.id)}</td>
          <td>${escapeHtml(packet.endpoint_name || '')}<br><span class="subtle">${escapeHtml(packet.observed_at || '')}</span></td>
          <td>${escapeHtml(packet.direction || '')}</td>
          <td>${escapeHtml(packet.payload_type || '')}<br><span class="subtle">${escapeHtml(packet.route_type || '')}</span></td>
          <td>${escapeHtml(packet.remote_pubkey_hex || '')}<br><span class="subtle">${escapeHtml(packet.request_tag || packet.notes || '')}</span></td>
        </tr>
      `).join('');
      $('#fileLogs').innerHTML = (logs.file_logs || []).map((entry) => `
        <div class="card" style="padding:16px;">
          <div class="section-head">
            <div>
              <h3 class="section-title">${escapeHtml(entry.name)}</h3>
              <div class="subtle">${escapeHtml(entry.path)}</div>
            </div>
          </div>
          <div class="log-block">${escapeHtml(entry.tail || '')}</div>
        </div>
      `).join('') || '<div class="subtle">Brak plików logów w katalogu logs/.</div>';
    }

    function renderNodes() {
      const dashboard = state.dashboard;
      if (!dashboard) {
        return;
      }
      $('#nodeBody').innerHTML = (dashboard.repeaters || []).map((repeater) => `
        <tr>
          <td>${escapeHtml(repeater.id)}</td>
          <td><strong>${escapeHtml(repeater.name || '')}</strong><br><span class="subtle">${escapeHtml(repeater.role || '')}</span></td>
          <td>${escapeHtml(repeater.identity_hex || '')}</td>
          <td>${escapeHtml(repeater.last_advert_at || repeater.last_seen_at || 'brak')}</td>
          <td>${escapeHtml(repeater.last_probe_status || 'n/a')}<br><span class="subtle">${escapeHtml(repeater.last_probe_at || '')}</span></td>
          <td>${repeater.data_fetch_ok ? '<span class="pill">OK</span>' : '<span class="pill warn">brak</span>'}</td>
          <td><button type="button" data-action="probe" data-repeater-id="${escapeHtml(repeater.id)}">Queue probe</button></td>
        </tr>
      `).join('');
    }

    function collectEndpoints() {
      return Array.from(document.querySelectorAll('.endpoint-row')).map((row) => {
        const lookup = (field) => row.querySelector(`[data-field="${field}"]`);
        return {
          name: lookup('name').value,
          raw_host: lookup('raw_host').value,
          raw_port: Number(lookup('raw_port').value || 5002),
          console_port: lookup('console_port').value === '' ? '' : Number(lookup('console_port').value),
          local_node_name: lookup('local_node_name').value,
          console_mirror_host: lookup('console_mirror_host').value,
          console_mirror_port: lookup('console_mirror_port').value === '' ? '' : Number(lookup('console_mirror_port').value),
          enabled: lookup('enabled').checked,
        };
      });
    }

    function collectConfigPayload() {
      return {
        service: {
          name: $('#cfg-service-name').value,
          log_level: $('#cfg-log-level').value,
        },
        web: {
          host: $('#cfg-web-host').value,
          port: Number($('#cfg-web-port').value || 0),
          admin_username: $('#cfg-admin-username').value,
          admin_password: $('#cfg-admin-password').value,
        },
        bot: {
          enabled: $('#cfg-bot-enabled').checked,
          sender_name: $('#cfg-bot-sender-name').value,
          reply_endpoint_name: $('#cfg-bot-reply-endpoint').value,
          channels: linesToArray($('#cfg-bot-channels').value),
          enabled_commands: linesToArray($('#cfg-bot-commands').value),
          min_response_delay_secs: Number($('#cfg-bot-min-delay').value || 0),
          response_attempts: Number($('#cfg-bot-attempts').value || 0),
          response_attempts_max: Number($('#cfg-bot-attempts-max').value || 0),
          quiet_window_secs: Number($('#cfg-bot-quiet-window').value || 0),
          command_dedup_ttl_secs: Number($('#cfg-bot-dedup-ttl').value || 0),
          include_test_signal: $('#cfg-bot-include-test').checked,
        },
        probe: {
          admin_password: $('#cfg-probe-admin-password').value,
          guest_password: $('#cfg-probe-guest-password').value,
          default_guest_password: $('#cfg-probe-default-guest-password').value,
          pre_login_advert_name: $('#cfg-probe-pre-login-advert').value,
          poll_interval_secs: Number($('#cfg-probe-poll-interval').value || 0),
          request_timeout_secs: Number($('#cfg-probe-request-timeout').value || 0),
          route_freshness_secs: Number($('#cfg-probe-route-freshness').value || 0),
          scheduled_reprobe_interval_secs: Number($('#cfg-probe-scheduled-interval').value || 0),
          scheduled_reprobe_max_batch: Number($('#cfg-probe-scheduled-batch').value || 0),
          scheduled_reprobe_seen_within_secs: Number($('#cfg-probe-seen-within').value || 0),
          night_failed_retry_interval_secs: Number($('#cfg-probe-night-interval').value || 0),
          night_failed_retry_max_batch: Number($('#cfg-probe-night-batch').value || 0),
          advert_probe_min_interval_secs: Number($('#cfg-probe-advert-min-interval').value || 0),
          advert_reprobe_failure_cooldown_secs: Number($('#cfg-probe-advert-failure-cooldown').value || 0),
        },
        gateway: {
          traffic_watchdog_secs: Number($('#cfg-gateway-watchdog').value || 0),
          close_timeout_secs: Number($('#cfg-gateway-close-timeout').value || 0),
          console_probe_timeout_secs: Number($('#cfg-gateway-console-timeout').value || 0),
        },
        endpoints: collectEndpoints(),
      };
    }

    async function refreshAll() {
      const [dashboard, config, logs] = await Promise.all([
        api('/api/admin/dashboard'),
        api('/api/admin/config'),
        api('/api/admin/logs'),
      ]);
      state.dashboard = dashboard;
      state.config = config;
      state.logs = logs;
      renderStatus();
      renderConfig();
      renderLogs();
      renderNodes();
    }

    async function initSession() {
      const session = await api('/api/admin/session');
      if (!session.authenticated) {
        showLogin();
        return;
      }
      showDashboard(session.username || 'admin');
      await refreshAll();
    }

    $('#loginForm').addEventListener('submit', async (event) => {
      event.preventDefault();
      try {
        const payload = await api('/api/admin/login', {
          method: 'POST',
          body: JSON.stringify({
            username: $('#loginUsername').value,
            password: $('#loginPassword').value,
          }),
        });
        showDashboard(payload.session?.username || 'admin');
        await refreshAll();
        showToast('Zalogowano.');
      } catch (error) {
        showToast(error.message || 'Błąd logowania', true);
      }
    });

    $('#logoutButton').addEventListener('click', async () => {
      try {
        await api('/api/admin/logout', { method: 'POST' });
      } catch {
        // ignore logout failures and just reset UI
      }
      showLogin();
      showToast('Wylogowano.');
    });

    $('#refreshButton').addEventListener('click', async () => {
      try {
        await refreshAll();
        showToast('Odświeżono dane.');
      } catch (error) {
        showToast(error.message || 'Nie udało się odświeżyć', true);
      }
    });

    $('#wakeupButton').addEventListener('click', async () => {
      try {
        const payload = await api('/api/admin/actions/wakeup', { method: 'POST' });
        showToast(payload.notified ? 'Worker został obudzony.' : 'Nie udało się połączyć z wake socketem.', !payload.notified);
      } catch (error) {
        showToast(error.message || 'Wakeup failed', true);
      }
    });

    $('#cleanupButton').addEventListener('click', async () => {
      try {
        const payload = await api('/api/admin/actions/cleanup-failed', {
          method: 'POST',
          body: JSON.stringify({ failed_older_than_hours: 24 }),
        });
        await refreshAll();
        showToast(`Usunięto ${payload.deleted_count || 0} starych failed probe job.`);
      } catch (error) {
        showToast(error.message || 'Cleanup failed', true);
      }
    });

    $('#saveConfigButton').addEventListener('click', async () => {
      try {
        const payload = await api('/api/admin/config', {
          method: 'POST',
          body: JSON.stringify(collectConfigPayload()),
        });
        state.config = payload.config;
        renderConfig();
        await refreshAll();
        showToast(payload.restart_required ? 'Config zapisany. Worker może wymagać restartu.' : 'Config zapisany.');
      } catch (error) {
        showToast(error.message || 'Save failed', true);
      }
    });

    $('#addEndpointButton').addEventListener('click', () => {
      $('#endpointList').insertAdjacentHTML('beforeend', endpointRow({ enabled: true, raw_port: 5002, console_port: 5001 }));
    });

    document.addEventListener('click', async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (target.matches('.tab-button')) {
        setTab(target.dataset.tab || 'status');
        return;
      }
      if (target.dataset.action === 'remove-endpoint') {
        target.closest('.endpoint-row')?.remove();
        return;
      }
      if (target.dataset.action === 'probe') {
        try {
          await api('/api/admin/actions/manual-probe', {
            method: 'POST',
            body: JSON.stringify({ repeater_id: Number(target.dataset.repeaterId || 0) }),
          });
          await refreshAll();
          showToast('Probe job zakolejkowany.');
        } catch (error) {
          showToast(error.message || 'Queue probe failed', true);
        }
      }
    });

    setTab('status');
    initSession().catch((error) => {
      showLogin();
      showToast(error.message || 'Nie udało się wczytać sesji', true);
    });
  </script>
</body>
</html>
"""


def create_app(database: BotDatabase, config: AppConfig, *, config_path: str | Path = "config/config.toml") -> FastAPI:
    app = FastAPI(title="meshcore-bot", version="0.1.0")
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    config_holder: dict[str, AppConfig] = {"value": config}
    config_path_holder: dict[str, Path] = {"value": Path(config_path).expanduser().resolve()}

    def active_config() -> AppConfig:
        return config_holder["value"]

    def active_config_path() -> Path:
        return config_path_holder["value"]

    state_cache = _StateSnapshotCache(
        payload_builder=_build_state_payload,
        ttl_builder=_state_cache_ttl_secs,
        cache_control_builder=_state_cache_control,
    )
    management_cache = _StateSnapshotCache(
        payload_builder=lambda db: _build_management_payload(db, include_historical=False),
        ttl_builder=_management_cache_ttl_secs,
        cache_control_builder=_management_cache_control,
    )
    management_with_history_cache = _StateSnapshotCache(
        payload_builder=lambda db: _build_management_payload(db, include_historical=True),
        ttl_builder=_management_cache_ttl_secs,
        cache_control_builder=_management_cache_control,
    )

    async def enqueue_manual_probe(repeater_id: int) -> dict[str, object]:
      repeater = database.repeater_full_state(repeater_id=repeater_id)
      if repeater is None:
        raise HTTPException(status_code=404, detail="unknown repeater")

      active_job = _active_probe_job_for_repeater(database, repeater_id=repeater_id)
      if active_job is not None:
        return {
          "status": "already_pending",
          "job_id": active_job.get("id"),
          "endpoint_name": active_job.get("endpoint_name"),
          "scheduled_at": active_job.get("scheduled_at"),
        }

      current_config = active_config()
      endpoint_name = _resolve_manual_probe_endpoint_name(
        config=current_config,
        database=database,
        repeater_id=repeater_id,
        repeater_name=str(repeater.get("last_name_from_advert") or ""),
      )
      if not endpoint_name:
        raise HTTPException(status_code=503, detail="no enabled endpoint available")

      success_cooldown_secs = max(
        MANUAL_WEB_PROBE_MIN_SUCCESS_COOLDOWN_SECS,
        float(current_config.probe.advert_probe_min_interval_secs),
      )
      failure_cooldown_secs = max(
        float(current_config.probe.advert_reprobe_failure_cooldown_secs),
        float(current_config.probe.request_timeout_secs),
      )
      scheduled_at = _manual_probe_schedule_at(database, config=current_config)
      job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name=endpoint_name,
        reason=MANUAL_WEB_PROBE_REASON,
        success_cooldown_secs=success_cooldown_secs,
        failure_cooldown_secs=failure_cooldown_secs,
        scheduled_at=scheduled_at,
        max_recent_jobs=MANUAL_WEB_PROBE_MAX_PER_WINDOW,
        recent_window_secs=MANUAL_WEB_PROBE_WINDOW_SECS,
      )
      if job_id is not None:
        state_cache.invalidate()
        management_cache.invalidate()
        management_with_history_cache.invalidate()
        if scheduled_at is None:
          await _notify_probe_worker(current_config)
        return {
          "status": "queued",
          "job_id": job_id,
          "endpoint_name": endpoint_name,
          "scheduled_at": scheduled_at,
        }

      active_job = _active_probe_job_for_repeater(database, repeater_id=repeater_id)
      if active_job is not None:
        return {
          "status": "already_pending",
          "job_id": active_job.get("id"),
          "endpoint_name": active_job.get("endpoint_name"),
          "scheduled_at": active_job.get("scheduled_at"),
        }

      return {
        "status": "cooldown",
        "job_id": None,
        "endpoint_name": endpoint_name,
        "scheduled_at": None,
      }

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok", "database": database.snapshot_overview()})

    @app.get("/api/state")
    async def api_state(request: Request) -> Response:
        snapshot = await state_cache.get_snapshot(database)
        headers = {
            "Cache-Control": snapshot.cache_control,
            "ETag": snapshot.etag,
        }
        if _etag_matches(request.headers.get("if-none-match"), snapshot.etag):
            return Response(status_code=304, headers=headers)
        return Response(content=snapshot.payload_bytes, media_type="application/json", headers=headers)

    @app.get("/api/management")
    async def api_management(request: Request, include_historical: bool = False) -> Response:
        snapshot = await (management_with_history_cache if include_historical else management_cache).get_snapshot(database)
        headers = {
            "Cache-Control": snapshot.cache_control,
            "ETag": snapshot.etag,
        }
        if _etag_matches(request.headers.get("if-none-match"), snapshot.etag):
            return Response(status_code=304, headers=headers)
        return Response(content=snapshot.payload_bytes, media_type="application/json", headers=headers)

    @app.get("/api/repeaters/{repeater_id}/signal-history")
    async def api_repeater_signal_history(repeater_id: int) -> JSONResponse:
        repeater = database.repeater_full_state(repeater_id=repeater_id)
        if repeater is None:
            raise HTTPException(status_code=404, detail="unknown repeater")
        return JSONResponse({"rows": database.repeater_signal_history(repeater_id=repeater_id, limit_samples=128)})

    @app.post("/api/probe-jobs")
    async def create_probe_job(payload: ProbeJobCreatePayload) -> JSONResponse:
      return JSONResponse(await enqueue_manual_probe(int(payload.repeater_id)))

    @app.get("/api/admin/session")
    async def admin_session(request: Request) -> JSONResponse:
      current_config = active_config()
      if not _is_admin_authenticated(request, current_config):
        return JSONResponse({"authenticated": False})
      return JSONResponse(_admin_session_payload(current_config))

    @app.post("/api/admin/login")
    async def admin_login(request: Request, payload: AdminLoginPayload) -> JSONResponse:
      current_config = active_config()
      if payload.username != _current_admin_username(current_config) or payload.password != current_config.web.admin_password:
        raise HTTPException(status_code=401, detail="invalid credentials")
      response = JSONResponse({"status": "ok", "session": _admin_session_payload(current_config)})
      _set_admin_cookie(response, request, current_config)
      return response

    @app.post("/api/admin/logout")
    async def admin_logout() -> JSONResponse:
      response = JSONResponse({"status": "ok"})
      _clear_admin_cookie(response)
      return response

    @app.get("/api/admin/dashboard")
    async def admin_dashboard(request: Request) -> JSONResponse:
      _require_admin(request, active_config())
      return JSONResponse(_build_admin_dashboard_payload(database, active_config(), config_path=active_config_path()))

    @app.get("/api/admin/config")
    async def admin_config(request: Request) -> JSONResponse:
      _require_admin(request, active_config())
      return JSONResponse(_admin_config_payload(active_config(), config_path=active_config_path()))

    @app.post("/api/admin/config")
    async def admin_config_save(request: Request) -> JSONResponse:
      _require_admin(request, active_config())
      payload = await request.json()
      try:
        _, raw_config = load_raw_config(active_config_path())
        updated_raw_config = _apply_admin_config_payload(raw_config, payload)
        validated_config = _save_validated_config(active_config_path(), updated_raw_config)
      except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
      except Exception as exc:
        raise HTTPException(status_code=400, detail=f"failed to save config: {exc}") from exc

      config_holder["value"] = validated_config
      state_cache.invalidate()
      management_cache.invalidate()
      management_with_history_cache.invalidate()
      response = JSONResponse(
        {
          "status": "saved",
          "config": _admin_config_payload(validated_config, config_path=active_config_path()),
          "restart_required": True,
        }
      )
      _set_admin_cookie(response, request, validated_config)
      return response

    @app.get("/api/admin/logs")
    async def admin_logs(request: Request) -> JSONResponse:
      _require_admin(request, active_config())
      return JSONResponse(_build_admin_logs_payload(database, config_path=active_config_path()))

    @app.post("/api/admin/actions/wakeup")
    async def admin_wakeup(request: Request) -> JSONResponse:
      _require_admin(request, active_config())
      notified = await _notify_probe_worker(active_config())
      return JSONResponse({"status": "ok", "notified": notified})

    @app.post("/api/admin/actions/cleanup-failed")
    async def admin_cleanup_failed(request: Request, payload: AdminCleanupPayload) -> JSONResponse:
      _require_admin(request, active_config())
      deleted_count = database.delete_failed_probe_jobs_older_than(
        older_than_secs=max(0.0, float(payload.failed_older_than_hours)) * 3600.0,
      )
      management_cache.invalidate()
      management_with_history_cache.invalidate()
      return JSONResponse({"status": "ok", "deleted_count": deleted_count})

    @app.post("/api/admin/actions/manual-probe")
    async def admin_manual_probe(request: Request, payload: ProbeJobCreatePayload) -> JSONResponse:
      _require_admin(request, active_config())
      return JSONResponse(await enqueue_manual_probe(int(payload.repeater_id)))

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_root() -> HTMLResponse:
      return HTMLResponse(ADMIN_HTML)

    @app.get("/", response_class=HTMLResponse)
    async def root() -> HTMLResponse:
        return HTMLResponse(INDEX_HTML)

    return app
