from __future__ import annotations

import asyncio
import copy
import hmac
import hashlib
import json
import secrets
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
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
ADMIN_LOGIN_RATE_LIMIT_MAX_ATTEMPTS = 5
ADMIN_LOGIN_RATE_LIMIT_WINDOW_SECS = 300.0
_ADMIN_SESSION_SECRET = secrets.token_bytes(32)
_ADMIN_LOGIN_ATTEMPTS: dict[str, deque[float]] = {}
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


def _sign_admin_cookie(username: str, expires_at: int) -> str:
    msg = f"{username}|{expires_at}".encode("utf-8")
    sig = hmac.new(_ADMIN_SESSION_SECRET, msg, hashlib.sha256).hexdigest()
    return f"{username}|{expires_at}|{sig}"


def _admin_cookie_value(config: AppConfig) -> str:
    expires_at = int(time.time()) + ADMIN_COOKIE_MAX_AGE_SECS
    return _sign_admin_cookie(_current_admin_username(config), expires_at)


def _verify_admin_cookie(cookie: str, config: AppConfig) -> bool:
    parts = cookie.split("|")
    if len(parts) != 3:
        return False
    username, exp_raw, sig = parts
    if username != _current_admin_username(config):
        return False
    try:
        expires_at = int(exp_raw)
    except ValueError:
        return False
    if expires_at < int(time.time()):
        return False
    expected = _sign_admin_cookie(username, expires_at)
    return hmac.compare_digest(cookie, expected)


def _client_ip(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for", "")).split(",", 1)[0].strip()
    if forwarded:
        return forwarded
    if request.client is not None:
        return request.client.host or "unknown"
    return "unknown"


def _check_login_rate_limit(request: Request) -> None:
    now = time.monotonic()
    ip = _client_ip(request)
    bucket = _ADMIN_LOGIN_ATTEMPTS.setdefault(ip, deque())
    cutoff = now - ADMIN_LOGIN_RATE_LIMIT_WINDOW_SECS
    while bucket and bucket[0] < cutoff:
        bucket.popleft()
    if len(bucket) >= ADMIN_LOGIN_RATE_LIMIT_MAX_ATTEMPTS:
        retry_after = max(1, int(ADMIN_LOGIN_RATE_LIMIT_WINDOW_SECS - (now - bucket[0])))
        raise HTTPException(
            status_code=429,
            detail=f"too many login attempts, retry in {retry_after}s",
            headers={"Retry-After": str(retry_after)},
        )
    bucket.append(now)
    if len(_ADMIN_LOGIN_ATTEMPTS) > 1024:
        for stale_ip in [k for k, v in _ADMIN_LOGIN_ATTEMPTS.items() if not v or v[-1] < cutoff]:
            _ADMIN_LOGIN_ATTEMPTS.pop(stale_ip, None)


def _reset_login_rate_limit(request: Request) -> None:
    _ADMIN_LOGIN_ATTEMPTS.pop(_client_ip(request), None)


def _request_uses_https(request: Request) -> bool:
    forwarded_proto = str(request.headers.get("x-forwarded-proto", "")).split(",", 1)[0].strip().lower()
    return forwarded_proto == "https" or request.url.scheme == "https"


def _is_admin_authenticated(request: Request, config: AppConfig) -> bool:
    cookie = request.cookies.get(ADMIN_COOKIE_NAME)
    if not cookie:
      return False
    return _verify_admin_cookie(cookie, config)


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
        admin_password = str(web_payload.get("admin_password") or "")
        if admin_password:
          web["admin_password"] = admin_password

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
          string_value = str(probe_payload.get(field_name) or "")
          if field_name in {"admin_password", "guest_password"}:
            if string_value:
              probe[field_name] = string_value
            continue
          probe[field_name] = string_value
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
        "admin_password_configured": bool(config.web.admin_password),
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
        "admin_password_configured": bool(config.probe.admin_password),
        "guest_password_configured": bool(config.probe.guest_password),
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


_STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML = (_STATIC_DIR / "index.html").read_text(encoding="utf-8")
ADMIN_HTML = (_STATIC_DIR / "admin.html").read_text(encoding="utf-8")
APP_CSS = (_STATIC_DIR / "app.css").read_text(encoding="utf-8")
APP_JS = (_STATIC_DIR / "app.js").read_text(encoding="utf-8")
# Cache-bust the extracted assets: without it a browser can pair a fresh
# index.html with a stale app.css/app.js after a deploy.
ASSET_VERSION = hashlib.sha256(f"{APP_CSS}{APP_JS}".encode("utf-8")).hexdigest()[:12]
INDEX_HTML = INDEX_HTML.replace("__ASSET_VERSION__", ASSET_VERSION)
# Markup plus behaviour, for tests that assert on the whole dashboard bundle.
DASHBOARD_BUNDLE = f"{INDEX_HTML}\n{APP_CSS}\n{APP_JS}"


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
      _check_login_rate_limit(request)
      current_config = active_config()
      if payload.username != _current_admin_username(current_config) or payload.password != current_config.web.admin_password:
        raise HTTPException(status_code=401, detail="invalid credentials")
      _reset_login_rate_limit(request)
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

    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    return app
