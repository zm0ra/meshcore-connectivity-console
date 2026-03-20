from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .bot_service import ChannelCommandBotService
from .bridge_gateway import BridgeGatewayService
from .config import load_config, load_raw_config, save_raw_config
from .database import BotDatabase
from .identity import LocalIdentity
from .ingest_service import AdvertIngestService
from .neighbours_worker import NeighboursWorkerApp
from .probe_service import GuestProbeWorker, LocalConsoleEndpointResolver
from .web_service import create_app

import uvicorn


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )


def print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=True))


class DirectProbeConsoleReporter:
    def __init__(self, *, verbose: bool) -> None:
        self.verbose = verbose

    def print_start(
        self,
        *,
        repeater_id: int,
        name: object,
        endpoint_name: str,
        login: dict[str, object] | None,
        forced_login: tuple[str, str] | None = None,
        force_path_discovery: bool = False,
    ) -> None:
        print(f"Starting probe for RPT {repeater_id}: {name or '-'}")
        print(f"Endpoint: {endpoint_name}")
        if force_path_discovery:
            print("Route mode: force fresh discovery")
        if forced_login is not None:
            password = "empty" if forced_login[1] == "" else "provided"
            print(f"Preferred login: {forced_login[0]}/{password}")
        elif login is not None:
            role = login.get("learned_login_role") or "-"
            password = "learned" if login.get("learned_login_password") not in (None, "") else "empty"
            print(f"Preferred login: {role}/{password}")

    def __call__(self, event: str, payload: dict[str, object]) -> None:
        if event == "path_discovery_forced":
            print("- Ignoring remembered routes and adverts")
            return
        if event == "path_discovery_started":
            print("- Discovering route")
            return
        if event == "login_attempt_started":
            print(
                f"- Login attempt: role={payload['login_role']} route={payload['route']} password={payload['password_label']}"
            )
            return
        if event == "login_attempt_failed":
            print(
                f"  failed: role={payload['login_role']} route={payload['route']} error={payload['error']}"
            )
            return
        if event == "login_succeeded":
            print(
                f"- Login succeeded: role={payload['login_role']} permissions={payload['guest_permissions']} capability={payload['firmware_capability_level']}"
            )
            return
        if event == "neighbours_started":
            print("- Fetching neighbours")
            return
        if event == "neighbours_page_saved":
            print(
                f"  neighbours page: offset={payload['page_offset']} results={payload['results_count']} total={payload['total_neighbours_count']}"
            )
            return
        if event == "status_requested":
            print("- Fetching status")
            return
        if event == "status_received":
            print("  status received")
            return
        if event == "status_failed":
            print(f"  status skipped: {payload['error']}")
            return
        if event == "owner_requested":
            print("- Fetching owner info")
            return
        if event == "owner_received":
            print("  owner info received")
            return
        if event == "owner_failed":
            print(f"  owner info skipped: {payload['error']}")
            return


def resolve_endpoint(config, endpoint_name: str | None):
    if endpoint_name is None:
        selected_name = default_endpoint_name(config)
    else:
        selected_name = endpoint_name
    for endpoint in config.endpoints:
        if endpoint.name == selected_name and endpoint.enabled:
            return endpoint
    raise SystemExit(f"unknown or disabled endpoint: {selected_name}")


def default_endpoint_name(config) -> str:
    for endpoint in config.endpoints:
        if endpoint.enabled:
            return endpoint.name
    if config.endpoints:
        return config.endpoints[0].name
    raise SystemExit("no endpoints configured")


def resolve_probe_endpoint(config, database: BotDatabase, repeater_id: int, endpoint_name: str | None):
    if endpoint_name is not None:
        return resolve_endpoint(config, endpoint_name)

    preferred_endpoint = database.preferred_repeater_endpoint(repeater_id=repeater_id)
    if preferred_endpoint is not None:
        preferred_name = str(preferred_endpoint.get("preferred_endpoint_name") or "").strip()
        if preferred_name:
            for endpoint in config.endpoints:
                if endpoint.name == preferred_name and endpoint.enabled:
                    return endpoint

    for advert_row in (
        database.latest_repeater_advert_path(repeater_id=repeater_id),
        database.latest_repeater_advert(repeater_id=repeater_id),
    ):
        if advert_row is None:
            continue
        candidate_name = str(advert_row.get("endpoint_name") or "").strip()
        if not candidate_name:
            continue
        for endpoint in config.endpoints:
            if endpoint.name == candidate_name and endpoint.enabled:
                return endpoint

    return resolve_endpoint(config, None)


def resolve_local_console_probe_endpoint(config, repeater_name: str | None):
    resolver = LocalConsoleEndpointResolver(config)
    return asyncio.run(resolver.resolve_endpoint(repeater_name))


def resolve_repeater(database: BotDatabase, selector: str) -> dict[str, object]:
    normalized = selector.strip()
    if not normalized:
        raise SystemExit("repeater selector cannot be empty")

    repeaters = database.list_repeaters(limit=500)
    lowered = normalized.lower()

    if normalized.isdigit():
        for repeater in repeaters:
            if int(repeater["id"]) == int(normalized):
                return repeater

    for repeater in repeaters:
        if str(repeater["pubkey_hex"]).lower() == lowered:
            return repeater

    prefix_matches = [repeater for repeater in repeaters if str(repeater["pubkey_hex"]).lower().startswith(lowered)]
    if len(prefix_matches) == 1:
        return prefix_matches[0]

    exact_name_matches = [repeater for repeater in repeaters if str(repeater.get("name") or "").lower() == lowered]
    if len(exact_name_matches) == 1:
        return exact_name_matches[0]

    substring_matches = [repeater for repeater in repeaters if lowered in str(repeater.get("name") or "").lower()]
    if len(substring_matches) == 1:
        return substring_matches[0]

    combined_matches = prefix_matches or exact_name_matches or substring_matches
    if not combined_matches:
        raise SystemExit(f"repeater not found for selector: {selector}")

    raise SystemExit(
        "ambiguous repeater selector: "
        + selector
        + "\n"
        + json.dumps(
            [
                {
                    "id": int(repeater["id"]),
                    "name": repeater.get("name"),
                    "pubkey_hex": repeater.get("pubkey_hex"),
                }
                for repeater in combined_matches[:10]
            ],
            indent=2,
            ensure_ascii=True,
        )
    )


def build_repeater_payload(database: BotDatabase, repeater_id: int, *, adverts_limit: int, jobs_limit: int, probe_runs_limit: int, neighbours_limit: int) -> dict[str, object]:
    details = database.repeater_full_state(repeater_id=repeater_id)
    if details is None:
        raise SystemExit(f"repeater id not found: {repeater_id}")
    return {
        "repeater": details,
        "probe_state": database.repeater_probe_state(repeater_id=repeater_id),
        "latest_advert": database.latest_repeater_advert(repeater_id=repeater_id),
        "latest_zero_hop_advert": database.latest_repeater_zero_hop_advert(repeater_id=repeater_id),
        "latest_advert_path": database.latest_repeater_advert_path(repeater_id=repeater_id),
        "latest_saved_path": database.latest_repeater_path(repeater_id=repeater_id),
        "recent_adverts": database.recent_repeater_adverts(repeater_id=repeater_id, limit=adverts_limit),
        "recent_advert_paths": database.recent_repeater_advert_paths(repeater_id=repeater_id, limit=adverts_limit),
        "recent_probe_runs": database.repeater_recent_probe_runs(repeater_id=repeater_id, limit=probe_runs_limit),
        "probe_jobs": database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=jobs_limit),
        "latest_neighbours": database.latest_repeater_neighbours(repeater_id=repeater_id, limit=neighbours_limit),
    }


def build_endpoint_payload(config, database: BotDatabase, endpoint_name: str, *, limit: int, seen_within_hours: float | None) -> dict[str, object]:
    endpoint = resolve_endpoint(config, endpoint_name)
    repeaters = database.list_repeaters_seen_on_endpoint(
        endpoint_name=endpoint.name,
        limit=limit,
        seen_within_hours=seen_within_hours,
    )
    return {
        "endpoint": {
            "name": endpoint.name,
            "raw_host": endpoint.raw_host,
            "raw_port": endpoint.raw_port,
            "enabled": endpoint.enabled,
            "console_mirror_host": endpoint.console_mirror_host or endpoint.raw_host,
            "console_mirror_port": endpoint.console_mirror_port,
        },
        "seen_within_hours": seen_within_hours,
        "count": len(repeaters),
        "repeaters": repeaters,
    }


def _configured_endpoints(raw_config: dict[str, object]) -> list[dict[str, object]]:
    endpoints = raw_config.setdefault("endpoints", [])
    if not isinstance(endpoints, list) or not all(isinstance(item, dict) for item in endpoints):
        raise SystemExit("config endpoints must be an array of tables")
    return endpoints


def _normalize_endpoint_name(name: str) -> str:
    normalized = name.strip()
    if not normalized:
        raise SystemExit("endpoint name cannot be empty")
    return normalized


def _find_config_endpoint(endpoints: list[dict[str, object]], name: str) -> dict[str, object] | None:
    lowered = name.strip().lower()
    for endpoint in endpoints:
        if str(endpoint.get("name") or "").strip().lower() == lowered:
            return endpoint
    return None


def _endpoint_public_payload(endpoint: dict[str, object]) -> dict[str, object]:
    return {
        "name": str(endpoint.get("name") or ""),
        "raw_host": str(endpoint.get("raw_host") or ""),
        "raw_port": int(endpoint.get("raw_port", 5002)),
        "console_port": int(endpoint.get("console_port", 5001)) if endpoint.get("console_port", 5001) is not None else None,
        "local_node_name": endpoint.get("local_node_name"),
        "enabled": bool(endpoint.get("enabled", True)),
        "console_mirror_host": endpoint.get("console_mirror_host"),
        "console_mirror_port": endpoint.get("console_mirror_port"),
    }


def _load_endpoint_config_entries(config_path: str | Path) -> tuple[Path, dict[str, object], list[dict[str, object]]]:
    path, raw_config = load_raw_config(config_path)
    endpoints = _configured_endpoints(raw_config)
    return path, raw_config, endpoints


def main() -> None:
    parser = argparse.ArgumentParser(description="MeshCore TCP bot foundation")
    subparsers = parser.add_subparsers(dest="command")

    init_db = subparsers.add_parser("init-db", help="initialize SQLite schema")
    init_db.add_argument("--config", default="config/config.toml", help="path to TOML config")

    show_config = subparsers.add_parser("show-config", help="print resolved config")
    show_config.add_argument("--config", default="config/config.toml", help="path to TOML config")

    ensure_identity = subparsers.add_parser("ensure-identity", help="create or load local MeshCore identity")
    ensure_identity.add_argument("--config", default="config/config.toml", help="path to TOML config")

    run_ingest = subparsers.add_parser("run-ingest", help="run advert ingest worker")
    run_ingest.add_argument("--config", default="config/config.toml", help="path to TOML config")

    run_probe = subparsers.add_parser("run-probe", help="run guest probe worker")
    run_probe.add_argument("--config", default="config/config.toml", help="path to TOML config")

    run_bridge_gateway = subparsers.add_parser("run-bridge-gateway", help="run bridge gateway process")
    run_bridge_gateway.add_argument("--config", default="config/config.toml", help="path to TOML config")

    run_neighbours_worker = subparsers.add_parser("run-neighbours-worker", help="run neighbours ingest and probe worker")
    run_neighbours_worker.add_argument("--config", default="config/config.toml", help="path to TOML config")

    run_bot_worker = subparsers.add_parser("run-bot-worker", help="run hashtag command bot worker")
    run_bot_worker.add_argument("--config", default="config/config.toml", help="path to TOML config")

    run_web = subparsers.add_parser("run-web", help="run status web service")
    run_web.add_argument("--config", default="config/config.toml", help="path to TOML config")

    cleanup_probe_jobs = subparsers.add_parser("cleanup-probe-jobs", help="delete old failed probe jobs")
    cleanup_probe_jobs.add_argument("--config", default="config/config.toml", help="path to TOML config")
    cleanup_probe_jobs.add_argument(
        "--failed-older-than-hours",
        type=float,
        default=12.0,
        help="delete only failed jobs older than this many hours",
    )
    cleanup_probe_jobs.add_argument("--dry-run", action="store_true", help="report how many rows would be deleted")

    rpt_list = subparsers.add_parser("rpt-list", help="list known repeaters")
    rpt_list.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_list.add_argument("--query", default=None, help="filter by id, name, or pubkey hex")
    rpt_list.add_argument("--limit", type=int, default=100, help="maximum number of repeaters to return")

    rpt_show = subparsers.add_parser("rpt-show", help="show full repeater state")
    rpt_show.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_show.add_argument("selector", help="repeater id, pubkey hex/prefix, or name")
    rpt_show.add_argument("--adverts-limit", type=int, default=8)
    rpt_show.add_argument("--jobs-limit", type=int, default=10)
    rpt_show.add_argument("--probe-runs-limit", type=int, default=10)
    rpt_show.add_argument("--neighbours-limit", type=int, default=32)

    endpoint_show = subparsers.add_parser("endpoint-show", help="show repeaters recently seen on one endpoint")
    endpoint_show.add_argument("--config", default="config/config.toml", help="path to TOML config")
    endpoint_show.add_argument("endpoint", help="endpoint name")
    endpoint_show.add_argument("--limit", type=int, default=100, help="maximum number of repeaters to return")
    endpoint_show.add_argument(
        "--seen-within-hours",
        type=float,
        default=24.0,
        help="only include repeaters seen on this endpoint within this many hours; set 0 to disable time filter",
    )

    endpoint_list = subparsers.add_parser("endpoint-list", help="list configured transport endpoints")
    endpoint_list.add_argument("--config", default="config/config.toml", help="path to TOML config")

    endpoint_add = subparsers.add_parser("endpoint-add", help="add endpoint to TOML config")
    endpoint_add.add_argument("--config", default="config/config.toml", help="path to TOML config")
    endpoint_add.add_argument("--name", required=True, help="endpoint name")
    endpoint_add.add_argument("--raw-host", required=True, help="RS232@TCP host")
    endpoint_add.add_argument("--raw-port", type=int, default=5002, help="RS232@TCP port")
    endpoint_add.add_argument("--console-port", type=int, default=5001, help="clean CLI console port for direct local-node harvest")
    endpoint_add.add_argument("--local-node-name", default=None, help="optional local repeater name exposed on this endpoint")
    endpoint_add.add_argument("--console-mirror-host", default=None, help="optional console mirror host")
    endpoint_add.add_argument("--console-mirror-port", type=int, default=None, help="optional console mirror port")
    endpoint_add.add_argument("--disabled", action="store_true", help="create endpoint as disabled")

    endpoint_update = subparsers.add_parser("endpoint-update", help="update endpoint in TOML config")
    endpoint_update.add_argument("--config", default="config/config.toml", help="path to TOML config")
    endpoint_update.add_argument("endpoint", help="current endpoint name")
    endpoint_update.add_argument("--name", default=None, help="new endpoint name")
    endpoint_update.add_argument("--raw-host", default=None, help="new RS232@TCP host")
    endpoint_update.add_argument("--raw-port", type=int, default=None, help="new RS232@TCP port")
    endpoint_update.add_argument("--console-port", type=int, default=None, help="set clean CLI console port")
    endpoint_update.add_argument("--clear-console-port", action="store_true", help="remove clean CLI console port")
    endpoint_update.add_argument("--local-node-name", default=None, help="set local repeater name exposed on this endpoint")
    endpoint_update.add_argument("--clear-local-node-name", action="store_true", help="remove local repeater name mapping")
    endpoint_update.add_argument("--console-mirror-host", default=None, help="set console mirror host")
    endpoint_update.add_argument("--console-mirror-port", type=int, default=None, help="set console mirror port")
    endpoint_update.add_argument("--clear-console-mirror-host", action="store_true", help="remove console mirror host")
    endpoint_update.add_argument("--clear-console-mirror-port", action="store_true", help="remove console mirror port")
    endpoint_update.add_argument("--enabled", action="store_true", help="mark endpoint enabled")
    endpoint_update.add_argument("--disabled", action="store_true", help="mark endpoint disabled")

    endpoint_delete = subparsers.add_parser("endpoint-delete", help="delete endpoint from TOML config")
    endpoint_delete.add_argument("--config", default="config/config.toml", help="path to TOML config")
    endpoint_delete.add_argument("endpoint", help="endpoint name")
    endpoint_delete.add_argument("--yes", action="store_true", help="confirm endpoint deletion")

    rpt_probe = subparsers.add_parser("rpt-probe", help="enqueue manual probe for repeater")
    rpt_probe.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_probe.add_argument("selector", help="repeater id, pubkey hex/prefix, or name")
    rpt_probe.add_argument("--endpoint", default=None, help="endpoint name, defaults to first enabled endpoint")
    rpt_probe.add_argument("--reason", default="manual cli probe", help="probe job reason")
    rpt_probe.add_argument("--schedule-after-secs", type=float, default=0.0, help="delay before job becomes claimable")
    rpt_probe.add_argument("--role", choices=["guest", "admin"], default=None, help="remember this login role before probe")
    rpt_probe.add_argument("--password", default=None, help="remember this login password before probe")
    rpt_probe.add_argument("--clear-learned-login", action="store_true", help="clear learned login before probe")

    rpt_probe_now = subparsers.add_parser("rpt-probe-now", help="run manual probe immediately and stream progress")
    rpt_probe_now.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_probe_now.add_argument("selector", help="repeater id, pubkey hex/prefix, or name")
    rpt_probe_now.add_argument("--endpoint", default=None, help="endpoint name, defaults to first enabled endpoint")
    rpt_probe_now.add_argument("--role", choices=["guest", "admin"], default=None, help="remember this login role before probe")
    rpt_probe_now.add_argument("--password", default=None, help="remember this login password before probe")
    rpt_probe_now.add_argument("--clear-learned-login", action="store_true", help="clear learned login before probe")
    rpt_probe_now.add_argument("--force-path-discovery", action="store_true", help="ignore remembered routes and discover a fresh path")
    rpt_probe_now.add_argument("--verbose", action="store_true", help="show raw packet and debug logs")

    rpt_login_set = subparsers.add_parser("rpt-login-set", help="store learned login override for repeater")
    rpt_login_set.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_login_set.add_argument("selector", help="repeater id, pubkey hex/prefix, or name")
    rpt_login_set.add_argument("--role", required=True, choices=["guest", "admin"])
    rpt_login_set.add_argument("--password", required=True)

    rpt_login_clear = subparsers.add_parser("rpt-login-clear", help="clear learned login override for repeater")
    rpt_login_clear.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_login_clear.add_argument("selector", help="repeater id, pubkey hex/prefix, or name")

    rpt_update = subparsers.add_parser("rpt-update", help="update repeater metadata")
    rpt_update.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_update.add_argument("selector", help="repeater id, pubkey hex/prefix, or name")
    rpt_update.add_argument("--name", default=None, help="override stored repeater name")
    rpt_update.add_argument("--lat", type=float, default=None, help="override latitude")
    rpt_update.add_argument("--lon", type=float, default=None, help="override longitude")

    rpt_add = subparsers.add_parser("rpt-add", help="create or refresh manual repeater row")
    rpt_add.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_add.add_argument("--pubkey", required=True, help="full repeater public key hex")
    rpt_add.add_argument("--name", default=None, help="display name")
    rpt_add.add_argument("--endpoint", default="manual", help="synthetic endpoint label for manual row")
    rpt_add.add_argument("--lat", type=float, default=None, help="initial latitude")
    rpt_add.add_argument("--lon", type=float, default=None, help="initial longitude")

    rpt_delete = subparsers.add_parser("rpt-delete", help="delete repeater and all related history")
    rpt_delete.add_argument("--config", default="config/config.toml", help="path to TOML config")
    rpt_delete.add_argument("selector", help="repeater id, pubkey hex/prefix, or name")
    rpt_delete.add_argument("--yes", action="store_true", help="confirm destructive delete")

    args = parser.parse_args()
    command = args.command
    if command is None:
        parser.print_help()
        return

    config = load_config(getattr(args, "config", "config/config.toml"))
    configure_logging(config.service.log_level)

    if command == "show-config":
        payload = {
            "service": {
                "name": config.service.name,
                "log_level": config.service.log_level,
            },
            "storage": {
                "database_path": str(config.storage.database_path),
            },
            "identity": {
                "key_file_path": str(config.identity.key_file_path),
            },
            "probe": {
                "key_file_path": str(config.probe.key_file_path) if config.probe.key_file_path else None,
                "admin_password_configured": bool(config.probe.admin_password),
                "admin_password_name_prefixes": list(config.probe.admin_password_name_prefixes),
                "admin_password_pubkey_prefixes": list(config.probe.admin_password_pubkey_prefixes),
                "guest_password_configured": bool(config.probe.guest_password),
                "default_guest_password_configured": bool(config.probe.default_guest_password),
                "guest_password_name_prefixes": list(config.probe.guest_password_name_prefixes),
                "guest_password_pubkey_prefixes": list(config.probe.guest_password_pubkey_prefixes),
                "pre_login_advert_name": config.probe.pre_login_advert_name,
                "pre_login_advert_delay_secs": config.probe.pre_login_advert_delay_secs,
                "advert_reprobe_success_cooldown_secs": config.probe.advert_reprobe_success_cooldown_secs,
                "advert_reprobe_failure_cooldown_secs": config.probe.advert_reprobe_failure_cooldown_secs,
                "advert_probe_min_interval_secs": config.probe.advert_probe_min_interval_secs,
                "advert_path_change_cooldown_secs": config.probe.advert_path_change_cooldown_secs,
                "automatic_probe_max_per_day": config.probe.automatic_probe_max_per_day,
                "scheduled_reprobe_interval_secs": config.probe.scheduled_reprobe_interval_secs,
                "night_failed_retry_start_hour": config.probe.night_failed_retry_start_hour,
                "night_failed_retry_end_hour": config.probe.night_failed_retry_end_hour,
                "night_failed_retry_interval_secs": config.probe.night_failed_retry_interval_secs,
                "poll_interval_secs": config.probe.poll_interval_secs,
                "request_timeout_secs": config.probe.request_timeout_secs,
                "neighbours_page_size": config.probe.neighbours_page_size,
                "neighbours_prefix_len": config.probe.neighbours_prefix_len,
            },
            "bot": {
                "enabled": config.bot.enabled,
                "sender_name": config.bot.sender_name,
                "channels": list(config.bot.channels),
                "enabled_commands": list(config.bot.enabled_commands),
                "min_response_delay_secs": config.bot.min_response_delay_secs,
                "response_attempts": config.bot.response_attempts,
                "response_attempts_max": config.bot.response_attempts_max,
                "echo_ack_timeout_secs": config.bot.echo_ack_timeout_secs,
                "response_retry_delay_secs": config.bot.response_retry_delay_secs,
                "response_retry_backoff_multiplier": config.bot.response_retry_backoff_multiplier,
                "response_retry_max_delay_secs": config.bot.response_retry_max_delay_secs,
                "quiet_window_secs": config.bot.quiet_window_secs,
                "command_dedup_ttl_secs": config.bot.command_dedup_ttl_secs,
                "include_test_signal": config.bot.include_test_signal,
            },
            "web": {
                "host": config.web.host,
                "port": config.web.port,
            },
            "gateway": {
                "control_socket_path": str(config.gateway.control_socket_path),
                "event_socket_path": str(config.gateway.event_socket_path),
                "traffic_watchdog_secs": config.gateway.traffic_watchdog_secs,
                "close_timeout_secs": config.gateway.close_timeout_secs,
                "console_probe_timeout_secs": config.gateway.console_probe_timeout_secs,
            },
            "endpoints": [
                {
                    "name": endpoint.name,
                    "raw_host": endpoint.raw_host,
                    "raw_port": endpoint.raw_port,
                    "console_port": endpoint.console_port,
                    "local_node_name": endpoint.local_node_name,
                    "enabled": endpoint.enabled,
                    "console_mirror_host": endpoint.console_mirror_host,
                    "console_mirror_port": endpoint.console_mirror_port,
                }
                for endpoint in config.endpoints
            ],
        }
        print_json(payload)
        return

    if command == "endpoint-list":
        config_path, raw_config, endpoints = _load_endpoint_config_entries(args.config)
        print_json(
            {
                "config_path": str(config_path),
                "count": len(endpoints),
                "endpoints": [_endpoint_public_payload(endpoint) for endpoint in endpoints],
            }
        )
        return

    if command == "endpoint-add":
        config_path, raw_config, endpoints = _load_endpoint_config_entries(args.config)
        name = _normalize_endpoint_name(str(args.name))
        if _find_config_endpoint(endpoints, name) is not None:
            raise SystemExit(f"endpoint already exists: {name}")
        endpoint = {
            "name": name,
            "raw_host": str(args.raw_host),
            "raw_port": int(args.raw_port),
            "console_port": int(args.console_port) if args.console_port is not None else None,
            "enabled": not bool(args.disabled),
        }
        if args.local_node_name is not None:
            endpoint["local_node_name"] = str(args.local_node_name)
        if args.console_mirror_host is not None:
            endpoint["console_mirror_host"] = str(args.console_mirror_host)
        if args.console_mirror_port is not None:
            endpoint["console_mirror_port"] = int(args.console_mirror_port)
        endpoints.append(endpoint)
        save_raw_config(config_path, raw_config)
        print_json({"config_path": str(config_path), "endpoint": _endpoint_public_payload(endpoint)})
        return

    if command == "endpoint-update":
        config_path, raw_config, endpoints = _load_endpoint_config_entries(args.config)
        endpoint = _find_config_endpoint(endpoints, str(args.endpoint))
        if endpoint is None:
            raise SystemExit(f"endpoint not found: {args.endpoint}")
        if args.name is not None:
            new_name = _normalize_endpoint_name(str(args.name))
            existing = _find_config_endpoint(endpoints, new_name)
            if existing is not None and existing is not endpoint:
                raise SystemExit(f"endpoint already exists: {new_name}")
            endpoint["name"] = new_name
        if args.raw_host is not None:
            endpoint["raw_host"] = str(args.raw_host)
        if args.raw_port is not None:
            endpoint["raw_port"] = int(args.raw_port)
        if args.clear_console_port:
            endpoint.pop("console_port", None)
        elif args.console_port is not None:
            endpoint["console_port"] = int(args.console_port)
        if args.clear_local_node_name:
            endpoint.pop("local_node_name", None)
        elif args.local_node_name is not None:
            endpoint["local_node_name"] = str(args.local_node_name)
        if args.enabled and args.disabled:
            raise SystemExit("--enabled and --disabled are mutually exclusive")
        if args.enabled:
            endpoint["enabled"] = True
        elif args.disabled:
            endpoint["enabled"] = False
        if args.clear_console_mirror_host:
            endpoint.pop("console_mirror_host", None)
        elif args.console_mirror_host is not None:
            endpoint["console_mirror_host"] = str(args.console_mirror_host)
        if args.clear_console_mirror_port:
            endpoint.pop("console_mirror_port", None)
        elif args.console_mirror_port is not None:
            endpoint["console_mirror_port"] = int(args.console_mirror_port)
        save_raw_config(config_path, raw_config)
        print_json({"config_path": str(config_path), "endpoint": _endpoint_public_payload(endpoint)})
        return

    if command == "endpoint-delete":
        if not args.yes:
            raise SystemExit("endpoint-delete requires --yes")
        config_path, raw_config, endpoints = _load_endpoint_config_entries(args.config)
        endpoint = _find_config_endpoint(endpoints, str(args.endpoint))
        if endpoint is None:
            raise SystemExit(f"endpoint not found: {args.endpoint}")
        deleted_payload = _endpoint_public_payload(endpoint)
        endpoints.remove(endpoint)
        save_raw_config(config_path, raw_config)
        print_json({"config_path": str(config_path), "deleted": deleted_payload})
        return

    if command == "ensure-identity":
        identity = LocalIdentity.load_or_create(config.identity.key_file_path)
        print(
            json.dumps(
                {
                    "key_file_path": str(config.identity.key_file_path),
                    "public_key_hex": identity.public_key.hex().upper(),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return

    database = BotDatabase(config.storage.database_path)
    if command == "init-db":
        database.initialize()
        print_json(database.snapshot_overview())
        return

    if command == "cleanup-probe-jobs":
        database.initialize()
        older_than_secs = float(args.failed_older_than_hours) * 3600.0
        deleted_count = database.delete_failed_probe_jobs_older_than(
            older_than_secs=older_than_secs,
            dry_run=bool(args.dry_run),
        )
        print(
            json.dumps(
                {
                    "dry_run": bool(args.dry_run),
                    "failed_older_than_hours": float(args.failed_older_than_hours),
                    "matched_failed_jobs": deleted_count,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return

    if command == "rpt-list":
        database.initialize()
        print_json(
            {
                "count": len(database.list_repeaters(query=args.query, limit=args.limit)),
                "repeaters": database.list_repeaters(query=args.query, limit=args.limit),
            }
        )
        return

    if command == "rpt-show":
        database.initialize()
        repeater = resolve_repeater(database, args.selector)
        print_json(
            build_repeater_payload(
                database,
                int(repeater["id"]),
                adverts_limit=args.adverts_limit,
                jobs_limit=args.jobs_limit,
                probe_runs_limit=args.probe_runs_limit,
                neighbours_limit=args.neighbours_limit,
            )
        )
        return

    if command == "endpoint-show":
        database.initialize()
        seen_within_hours = None if float(args.seen_within_hours) <= 0 else float(args.seen_within_hours)
        print_json(
            build_endpoint_payload(
                config,
                database,
                str(args.endpoint),
                limit=int(args.limit),
                seen_within_hours=seen_within_hours,
            )
        )
        return

    if command == "rpt-probe-now":
        database.initialize()
        repeater = resolve_repeater(database, args.selector)
        repeater_id = int(repeater["id"])
        local_console_endpoint = resolve_local_console_probe_endpoint(config, str(repeater.get("name") or ""))
        endpoint = local_console_endpoint or resolve_probe_endpoint(config, database, repeater_id, args.endpoint)
        reporter = DirectProbeConsoleReporter(verbose=bool(args.verbose))
        if not args.verbose:
            logging.getLogger("meshcore-bot.tcp_client").setLevel(logging.ERROR)
            logging.getLogger(f"{config.service.name}.probe").setLevel(logging.ERROR)
        forced_login = None
        if args.clear_learned_login:
            database.reset_repeater_login_if_stable(repeater_id=repeater_id, min_success_count=0)
        if args.role is not None or args.password is not None:
            if not args.role or args.password is None:
                raise SystemExit("--role and --password must be provided together")
            forced_login = (str(args.role), str(args.password))
            database.remember_repeater_login(
                repeater_id=repeater_id,
                login_role=str(args.role),
                login_password=str(args.password),
            )

        worker = GuestProbeWorker(config, database, progress_callback=reporter if not args.verbose else None)
        probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name=endpoint.name)
        if args.verbose:
            print_json(
                {
                    "mode": "direct",
                    "action": "starting probe",
                    "repeater_id": repeater_id,
                    "name": repeater.get("name"),
                    "pubkey_hex": repeater.get("pubkey_hex"),
                    "endpoint_name": endpoint.name,
                    "probe_run_id": probe_run_id,
                    "learned_login": database.preferred_repeater_login(repeater_id=repeater_id),
                }
            )
        else:
            reporter.print_start(
                repeater_id=repeater_id,
                name=repeater.get("name"),
                endpoint_name=endpoint.name,
                login=database.preferred_repeater_login(repeater_id=repeater_id),
                forced_login=forced_login,
                force_path_discovery=bool(args.force_path_discovery),
            )
        try:
            if local_console_endpoint is not None and local_console_endpoint.name == endpoint.name:
                asyncio.run(
                    worker.probe_repeater_via_console(
                        probe_run_id=probe_run_id,
                        repeater_id=repeater_id,
                        endpoint=endpoint,
                        repeater_name=str(repeater.get("name") or "") or None,
                    )
                )
            else:
                asyncio.run(
                    worker.probe_repeater_as_guest(
                        probe_run_id=probe_run_id,
                        repeater_id=repeater_id,
                        endpoint=endpoint,
                        remote_pubkey=bytes.fromhex(str(repeater["pubkey_hex"])),
                        repeater_name=str(repeater.get("name") or "") or None,
                        forced_login=forced_login,
                        allow_default_guest_fallback=forced_login is None,
                        force_path_discovery=bool(args.force_path_discovery),
                    )
                )
        except Exception as exc:
            database.complete_probe_run(
                probe_run_id,
                repeater_id=repeater_id,
                result="failed",
                guest_login_ok=False,
                guest_permissions=None,
                firmware_capability_level=None,
                login_server_time=None,
                error_message=str(exc),
            )
            if args.verbose:
                print_json(
                    {
                        "mode": "direct",
                        "action": "probe failed",
                        "repeater_id": repeater_id,
                        "probe_run_id": probe_run_id,
                        "error": str(exc),
                        "repeater": database.repeater_full_state(repeater_id=repeater_id),
                        "recent_probe_runs": database.repeater_recent_probe_runs(repeater_id=repeater_id, limit=3),
                    }
                )
            else:
                print(f"Probe failed: {exc}")
                repeater_state = database.repeater_full_state(repeater_id=repeater_id)
                print(f"Last probe status: {repeater_state['last_probe_status'] if repeater_state else 'failed'}")
            raise SystemExit(1) from exc

        if args.verbose:
            print_json(
                {
                    "mode": "direct",
                    "action": "probe completed",
                    "repeater_id": repeater_id,
                    "probe_run_id": probe_run_id,
                    "repeater": database.repeater_full_state(repeater_id=repeater_id),
                    "recent_probe_runs": database.repeater_recent_probe_runs(repeater_id=repeater_id, limit=3),
                    "latest_neighbours": database.latest_repeater_neighbours(repeater_id=repeater_id, limit=16),
                }
            )
        else:
            repeater_state = database.repeater_full_state(repeater_id=repeater_id)
            neighbours = database.latest_repeater_neighbours(repeater_id=repeater_id, limit=16)
            print("Probe completed successfully")
            if repeater_state is not None:
                print(f"Last probe status: {repeater_state['last_probe_status']}")
                print(f"Learned login: {repeater_state['learned_login_role'] or '-'}")
            print(f"Neighbours collected: {len(neighbours)}")
        return

    if command == "rpt-login-set":
        database.initialize()
        repeater = resolve_repeater(database, args.selector)
        database.remember_repeater_login(
            repeater_id=int(repeater["id"]),
            login_role=str(args.role),
            login_password=str(args.password),
        )
        print_json(
            {
                "updated": True,
                "repeater": database.repeater_full_state(repeater_id=int(repeater["id"])),
            }
        )
        return

    if command == "rpt-login-clear":
        database.initialize()
        repeater = resolve_repeater(database, args.selector)
        cleared = database.reset_repeater_login_if_stable(repeater_id=int(repeater["id"]), min_success_count=0)
        print_json(
            {
                "updated": bool(cleared),
                "repeater": database.repeater_full_state(repeater_id=int(repeater["id"])),
            }
        )
        return

    if command == "rpt-update":
        database.initialize()
        repeater = resolve_repeater(database, args.selector)
        if args.name is None and args.lat is None and args.lon is None:
            raise SystemExit("provide at least one of --name, --lat, or --lon")
        updated = database.update_repeater_metadata(
            repeater_id=int(repeater["id"]),
            name=args.name,
            latitude=args.lat,
            longitude=args.lon,
        )
        print_json({"updated": updated})
        return

    if command == "rpt-add":
        database.initialize()
        repeater_id = database.create_manual_repeater(
            pubkey_hex=args.pubkey,
            name=args.name,
            endpoint_name=args.endpoint,
            latitude=args.lat,
            longitude=args.lon,
        )
        print_json(
            {
                "repeater_id": repeater_id,
                "repeater": database.repeater_full_state(repeater_id=repeater_id),
            }
        )
        return

    if command == "rpt-delete":
        database.initialize()
        if not args.yes:
            raise SystemExit("rpt-delete requires --yes")
        repeater = resolve_repeater(database, args.selector)
        deleted = database.delete_repeater(repeater_id=int(repeater["id"]))
        print_json({"deleted": deleted, "repeater_id": int(repeater["id"])})
        return

    if command == "rpt-probe":
        database.initialize()
        repeater = resolve_repeater(database, args.selector)
        repeater_id = int(repeater["id"])
        if args.clear_learned_login:
            database.reset_repeater_login_if_stable(repeater_id=repeater_id, min_success_count=0)
        if args.role is not None or args.password is not None:
            if not args.role or args.password is None:
                raise SystemExit("--role and --password must be provided together")
            database.remember_repeater_login(
                repeater_id=repeater_id,
                login_role=str(args.role),
                login_password=str(args.password),
            )
        scheduled_at = None
        if float(args.schedule_after_secs) > 0:
            scheduled_at = (datetime.now(tz=UTC) + timedelta(seconds=float(args.schedule_after_secs))).isoformat()
        local_console_endpoint = resolve_local_console_probe_endpoint(config, str(repeater.get("name") or ""))
        endpoint_name = (local_console_endpoint or resolve_probe_endpoint(config, database, repeater_id, args.endpoint)).name
        job_id = database.enqueue_probe_job(
            repeater_id=repeater_id,
            endpoint_name=endpoint_name,
            reason=str(args.reason),
            scheduled_at=scheduled_at,
        )
        print_json(
            {
                "repeater_id": repeater_id,
                "endpoint_name": endpoint_name,
                "job_id": job_id,
                "scheduled_at": scheduled_at,
                "learned_login": database.preferred_repeater_login(repeater_id=repeater_id),
                "probe_jobs": database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=5),
            }
        )
        return

    if command == "run-ingest":
        asyncio.run(AdvertIngestService(config, database).run())
        return

    if command == "run-probe":
        asyncio.run(GuestProbeWorker(config, database).run())
        return

    if command == "run-bridge-gateway":
        asyncio.run(BridgeGatewayService(config).run())
        return

    if command == "run-neighbours-worker":
        asyncio.run(NeighboursWorkerApp(config, database).run())
        return

    if command == "run-bot-worker":
        asyncio.run(ChannelCommandBotService(config, database).run())
        return

    if command == "run-web":
        database.initialize()
        app = create_app(database)
        uvicorn.run(app, host=config.web.host, port=config.web.port, log_level=config.service.log_level.lower())
        return


if __name__ == "__main__":
    main()
