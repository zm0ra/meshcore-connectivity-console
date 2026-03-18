from __future__ import annotations

import argparse
import asyncio
import json
import logging

from .bot_service import ChannelCommandBotService
from .bridge_gateway import BridgeGatewayService
from .config import load_config
from .database import BotDatabase
from .identity import LocalIdentity
from .ingest_service import AdvertIngestService
from .neighbours_worker import NeighboursWorkerApp
from .probe_service import GuestProbeWorker
from .web_service import create_app

import uvicorn


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )


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

    args = parser.parse_args()
    command = args.command or "init-db"
    config = load_config(args.config)
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
                    "enabled": endpoint.enabled,
                    "console_mirror_host": endpoint.console_mirror_host,
                    "console_mirror_port": endpoint.console_mirror_port,
                }
                for endpoint in config.endpoints
            ],
        }
        print(json.dumps(payload, indent=2, ensure_ascii=True))
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
        print(json.dumps(database.snapshot_overview(), indent=2, ensure_ascii=True))
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
