from __future__ import annotations

import argparse
import asyncio
import json
import logging

from .config import load_config
from .database import BotDatabase
from .identity import LocalIdentity
from .ingest_service import AdvertIngestService
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

    run_web = subparsers.add_parser("run-web", help="run status web service")
    run_web.add_argument("--config", default="config/config.toml", help="path to TOML config")

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
                "poll_interval_secs": config.probe.poll_interval_secs,
                "request_timeout_secs": config.probe.request_timeout_secs,
                "neighbours_page_size": config.probe.neighbours_page_size,
                "neighbours_prefix_len": config.probe.neighbours_prefix_len,
            },
            "web": {
                "host": config.web.host,
                "port": config.web.port,
            },
            "endpoints": [
                {
                    "name": endpoint.name,
                    "raw_host": endpoint.raw_host,
                    "raw_port": endpoint.raw_port,
                    "enabled": endpoint.enabled,
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

    if command == "run-ingest":
        asyncio.run(AdvertIngestService(config, database).run())
        return

    if command == "run-probe":
        asyncio.run(GuestProbeWorker(config, database).run())
        return

    if command == "run-web":
        database.initialize()
        app = create_app(database)
        uvicorn.run(app, host=config.web.host, port=config.web.port, log_level=config.service.log_level.lower())
        return


if __name__ == "__main__":
    main()
