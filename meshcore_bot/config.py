from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tomllib


def _optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


@dataclass(frozen=True)
class BotConfig:
    name: str
    reply_prefix: str
    command_prefix: str
    message_history_size: int


@dataclass(frozen=True)
class ServiceConfig:
    name: str
    log_level: str


@dataclass(frozen=True)
class WebConfig:
    host: str
    port: int


@dataclass(frozen=True)
class StorageConfig:
    data_dir: Path
    logs_dir: Path
    database_path: Path


@dataclass(frozen=True)
class ChannelConfig:
    name: str
    psk: str | None
    listen: bool


@dataclass(frozen=True)
class EndpointConfig:
    name: str
    raw_host: str
    raw_port: int
    enabled: bool
    console_host: str | None
    console_port: int | None
    console_mirror_host: str | None
    console_mirror_port: int | None
    latitude: float | None
    longitude: float | None


@dataclass(frozen=True)
class ManagementTargetConfig:
    name: str
    endpoint_name: str
    target_hash_prefix: str | None
    target_identity_hex: str | None
    guest_password: str | None
    admin_password: str | None
    prefer_role: str
    enabled: bool
    notes: str | None


@dataclass(frozen=True)
class AppConfig:
    bot: BotConfig
    service: ServiceConfig
    web: WebConfig
    storage: StorageConfig
    channels: tuple[ChannelConfig, ...]
    endpoints: tuple[EndpointConfig, ...]
    management_targets: tuple[ManagementTargetConfig, ...]


def load_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path)
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    bot = raw.get("bot", {})
    service = raw.get("service", {})
    web = raw.get("web", {})
    storage = raw.get("storage", {})
    base_dir = path.parent.parent.resolve()

    channels = tuple(
        ChannelConfig(
            name=str(item["name"]).lower(),
            psk=_optional_str(item.get("psk")),
            listen=bool(item.get("listen", True)),
        )
        for item in raw.get("channels", [])
    )

    endpoints = tuple(
        EndpointConfig(
            name=str(item["name"]),
            raw_host=str(item["raw_host"]),
            raw_port=int(item.get("raw_port", 5002)),
            enabled=bool(item.get("enabled", True)),
            console_host=_optional_str(item.get("console_host")),
            console_port=int(item["console_port"]) if item.get("console_port") is not None else None,
            console_mirror_host=_optional_str(item.get("console_mirror_host")),
            console_mirror_port=int(item["console_mirror_port"]) if item.get("console_mirror_port") is not None else None,
            latitude=float(item["latitude"]) if item.get("latitude") is not None else None,
            longitude=float(item["longitude"]) if item.get("longitude") is not None else None,
        )
        for item in raw.get("endpoints", [])
    )

    management_targets = tuple(
        ManagementTargetConfig(
            name=str(item["name"]),
            endpoint_name=str(item["endpoint_name"]),
            target_hash_prefix=_optional_str(item.get("target_hash_prefix")),
            target_identity_hex=_optional_str(item.get("target_identity_hex")),
            guest_password=_optional_str(item.get("guest_password")),
            admin_password=_optional_str(item.get("admin_password")),
            prefer_role=str(item.get("prefer_role", "guest")).lower(),
            enabled=bool(item.get("enabled", True)),
            notes=_optional_str(item.get("notes")),
        )
        for item in raw.get("management_nodes", [])
    )

    return AppConfig(
        bot=BotConfig(
            name=str(bot.get("name", os.getenv("MESHCORE_BOT_NAME", "MeshBot"))),
            reply_prefix=str(bot.get("reply_prefix", "[MeshBot] ")),
            command_prefix=str(bot.get("command_prefix", "!")),
            message_history_size=int(bot.get("message_history_size", 200)),
        ),
        service=ServiceConfig(
            name=str(service.get("name", "meshcore-bot")),
            log_level=str(service.get("log_level", "INFO")),
        ),
        web=WebConfig(
            host=str(web.get("host", "0.0.0.0")),
            port=int(web.get("port", 8080)),
        ),
        storage=StorageConfig(
            data_dir=_resolve_path(base_dir, storage.get("data_dir", "./data")),
            logs_dir=_resolve_path(base_dir, storage.get("logs_dir", "./logs")),
            database_path=_resolve_path(base_dir, storage.get("database_path", "./data/meshcore-bot.db")),
        ),
        channels=channels,
        endpoints=endpoints,
        management_targets=management_targets,
    )


def _resolve_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()