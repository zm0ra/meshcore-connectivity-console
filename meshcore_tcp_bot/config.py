"""Configuration loading for the bot service."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import tomllib


def _optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _password_str(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


@dataclass(slots=True)
class ChannelConfig:
    name: str
    psk: str | None = None
    listen: bool = True


@dataclass(slots=True)
class EndpointConfig:
    name: str
    raw_host: str
    raw_port: int = 5002
    enabled: bool = True
    console_host: str | None = None
    console_port: int | None = None
    console_mirror_host: str | None = None
    console_mirror_port: int | None = None
    latitude: float | None = None
    longitude: float | None = None


@dataclass(slots=True)
class BotConfig:
    name: str = "MeshBot"
    reply_prefix: str = "[MeshBot] "
    command_prefix: str = "!"
    listen_channels: tuple[str, ...] = ("bot-test",)
    message_history_size: int = 200
    self_advert_enabled: bool = True
    self_advert_interval_seconds: int = 600


@dataclass(slots=True)
class IdentityConfig:
    file_path: str = "./data/bot-identity.json"


@dataclass(slots=True)
class StorageConfig:
    database_path: str = "./data/meshcore-bot.db"


@dataclass(slots=True)
class ManagementNodeConfig:
    name: str
    endpoint_name: str
    target_hash_prefix: str | None = None
    target_identity_hex: str | None = None
    guest_password: str | None = None
    admin_password: str | None = None
    prefer_role: str = "guest"
    enabled: bool = True
    notes: str | None = None


@dataclass(slots=True)
class WebConfig:
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(slots=True)
class AdminConfig:
    password: str | None = None
    session_secret: str | None = None


@dataclass(slots=True)
class ManagementRuntimeConfig:
    enabled: bool = True
    auto_discover_from_adverts: bool = True
    auto_guest_password: str | None = None
    auto_admin_password: str | None = None
    temporary_admin_password: str | None = None
    temporary_admin_name_prefixes: tuple[str, ...] = ()
    poll_interval_seconds: int = 15
    request_timeout_seconds: int = 20
    login_interval_seconds: int = 300
    retry_after_failed_poll_seconds: int = 180
    owner_poll_interval_seconds: int = 900
    acl_poll_interval_seconds: int = 900
    neighbors_poll_interval_seconds: int = 120
    console_neighbors_poll_interval_seconds: int = 120
    console_command_timeout_seconds: int = 5
    neighbors_request_count: int = 24
    neighbors_prefix_length: int = 6


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"


@dataclass(slots=True)
class AppConfig:
    bot: BotConfig
    identity: IdentityConfig
    storage: StorageConfig
    channels: tuple[ChannelConfig, ...]
    endpoints: tuple[EndpointConfig, ...]
    management_nodes: tuple[ManagementNodeConfig, ...]
    management: ManagementRuntimeConfig
    web: WebConfig
    admin: AdminConfig
    logging: LoggingConfig
    source_path: Path


def load_config(path: str | Path) -> AppConfig:
    source_path = Path(path).expanduser().resolve()
    with source_path.open("rb") as handle:
        raw = tomllib.load(handle)

    bot_raw = raw.get("bot", {})
    web_raw = raw.get("web", {})
    logging_raw = raw.get("logging", {})
    storage_raw = raw.get("storage", {})
    identity_raw = raw.get("identity", {})
    management_raw = raw.get("management", {})

    bot = BotConfig(
        name=str(bot_raw.get("name", "MeshBot")),
        reply_prefix=str(bot_raw.get("reply_prefix", "[MeshBot] ")),
        command_prefix=str(bot_raw.get("command_prefix", "!")),
        listen_channels=tuple(str(item).lower() for item in bot_raw.get("listen_channels", ["bot-test"])),
        message_history_size=int(bot_raw.get("message_history_size", 200)),
        self_advert_enabled=bool(bot_raw.get("self_advert_enabled", True)),
        self_advert_interval_seconds=int(bot_raw.get("self_advert_interval_seconds", 600)),
    )

    channels = tuple(
        ChannelConfig(
            name=str(item["name"]).lower(),
            psk=_optional_str(item.get("psk")),
            listen=str(item["name"]).lower() in {str(channel).lower() for channel in bot.listen_channels},
        )
        for item in raw.get("channels", [])
    )
    if not channels:
        raise ValueError("At least one channel must be configured")

    endpoints = tuple(
        EndpointConfig(
            name=str(item["name"]),
            raw_host=str(item["raw_host"]),
            raw_port=int(item.get("raw_port", 5002)),
            enabled=bool(item.get("enabled", True)),
            console_host=item.get("console_host") or item.get("console_mirror_host"),
            console_port=(
                int(item["console_port"])
                if item.get("console_port") is not None
                else (5001 if item.get("console_mirror_port") is not None else None)
            ),
            console_mirror_host=item.get("console_mirror_host"),
            console_mirror_port=int(item["console_mirror_port"]) if item.get("console_mirror_port") is not None else None,
            latitude=float(item["latitude"]) if item.get("latitude") is not None else None,
            longitude=float(item["longitude"]) if item.get("longitude") is not None else None,
        )
        for item in raw.get("endpoints", [])
    )
    if not endpoints:
        raise ValueError("At least one endpoint must be configured")

    management_nodes = tuple(
        ManagementNodeConfig(
            name=str(item["name"]),
            endpoint_name=str(item["endpoint_name"]),
            target_hash_prefix=str(item["target_hash_prefix"]).upper() if item.get("target_hash_prefix") else None,
            target_identity_hex=str(item["target_identity_hex"]).lower() if item.get("target_identity_hex") else None,
            guest_password=_password_str(item.get("guest_password")),
            admin_password=_password_str(item.get("admin_password")),
            prefer_role=str(item.get("prefer_role", "guest")).lower(),
            enabled=bool(item.get("enabled", True)),
            notes=_optional_str(item.get("notes")),
        )
        for item in raw.get("management_nodes", [])
    )

    identity = IdentityConfig(file_path=str(identity_raw.get("file_path", "./data/bot-identity.json")))

    storage = StorageConfig(database_path=str(storage_raw.get("database_path", "./data/meshcore-bot.db")))

    management = ManagementRuntimeConfig(
        enabled=bool(management_raw.get("enabled", True)),
        auto_discover_from_adverts=bool(management_raw.get("auto_discover_from_adverts", True)),
        auto_guest_password=_password_str(management_raw.get("auto_guest_password")),
        auto_admin_password=_password_str(management_raw.get("auto_admin_password")),
        temporary_admin_password=_optional_str(management_raw.get("temporary_admin_password")),
        temporary_admin_name_prefixes=tuple(str(item) for item in management_raw.get("temporary_admin_name_prefixes", [])),
        poll_interval_seconds=int(management_raw.get("poll_interval_seconds", 15)),
        request_timeout_seconds=int(management_raw.get("request_timeout_seconds", 20)),
        login_interval_seconds=int(management_raw.get("login_interval_seconds", 300)),
        retry_after_failed_poll_seconds=int(management_raw.get("retry_after_failed_poll_seconds", 180)),
        owner_poll_interval_seconds=int(management_raw.get("owner_poll_interval_seconds", 900)),
        acl_poll_interval_seconds=int(management_raw.get("acl_poll_interval_seconds", 900)),
        neighbors_poll_interval_seconds=int(management_raw.get("neighbors_poll_interval_seconds", 120)),
        console_neighbors_poll_interval_seconds=int(management_raw.get("console_neighbors_poll_interval_seconds", 120)),
        console_command_timeout_seconds=int(management_raw.get("console_command_timeout_seconds", 5)),
        neighbors_request_count=int(management_raw.get("neighbors_request_count", 24)),
        neighbors_prefix_length=int(management_raw.get("neighbors_prefix_length", 6)),
    )

    web = WebConfig(
        enabled=bool(web_raw.get("enabled", True)),
        host=str(web_raw.get("host", "0.0.0.0")),
        port=int(web_raw.get("port", 8080)),
    )

    admin_password = _optional_str(os.getenv("MESHCORE_ADMIN_PASSWORD")) or "changeme"
    admin_session_secret = _optional_str(os.getenv("MESHCORE_ADMIN_SESSION_SECRET"))
    if admin_session_secret is None and admin_password:
        admin_session_secret = hashlib.sha256(
            f"{source_path}:{admin_password}".encode("utf-8")
        ).hexdigest()
    admin = AdminConfig(
        password=admin_password,
        session_secret=admin_session_secret,
    )

    logging = LoggingConfig(level=str(logging_raw.get("level", "INFO")).upper())

    return AppConfig(
        bot=bot,
        identity=identity,
        storage=storage,
        channels=channels,
        endpoints=endpoints,
        management_nodes=management_nodes,
        management=management,
        web=web,
        admin=admin,
        logging=logging,
        source_path=source_path,
    )