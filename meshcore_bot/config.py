from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class ServiceConfig:
    name: str
    log_level: str


@dataclass(frozen=True)
class StorageConfig:
    database_path: Path


@dataclass(frozen=True)
class IdentityConfig:
    key_file_path: Path


@dataclass(frozen=True)
class ProbeConfig:
    key_file_path: Path | None
    admin_password: str
    admin_password_name_prefixes: tuple[str, ...]
    admin_password_pubkey_prefixes: tuple[str, ...]
    guest_password: str
    default_guest_password: str
    guest_password_name_prefixes: tuple[str, ...]
    guest_password_pubkey_prefixes: tuple[str, ...]
    pre_login_advert_name: str
    pre_login_advert_delay_secs: float
    poll_interval_secs: float
    request_timeout_secs: float
    route_freshness_secs: float
    neighbours_page_size: int
    neighbours_prefix_len: int


@dataclass(frozen=True)
class WebConfig:
    host: str
    port: int


@dataclass(frozen=True)
class GatewayConfig:
    control_socket_path: Path
    event_socket_path: Path


@dataclass(frozen=True)
class EndpointConfig:
    name: str
    raw_host: str
    raw_port: int
    enabled: bool


@dataclass(frozen=True)
class AppConfig:
    service: ServiceConfig
    storage: StorageConfig
    identity: IdentityConfig
    probe: ProbeConfig
    web: WebConfig
    gateway: GatewayConfig
    endpoints: tuple[EndpointConfig, ...]


def load_config(config_path: str | Path) -> AppConfig:
    path = _resolve_config_path(config_path)
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    base_dir = path.parent.parent.resolve()
    service = raw.get("service", {})
    storage = raw.get("storage", {})
    identity = raw.get("identity", {})
    probe = raw.get("probe", {})
    web = raw.get("web", {})
    gateway = raw.get("gateway", {})

    endpoints = tuple(
        EndpointConfig(
            name=str(item["name"]),
            raw_host=str(item["raw_host"]),
            raw_port=int(item.get("raw_port", 5002)),
            enabled=bool(item.get("enabled", True)),
        )
        for item in raw.get("endpoints", [])
    )

    return AppConfig(
        service=ServiceConfig(
            name=str(service.get("name", "meshcore-bot")),
            log_level=str(service.get("log_level", "INFO")),
        ),
        storage=StorageConfig(
            database_path=_resolve_path(base_dir, str(storage.get("database_path", "./data/meshcore-bot.db"))),
        ),
        identity=IdentityConfig(
            key_file_path=_resolve_path(base_dir, str(identity.get("key_file_path", "./data/identity.bin"))),
        ),
        probe=ProbeConfig(
            key_file_path=_resolve_optional_path(base_dir, probe.get("key_file_path")),
            admin_password=str(probe.get("admin_password", "")),
            admin_password_name_prefixes=tuple(str(item) for item in probe.get("admin_password_name_prefixes", [])),
            admin_password_pubkey_prefixes=tuple(str(item).upper() for item in probe.get("admin_password_pubkey_prefixes", [])),
            guest_password=str(probe.get("guest_password", "")),
            default_guest_password=str(probe.get("default_guest_password", "")),
            guest_password_name_prefixes=tuple(str(item) for item in probe.get("guest_password_name_prefixes", [])),
            guest_password_pubkey_prefixes=tuple(str(item).upper() for item in probe.get("guest_password_pubkey_prefixes", [])),
            pre_login_advert_name=str(probe.get("pre_login_advert_name", "")).strip(),
            pre_login_advert_delay_secs=float(probe.get("pre_login_advert_delay_secs", 1.0)),
            poll_interval_secs=float(probe.get("poll_interval_secs", 2.0)),
            request_timeout_secs=float(probe.get("request_timeout_secs", 8.0)),
            route_freshness_secs=float(probe.get("route_freshness_secs", 1800.0)),
            neighbours_page_size=int(probe.get("neighbours_page_size", 15)),
            neighbours_prefix_len=int(probe.get("neighbours_prefix_len", 4)),
        ),
        web=WebConfig(
            host=str(web.get("host", "0.0.0.0")),
            port=int(web.get("port", 8080)),
        ),
        gateway=GatewayConfig(
            control_socket_path=_resolve_path(base_dir, str(gateway.get("control_socket_path", "./data/gateway/control.sock"))),
            event_socket_path=_resolve_path(base_dir, str(gateway.get("event_socket_path", "./data/gateway/events.sock"))),
        ),
        endpoints=endpoints,
    )


def _resolve_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _resolve_optional_path(base_dir: Path, value: object) -> Path | None:
    if value in (None, ""):
        return None
    return _resolve_path(base_dir, str(value))


def _resolve_config_path(config_path: str | Path) -> Path:
    path = Path(config_path).expanduser()
    if path.is_absolute():
        return path.resolve()

    cwd_candidate = path.resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    repo_candidate = (Path(__file__).resolve().parent.parent / path).resolve()
    return repo_candidate
