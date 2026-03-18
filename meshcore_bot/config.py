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
    advert_reprobe_success_cooldown_secs: float
    advert_reprobe_failure_cooldown_secs: float
    scheduled_reprobe_interval_secs: float
    night_failed_retry_start_hour: int
    night_failed_retry_end_hour: int
    night_failed_retry_interval_secs: float
    poll_interval_secs: float
    request_timeout_secs: float
    route_freshness_secs: float
    neighbours_page_size: int
    neighbours_prefix_len: int


@dataclass(frozen=True)
class BotConfig:
    enabled: bool
    sender_name: str
    channels: tuple[str, ...]
    enabled_commands: tuple[str, ...]
    min_response_delay_secs: float
    response_attempts: int
    echo_ack_timeout_secs: float
    response_retry_delay_secs: float
    response_retry_backoff_multiplier: float
    response_retry_max_delay_secs: float
    quiet_window_secs: float
    command_dedup_ttl_secs: float
    include_test_signal: bool


@dataclass(frozen=True)
class WebConfig:
    host: str
    port: int


@dataclass(frozen=True)
class GatewayConfig:
    control_socket_path: Path
    event_socket_path: Path
    traffic_watchdog_secs: float = 900.0
    close_timeout_secs: float = 2.0
    console_probe_timeout_secs: float = 1.0


@dataclass(frozen=True)
class EndpointConfig:
    name: str
    raw_host: str
    raw_port: int
    enabled: bool
    console_mirror_host: str | None = None
    console_mirror_port: int | None = None


@dataclass(frozen=True)
class AppConfig:
    service: ServiceConfig
    storage: StorageConfig
    identity: IdentityConfig
    probe: ProbeConfig
    bot: BotConfig
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
    bot = raw.get("bot", {})
    web = raw.get("web", {})
    gateway = raw.get("gateway", {})
    legacy_advert_reprobe_cooldown_secs = float(probe.get("advert_reprobe_cooldown_secs", 60.0))

    endpoints = tuple(
        EndpointConfig(
            name=str(item["name"]),
            raw_host=str(item["raw_host"]),
            raw_port=int(item.get("raw_port", 5002)),
            enabled=bool(item.get("enabled", True)),
            console_mirror_host=str(item["console_mirror_host"]).strip() or None
            if item.get("console_mirror_host") is not None
            else None,
            console_mirror_port=int(item["console_mirror_port"]) if item.get("console_mirror_port") is not None else None,
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
            advert_reprobe_success_cooldown_secs=float(
                probe.get("advert_reprobe_success_cooldown_secs", legacy_advert_reprobe_cooldown_secs)
            ),
            advert_reprobe_failure_cooldown_secs=float(
                probe.get("advert_reprobe_failure_cooldown_secs", legacy_advert_reprobe_cooldown_secs)
            ),
            scheduled_reprobe_interval_secs=float(probe.get("scheduled_reprobe_interval_secs", 7200.0)),
            night_failed_retry_start_hour=int(probe.get("night_failed_retry_start_hour", 1)),
            night_failed_retry_end_hour=int(probe.get("night_failed_retry_end_hour", 7)),
            night_failed_retry_interval_secs=float(probe.get("night_failed_retry_interval_secs", 3600.0)),
            poll_interval_secs=float(probe.get("poll_interval_secs", 2.0)),
            request_timeout_secs=float(probe.get("request_timeout_secs", 8.0)),
            route_freshness_secs=float(probe.get("route_freshness_secs", 1800.0)),
            neighbours_page_size=int(probe.get("neighbours_page_size", 15)),
            neighbours_prefix_len=int(probe.get("neighbours_prefix_len", 4)),
        ),
        bot=BotConfig(
            enabled=bool(bot.get("enabled", True)),
            sender_name=str(bot.get("sender_name", "")).strip(),
            channels=tuple(str(item).strip() for item in bot.get("channels", ["#bot-test"]) if str(item).strip()),
            enabled_commands=tuple(
                str(item).strip().lower() for item in bot.get("enabled_commands", ["!ping", "!test", "!help"])
                if str(item).strip()
            ),
            min_response_delay_secs=float(bot.get("min_response_delay_secs", 1.0)),
            response_attempts=max(1, int(bot.get("response_attempts", 2))),
            echo_ack_timeout_secs=max(0.0, float(bot.get("echo_ack_timeout_secs", 1.5))),
            response_retry_delay_secs=float(bot.get("response_retry_delay_secs", 1.75)),
            response_retry_backoff_multiplier=max(1.0, float(bot.get("response_retry_backoff_multiplier", 1.6))),
            response_retry_max_delay_secs=max(0.0, float(bot.get("response_retry_max_delay_secs", 10.0))),
            quiet_window_secs=float(bot.get("quiet_window_secs", 8.0)),
            command_dedup_ttl_secs=float(bot.get("command_dedup_ttl_secs", 30.0)),
            include_test_signal=bool(bot.get("include_test_signal", True)),
        ),
        web=WebConfig(
            host=str(web.get("host", "0.0.0.0")),
            port=int(web.get("port", 8080)),
        ),
        gateway=GatewayConfig(
            control_socket_path=_resolve_path(base_dir, str(gateway.get("control_socket_path", "./data/gateway/control.sock"))),
            event_socket_path=_resolve_path(base_dir, str(gateway.get("event_socket_path", "./data/gateway/events.sock"))),
            traffic_watchdog_secs=max(0.0, float(gateway.get("traffic_watchdog_secs", 900.0))),
            close_timeout_secs=max(0.0, float(gateway.get("close_timeout_secs", 2.0))),
            console_probe_timeout_secs=max(0.0, float(gateway.get("console_probe_timeout_secs", 1.0))),
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
