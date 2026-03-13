from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


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


@dataclass(frozen=True)
class AppConfig:
    service: ServiceConfig
    web: WebConfig
    storage: StorageConfig


def load_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path)
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    service = raw.get("service", {})
    web = raw.get("web", {})
    storage = raw.get("storage", {})
    base_dir = path.parent.parent.resolve()

    return AppConfig(
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
        ),
    )


def _resolve_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()