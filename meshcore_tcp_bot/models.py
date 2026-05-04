"""Shared runtime models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(slots=True)
class EndpointTelemetry:
    endpoint_name: str
    route_label: str
    payload_type: int
    payload_len: int
    snr: float | None
    rssi: int | None
    observed_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


@dataclass(slots=True)
class NodeRecord:
    identity_hex: str
    hash_prefix_hex: str
    name: str | None = None
    role: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    last_advert_at: datetime | None = None
    last_seen_endpoint: str | None = None


@dataclass(slots=True)
class MeshMessage:
    endpoint_name: str
    channel_name: str
    channel_psk: str | None
    sender: str
    sender_identity_hex: str | None
    content: str
    packet_type: str
    route_name: str
    path_hashes: list[str]
    path_len: int
    received_at: datetime
    channel_hash: int | None = None
    snr: float | None = None
    rssi: int | None = None
    distance_km: float | None = None
    raw_payload_hex: str = ""


@dataclass(slots=True)
class TraceResult:
    sender: str
    endpoint_name: str
    hops: list[str]
    hop_count: int


@dataclass(slots=True)
class RuntimeSnapshot:
    started_at: datetime
    endpoints: dict[str, dict[str, Any]]
    nodes: list[dict[str, Any]]
    messages: list[dict[str, Any]]
    diagnostics: dict[str, Any]
    identity: dict[str, Any] = field(default_factory=dict)
    persistence: dict[str, Any] = field(default_factory=dict)
    management: dict[str, Any] = field(default_factory=dict)