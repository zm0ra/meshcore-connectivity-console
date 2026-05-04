"""Console mirror parsing for telemetry enrichment."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
import re

from .models import EndpointTelemetry

RX_LINE_RE = re.compile(
    r"RX, len=(?P<len>\d+) \(type=(?P<ptype>\d+), route=(?P<route>[A-Z_]+), payload_len=(?P<payload_len>\d+)\) SNR=(?P<snr>-?\d+) RSSI=(?P<rssi>-?\d+)",
    re.IGNORECASE,
)

ROUTE_ALIASES = {
    "F": "FLOOD",
    "D": "DIRECT",
    "FLOOD": "FLOOD",
    "DIRECT": "DIRECT",
    "TRANSPORT_FLOOD": "TRANSPORT_FLOOD",
    "TRANSPORT_DIRECT": "TRANSPORT_DIRECT",
}


class ConsoleTelemetryBuffer:
    def __init__(self, endpoint_name: str, max_items: int = 128) -> None:
        self.endpoint_name = endpoint_name
        self._items: deque[EndpointTelemetry] = deque(maxlen=max_items)
        self._raw_lines: deque[str] = deque(maxlen=max_items)

    def ingest_line(self, line: str) -> EndpointTelemetry | None:
        self._raw_lines.append(line)
        match = RX_LINE_RE.search(line)
        if not match:
            return None
        telemetry = EndpointTelemetry(
            endpoint_name=self.endpoint_name,
            route_label=ROUTE_ALIASES.get(match.group("route").upper(), match.group("route").upper()),
            payload_type=int(match.group("ptype")),
            payload_len=int(match.group("payload_len")),
            snr=float(match.group("snr")),
            rssi=int(match.group("rssi")),
            observed_at=datetime.now(tz=UTC),
        )
        self._items.append(telemetry)
        return telemetry

    def match(self, route_label: str, payload_type: int, payload_len: int, window_seconds: float = 2.0) -> EndpointTelemetry | None:
        now = datetime.now(tz=UTC)
        cutoff = now - timedelta(seconds=window_seconds)
        best: EndpointTelemetry | None = None
        for item in reversed(self._items):
            if item.observed_at < cutoff:
                break
            if item.route_label != route_label.upper():
                continue
            if item.payload_type != payload_type:
                continue
            if item.payload_len != payload_len:
                continue
            best = item
            break
        return best

    def recent_lines(self, limit: int = 20) -> list[str]:
        if limit <= 0:
            return []
        return list(self._raw_lines)[-limit:]


def normalize_console_reply(transcript: str, command: str) -> str:
    lines: list[str] = []
    for raw_line in transcript.replace("\r", "").split("\n"):
        line = raw_line.rstrip()
        if not line:
            continue
        if line.startswith("MeshCore repeater console"):
            continue
        if line == command:
            continue
        if line == ">":
            continue
        if line.startswith("> "):
            line = line[2:]
            if not line:
                continue
        if line.startswith("  -> "):
            line = line[5:]
        lines.append(line)
    return "\n".join(lines).strip()


def parse_console_neighbors_reply(reply: str) -> list[dict[str, object]]:
    text = reply.strip()
    if not text or text == "-none-":
        return []

    neighbors: list[dict[str, object]] = []
    for line in text.splitlines():
        parts = line.strip().split(":", 2)
        if len(parts) != 3:
            continue
        prefix_hex, heard_seconds_text, snr_text = parts
        try:
            heard_seconds = int(heard_seconds_text)
            snr_raw = int(snr_text)
        except ValueError:
            continue
        neighbors.append(
            {
                "neighbor_hash_prefix": prefix_hex.upper(),
                "last_heard_seconds": heard_seconds,
                "snr": snr_raw / 4.0,
                "rssi": None,
            }
        )
    return neighbors


def parse_console_owner_reply(reply: str) -> str:
    text = reply.strip()
    if text.startswith("> "):
        text = text[2:]
    if text == ">":
        return ""
    return text.strip()


def parse_console_acl_reply(reply: str) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for raw_line in reply.replace("\r", "").split("\n"):
        line = raw_line.strip()
        if not line or line == "ACL:" or line.startswith("MeshCore repeater console"):
            continue
        if line.startswith("> "):
            line = line[2:].strip()
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        perms_text, pubkey_hex = parts
        try:
            permissions = int(perms_text, 16)
        except ValueError:
            continue
        cleaned = "".join(ch for ch in pubkey_hex.upper() if ch in "0123456789ABCDEF")
        if len(cleaned) < 12:
            continue
        entries.append(
            {
                "pubkey_prefix_hex": cleaned[:12],
                "permissions": permissions,
                "identity_hex": cleaned.lower(),
            }
        )
    return entries