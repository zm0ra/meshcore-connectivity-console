from __future__ import annotations

from dataclasses import dataclass
import struct


REQ_TYPE_GET_STATUS = 0x01
REQ_TYPE_KEEP_ALIVE = 0x02
REQ_TYPE_GET_TELEMETRY_DATA = 0x03
REQ_TYPE_GET_ACCESS_LIST = 0x05
REQ_TYPE_GET_NEIGHBOURS = 0x06
REQ_TYPE_GET_OWNER_INFO = 0x07
TELEM_PERM_BASE = 0x01

ANON_REQ_TYPE_REGIONS = 0x01
ANON_REQ_TYPE_OWNER = 0x02
ANON_REQ_TYPE_BASIC = 0x03

RESP_SERVER_LOGIN_OK = 0x00


@dataclass(slots=True)
class LoginResponse:
    server_time: int
    response_code: int
    keep_alive_secs: int
    is_admin_legacy: bool
    permissions: int
    random_bytes: bytes
    firmware_capability_level: int | None


@dataclass(slots=True)
class OwnerInfoResponse:
    request_tag: int
    firmware_version: str | None
    node_name: str | None
    owner_info: str | None
    raw_text: str


@dataclass(slots=True)
class RepeaterStatus:
    request_tag: int
    batt_milli_volts: int
    curr_tx_queue_len: int
    noise_floor: int
    last_rssi: int
    n_packets_recv: int
    n_packets_sent: int
    total_air_time_secs: int
    total_up_time_secs: int
    n_sent_flood: int
    n_sent_direct: int
    n_recv_flood: int
    n_recv_direct: int
    err_events: int
    last_snr: float
    n_direct_dups: int
    n_flood_dups: int
    total_rx_air_time_secs: int
    n_recv_errors: int


@dataclass(slots=True)
class NeighbourEntry:
    pubkey_prefix_hex: str
    heard_seconds_ago: int
    snr: float


@dataclass(slots=True)
class NeighboursResponse:
    request_tag: int
    neighbours_count: int
    results_count: int
    entries: list[NeighbourEntry]


class ResponseParseError(ValueError):
    pass


def build_path_discovery_request(request_tag: int, *, random_bytes: bytes) -> bytes:
    if len(random_bytes) != 4:
        raise ValueError("path discovery random_bytes must be 4 bytes")
    return (
        struct.pack("<I", request_tag)
        + bytes([
            REQ_TYPE_GET_TELEMETRY_DATA,
            (~TELEM_PERM_BASE) & 0xFF,
            0,
            0,
            0,
        ])
        + random_bytes
    )


def parse_login_response(payload: bytes) -> LoginResponse:
    if len(payload) < 12:
        raise ResponseParseError("login response too short")
    server_time = struct.unpack_from("<I", payload, 0)[0]
    response_code = payload[4]
    keep_alive_secs = payload[5] * 16
    is_admin_legacy = payload[6] != 0
    permissions = payload[7]
    random_bytes = payload[8:12]
    firmware_capability_level = payload[12] if len(payload) > 12 else None
    return LoginResponse(
        server_time=server_time,
        response_code=response_code,
        keep_alive_secs=keep_alive_secs,
        is_admin_legacy=is_admin_legacy,
        permissions=permissions,
        random_bytes=random_bytes,
        firmware_capability_level=firmware_capability_level,
    )


def parse_owner_info_response(payload: bytes) -> OwnerInfoResponse:
    if len(payload) < 4:
        raise ResponseParseError("owner info response too short")
    request_tag = struct.unpack_from("<I", payload, 0)[0]
    raw_text = payload[4:].split(b"\x00", 1)[0].decode("utf-8", errors="replace")
    parts = raw_text.split("\n", 2)
    firmware_version = parts[0] if len(parts) > 0 and parts[0] else None
    node_name = parts[1] if len(parts) > 1 and parts[1] else None
    owner_info = parts[2] if len(parts) > 2 and parts[2] else None
    return OwnerInfoResponse(
        request_tag=request_tag,
        firmware_version=firmware_version,
        node_name=node_name,
        owner_info=owner_info,
        raw_text=raw_text,
    )


def parse_status_response(payload: bytes) -> RepeaterStatus:
    expected_len = 4 + 2 + 2 + 2 + 2 + 4 * 8 + 2 + 2 + 2 + 2 + 4 + 4
    if len(payload) < expected_len:
        raise ResponseParseError("status response too short")

    request_tag = struct.unpack_from("<I", payload, 0)[0]
    fields = struct.unpack_from("<HHhhIIIIIIIIHhHHII", payload, 4)
    return RepeaterStatus(
        request_tag=request_tag,
        batt_milli_volts=fields[0],
        curr_tx_queue_len=fields[1],
        noise_floor=fields[2],
        last_rssi=fields[3],
        n_packets_recv=fields[4],
        n_packets_sent=fields[5],
        total_air_time_secs=fields[6],
        total_up_time_secs=fields[7],
        n_sent_flood=fields[8],
        n_sent_direct=fields[9],
        n_recv_flood=fields[10],
        n_recv_direct=fields[11],
        err_events=fields[12],
        last_snr=fields[13] / 4.0,
        n_direct_dups=fields[14],
        n_flood_dups=fields[15],
        total_rx_air_time_secs=fields[16],
        n_recv_errors=fields[17],
    )


def parse_neighbours_response(payload: bytes, *, pubkey_prefix_len: int) -> NeighboursResponse:
    if len(payload) < 8:
        raise ResponseParseError("neighbours response too short")
    if pubkey_prefix_len <= 0:
        raise ResponseParseError("pubkey_prefix_len must be > 0")

    request_tag = struct.unpack_from("<I", payload, 0)[0]
    neighbours_count, results_count = struct.unpack_from("<HH", payload, 4)
    offset = 8
    entry_len = pubkey_prefix_len + 4 + 1
    entries: list[NeighbourEntry] = []

    for _ in range(results_count):
        if offset + entry_len > len(payload):
            raise ResponseParseError("truncated neighbours entry")
        prefix = payload[offset : offset + pubkey_prefix_len]
        offset += pubkey_prefix_len
        heard_seconds_ago = struct.unpack_from("<I", payload, offset)[0]
        offset += 4
        snr_q4 = struct.unpack_from("<b", payload, offset)[0]
        offset += 1
        entries.append(
            NeighbourEntry(
                pubkey_prefix_hex=prefix.hex().upper(),
                heard_seconds_ago=heard_seconds_ago,
                snr=snr_q4 / 4.0,
            )
        )

    return NeighboursResponse(
        request_tag=request_tag,
        neighbours_count=neighbours_count,
        results_count=results_count,
        entries=entries,
    )
