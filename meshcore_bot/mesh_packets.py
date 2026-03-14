from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import struct


class RouteType(IntEnum):
    TRANSPORT_FLOOD = 0x00
    FLOOD = 0x01
    DIRECT = 0x02
    TRANSPORT_DIRECT = 0x03


class PayloadType(IntEnum):
    REQ = 0x00
    RESPONSE = 0x01
    TXT_MSG = 0x02
    ACK = 0x03
    ADVERT = 0x04
    GRP_TXT = 0x05
    GRP_DATA = 0x06
    ANON_REQ = 0x07
    PATH = 0x08
    TRACE = 0x09
    MULTIPART = 0x0A
    CONTROL = 0x0B
    RAW_CUSTOM = 0x0F


class AdvertType(IntEnum):
    NONE = 0
    CHAT = 1
    REPEATER = 2
    ROOM = 3
    SENSOR = 4


ADV_LATLON_MASK = 0x10
ADV_FEAT1_MASK = 0x20
ADV_FEAT2_MASK = 0x40
ADV_NAME_MASK = 0x80
PUB_KEY_SIZE = 32
PATH_HASH_SIZE_MASK = 0xC0
PATH_LEN_MASK = 0x3F


@dataclass(slots=True)
class PacketSummary:
    route_type: RouteType
    payload_type: PayloadType
    transport_codes: tuple[int, int] | None
    path_hash_size: int
    path_len: int
    encoded_path_len: int
    path_hashes: list[str]
    path_bytes: bytes
    payload: bytes
    raw: bytes


@dataclass(slots=True)
class AdvertRecord:
    public_key: bytes
    timestamp: int
    advert_type: AdvertType
    name: str | None
    latitude: float | None
    longitude: float | None
    feature_1: int | None
    feature_2: int | None
    signature: bytes
    app_data: bytes


class PacketParseError(ValueError):
    pass


class AdvertParseError(ValueError):
    pass


def parse_packet(packet: bytes) -> PacketSummary:
    if len(packet) < 2:
        raise PacketParseError("packet too short")

    header = packet[0]
    route_type = RouteType(header & 0x03)
    payload_type = PayloadType((header >> 2) & 0x0F)

    index = 1
    transport_codes: tuple[int, int] | None = None
    if route_type in (RouteType.TRANSPORT_FLOOD, RouteType.TRANSPORT_DIRECT):
        if len(packet) < index + 4:
            raise PacketParseError("missing transport codes")
        transport_codes = struct.unpack_from("<HH", packet, index)
        index += 4

    if len(packet) <= index:
        raise PacketParseError("missing path_len")

    encoded_path_len = packet[index]
    index += 1
    path_hash_size = (encoded_path_len >> 6) + 1
    path_len = encoded_path_len & PATH_LEN_MASK
    path_bytes_len = path_hash_size * path_len
    payload_index = index + path_bytes_len
    if payload_index > len(packet):
        raise PacketParseError("invalid path length")

    path_hashes = [
        packet[offset : offset + path_hash_size].hex().upper()
        for offset in range(index, payload_index, path_hash_size)
    ]
    path_bytes = packet[index:payload_index]

    return PacketSummary(
        route_type=route_type,
        payload_type=payload_type,
        transport_codes=transport_codes,
        path_hash_size=path_hash_size,
        path_len=path_len,
        encoded_path_len=encoded_path_len,
        path_hashes=path_hashes,
        path_bytes=path_bytes,
        payload=packet[payload_index:],
        raw=packet,
    )


def parse_advert(summary: PacketSummary) -> AdvertRecord:
    if summary.payload_type is not PayloadType.ADVERT:
        raise AdvertParseError("not an advert payload")
    if len(summary.payload) < PUB_KEY_SIZE + 4 + 64 + 1:
        raise AdvertParseError("advert payload too short")

    index = 0
    public_key = summary.payload[index : index + PUB_KEY_SIZE]
    index += PUB_KEY_SIZE
    timestamp = struct.unpack_from("<I", summary.payload, index)[0]
    index += 4
    signature = summary.payload[index : index + 64]
    index += 64
    app_data = summary.payload[index:]
    advert_type, name, latitude, longitude, feature_1, feature_2 = parse_advert_app_data(app_data)

    return AdvertRecord(
        public_key=public_key,
        timestamp=timestamp,
        advert_type=advert_type,
        name=name,
        latitude=latitude,
        longitude=longitude,
        feature_1=feature_1,
        feature_2=feature_2,
        signature=signature,
        app_data=app_data,
    )


def parse_advert_app_data(app_data: bytes) -> tuple[AdvertType, str | None, float | None, float | None, int | None, int | None]:
    if not app_data:
        raise AdvertParseError("missing app_data")

    flags = app_data[0]
    index = 1
    latitude: float | None = None
    longitude: float | None = None
    feature_1: int | None = None
    feature_2: int | None = None

    if flags & ADV_LATLON_MASK:
        if len(app_data) < index + 8:
            raise AdvertParseError("truncated lat/lon")
        raw_lat, raw_lon = struct.unpack_from("<ii", app_data, index)
        latitude = raw_lat / 1_000_000.0
        longitude = raw_lon / 1_000_000.0
        index += 8
    if flags & ADV_FEAT1_MASK:
        if len(app_data) < index + 2:
            raise AdvertParseError("truncated feature_1")
        feature_1 = struct.unpack_from("<H", app_data, index)[0]
        index += 2
    if flags & ADV_FEAT2_MASK:
        if len(app_data) < index + 2:
            raise AdvertParseError("truncated feature_2")
        feature_2 = struct.unpack_from("<H", app_data, index)[0]
        index += 2

    name: str | None = None
    if flags & ADV_NAME_MASK:
        name_bytes = app_data[index:]
        name = name_bytes.decode("utf-8", errors="replace") or None

    return AdvertType(flags & 0x0F), name, latitude, longitude, feature_1, feature_2


def describe_packet_summary(summary: PacketSummary) -> str:
    transport = "-"
    if summary.transport_codes is not None:
        transport = f"{summary.transport_codes[0]:04X}/{summary.transport_codes[1]:04X}"
    path_hashes = ",".join(summary.path_hashes) if summary.path_hashes else "-"
    return (
        f"route={summary.route_type.name} "
        f"payload={summary.payload_type.name} "
        f"transport={transport} "
        f"path_hash_size={summary.path_hash_size} "
        f"path_len={summary.path_len} "
        f"path_hashes={path_hashes} "
        f"payload_len={len(summary.payload)}"
    )
