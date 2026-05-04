"""Helpers for building, parsing, and decoding MeshCore packets over TCP."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum
import hashlib
import hmac
import math
import struct
import time

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .identity import MeshcoreIdentity

ROUTE_TRANSPORT_FLOOD = 0x00
ROUTE_FLOOD = 0x01
ROUTE_DIRECT = 0x02
ROUTE_TRANSPORT_DIRECT = 0x03

TYPE_REQ = 0x00
TYPE_RESPONSE = 0x01
TYPE_TXT_MSG = 0x02
TYPE_ADVERT = 0x04
TYPE_GRP_TXT = 0x05
TYPE_GRP_DATA = 0x06
TYPE_ANON_REQ = 0x07
TYPE_PATH = 0x08
TYPE_TRACE = 0x09

ADV_TYPE_CHAT = 0x01
ADV_LATLON_MASK = 0x10
ADV_FEAT1_MASK = 0x20
ADV_FEAT2_MASK = 0x40
ADV_NAME_MASK = 0x80

REQ_TYPE_GET_STATUS = 0x01
REQ_TYPE_KEEP_ALIVE = 0x02
REQ_TYPE_GET_TELEMETRY_DATA = 0x03
REQ_TYPE_GET_ACCESS_LIST = 0x05
REQ_TYPE_GET_NEIGHBORS = 0x06
REQ_TYPE_GET_OWNER_INFO = 0x07

RESP_SERVER_LOGIN_OK = 0x00

TXT_TYPE_PLAIN = 0x00
TXT_TYPE_CLI_DATA = 0x01
TXT_TYPE_SIGNED = 0x02

PUBLIC_GROUP_PSK = "izOH6cXN6mrJ5e26oRXNcg=="

CIPHER_KEY_SIZE = 16
PUB_KEY_SIZE = 32
CIPHER_MAC_SIZE = 2

_last_wire_timestamp = 0


class RouteType(IntEnum):
    TRANSPORT_FLOOD = 0x00
    FLOOD = 0x01
    DIRECT = 0x02
    TRANSPORT_DIRECT = 0x03


class PayloadType(IntEnum):
    REQ = 0x00
    RESPONSE = 0x01
    TXT_MSG = 0x02
    ADVERT = 0x04
    GRP_TXT = 0x05
    GRP_DATA = 0x06
    ANON_REQ = 0x07
    PATH = 0x08
    TRACE = 0x09


ROUTE_NAMES = {
    ROUTE_TRANSPORT_FLOOD: "TRANSPORT_FLOOD",
    ROUTE_FLOOD: "FLOOD",
    ROUTE_DIRECT: "DIRECT",
    ROUTE_TRANSPORT_DIRECT: "TRANSPORT_DIRECT",
}

TYPE_NAMES = {
    TYPE_REQ: "REQ",
    TYPE_RESPONSE: "RESPONSE",
    TYPE_TXT_MSG: "TXT_MSG",
    TYPE_ADVERT: "ADVERT",
    TYPE_GRP_TXT: "GRP_TXT",
    TYPE_GRP_DATA: "GRP_DATA",
    TYPE_ANON_REQ: "ANON_REQ",
    TYPE_PATH: "PATH",
    TYPE_TRACE: "TRACE",
}


def _next_wire_timestamp(timestamp: int | None = None) -> int:
    global _last_wire_timestamp
    if timestamp is not None:
        wire_timestamp = timestamp & 0xFFFFFFFF
        if wire_timestamp > _last_wire_timestamp:
            _last_wire_timestamp = wire_timestamp
        return wire_timestamp
    wire_timestamp = int(time.time()) & 0xFFFFFFFF
    if wire_timestamp <= _last_wire_timestamp:
        wire_timestamp = (_last_wire_timestamp + 1) & 0xFFFFFFFF
    _last_wire_timestamp = wire_timestamp
    return wire_timestamp


@dataclass(slots=True)
class PacketSummary:
    route: int
    route_name: str
    packet_type: int
    packet_type_name: str
    transport_codes: tuple[int, int] | None
    path_len: int
    path_hash_size: int
    path_hashes: list[str]
    payload: bytes
    raw: bytes


@dataclass(slots=True)
class PublicGroupText:
    timestamp: int
    timestamp_iso: str
    txt_type: int
    text: str
    channel_hash: int


@dataclass(slots=True)
class ChannelDefinition:
    name: str
    psk: str | None = None

    @property
    def secret(self) -> bytes:
        return resolve_channel_secret(self.name, self.psk)

    @property
    def channel_hash(self) -> int:
        return _public_channel_hash(self.secret)


@dataclass(slots=True)
class AdvertRecord:
    identity_hex: str
    hash_prefix_hex: str
    timestamp: int | None
    name: str | None
    role: str | None
    latitude: float | None
    longitude: float | None


@dataclass(slots=True)
class TracePacket:
    tag: int
    auth_code: int
    flags: int
    path_hashes: list[str]
    path_snrs: list[float]


@dataclass(slots=True)
class PeerContact:
    identity_hex: str
    public_key: bytes
    name: str | None = None


@dataclass(slots=True)
class PrivateDatagram:
    packet_type: int
    packet_type_name: str
    route_name: str
    sender_identity_hex: str
    sender_name: str | None
    sender_public_key: bytes
    path_hashes: list[str]
    path_len: int
    plaintext: bytes


@dataclass(slots=True)
class PathReturnPayload:
    path_hashes: list[str]
    path_len: int
    extra_type: int
    extra_type_name: str
    extra_payload: bytes


@dataclass(slots=True)
class LoginResponse:
    success: bool
    server_timestamp: int
    keep_alive_secs: int | None
    is_admin: bool
    permissions: int | None
    firmware_level: int | None
    raw_body_hex: str


@dataclass(slots=True)
class OwnerInfoResult:
    raw_text: str
    firmware_version: str | None
    node_name: str | None
    owner_info: str | None


@dataclass(slots=True)
class AclEntry:
    pubkey_prefix_hex: str
    permissions: int

    @property
    def is_admin(self) -> bool:
        return (self.permissions & 0x03) == 0x01

    @property
    def is_guest(self) -> bool:
        return (self.permissions & 0x03) == 0x02


@dataclass(slots=True)
class NeighborEntry:
    neighbor_hash_prefix: str
    last_heard_seconds: int
    snr: float


@dataclass(slots=True)
class NeighborSnapshotResult:
    total_count: int
    result_count: int
    entries: list[NeighborEntry]


def _public_channel_secret() -> bytes:
    return base64.b64decode(PUBLIC_GROUP_PSK)


def _normalize_hashtag_name(name: str) -> str:
    channel_name = name.strip().lower()
    if not channel_name:
        raise ValueError("channel name cannot be empty")
    return channel_name if channel_name.startswith("#") else f"#{channel_name}"


def build_hashtag_channel_psk(name: str) -> str:
    secret = hashlib.sha256(_normalize_hashtag_name(name).encode("utf-8")).digest()[:CIPHER_KEY_SIZE]
    return base64.b64encode(secret).decode("ascii")


def resolve_channel_secret(name: str, psk: str | None) -> bytes:
    if psk:
        return base64.b64decode(psk)
    return base64.b64decode(build_hashtag_channel_psk(name))


def _public_channel_hash(secret: bytes) -> int:
    digest = hashlib.sha256(secret).digest()
    return digest[0]


def _pad_to_block_size(data: bytes, block_size: int = 16) -> bytes:
    padding_len = (-len(data)) % block_size
    return data + (b"\x00" * padding_len)


def _encrypt_aes128(secret: bytes, plaintext: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(secret[:CIPHER_KEY_SIZE]), modes.ECB())
    encryptor = cipher.encryptor()
    return encryptor.update(_pad_to_block_size(plaintext)) + encryptor.finalize()


def _decrypt_aes128(secret: bytes, ciphertext: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(secret[:CIPHER_KEY_SIZE]), modes.ECB())
    decryptor = cipher.decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()


def _encrypt_then_mac(secret: bytes, plaintext: bytes) -> bytes:
    encrypted = _encrypt_aes128(secret, plaintext)
    mac = hmac.new(secret[:PUB_KEY_SIZE], encrypted, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
    return mac + encrypted


def _decrypt_then_verify(secret: bytes, ciphertext_with_mac: bytes) -> bytes:
    if len(ciphertext_with_mac) < CIPHER_MAC_SIZE:
        raise ValueError("ciphertext too short")

    received_mac = ciphertext_with_mac[:CIPHER_MAC_SIZE]
    ciphertext = ciphertext_with_mac[CIPHER_MAC_SIZE:]
    expected_mac = hmac.new(secret[:PUB_KEY_SIZE], ciphertext, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
    if received_mac != expected_mac:
        raise ValueError("message MAC mismatch")
    return _decrypt_aes128(secret, ciphertext).rstrip(b"\x00")


def _build_packet(
    packet_type: int,
    payload: bytes,
    *,
    route: int = ROUTE_FLOOD,
    path_hashes: list[str] | tuple[str, ...] = (),
    hash_size: int = 1,
    transport_codes: tuple[int, int] | None = None,
) -> bytes:
    packet = bytearray()
    packet.append(((packet_type & 0x0F) << 2) | (route & 0x03))
    if route in (ROUTE_TRANSPORT_FLOOD, ROUTE_TRANSPORT_DIRECT):
        if transport_codes is None:
            raise ValueError("transport route requires transport codes")
        packet.extend(struct.pack("<HH", *transport_codes))
    packet.append(((hash_size - 1) << 6) | (len(path_hashes) & 0x3F))
    for item in path_hashes:
        raw = bytes.fromhex(item)
        if len(raw) != hash_size:
            raise ValueError(f"path hash {item!r} does not match hash size {hash_size}")
        packet.extend(raw)
    packet.extend(payload)
    return bytes(packet)


def build_group_text_packet(
    sender_name: str,
    message: str,
    channel_psk: str | None = PUBLIC_GROUP_PSK,
    timestamp: int | None = None,
    channel_name: str = "public",
) -> bytes:
    secret = resolve_channel_secret(channel_name, channel_psk)
    channel_hash = _public_channel_hash(secret)
    timestamp = _next_wire_timestamp(timestamp)

    plaintext = bytearray()
    plaintext.extend(struct.pack("<I", timestamp))
    plaintext.append(0x00)
    plaintext.extend(f"{sender_name}: {message}".encode("utf-8"))

    encrypted = _encrypt_then_mac(secret, bytes(plaintext))
    payload = bytes([channel_hash]) + encrypted
    return _build_packet(TYPE_GRP_TXT, payload, route=ROUTE_FLOOD)


def _build_advert_app_data(
    advert_type: int,
    *,
    name: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    feature_1: int = 0,
    feature_2: int = 0,
) -> bytes:
    flags = advert_type & 0x0F
    payload = bytearray()
    if latitude is not None and longitude is not None:
        flags |= ADV_LATLON_MASK
        payload.extend(struct.pack("<i", int(latitude * 1_000_000)))
        payload.extend(struct.pack("<i", int(longitude * 1_000_000)))
    if feature_1:
        flags |= ADV_FEAT1_MASK
        payload.extend(struct.pack("<H", feature_1 & 0xFFFF))
    if feature_2:
        flags |= ADV_FEAT2_MASK
        payload.extend(struct.pack("<H", feature_2 & 0xFFFF))
    if name:
        flags |= ADV_NAME_MASK
        payload.extend(name.encode("utf-8")[:96])
    return bytes([flags]) + bytes(payload)


def build_advert_packet(
    local_identity: MeshcoreIdentity,
    *,
    name: str,
    timestamp: int | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    advert_type: int = ADV_TYPE_CHAT,
    route: int = ROUTE_DIRECT,
) -> bytes:
    wire_timestamp = int(time.time()) if timestamp is None else timestamp
    app_data = _build_advert_app_data(
        advert_type,
        name=name,
        latitude=latitude,
        longitude=longitude,
    )
    signed_message = local_identity.public_key + struct.pack("<I", wire_timestamp) + app_data
    signature = local_identity.sign(signed_message)
    payload = signed_message[:PUB_KEY_SIZE + 4] + signature + app_data
    return _build_packet(TYPE_ADVERT, payload, route=route)


def build_public_group_text_packet(sender_name: str, message: str, timestamp: int | None = None) -> bytes:
    return build_group_text_packet(sender_name, message, channel_psk=PUBLIC_GROUP_PSK, timestamp=timestamp, channel_name="public")


def build_text_plaintext(text: str, *, timestamp: int | None = None, txt_type: int = TXT_TYPE_PLAIN, attempt: int = 0) -> bytes:
    wire_timestamp = _next_wire_timestamp(timestamp)
    flags = ((txt_type & 0x3F) << 2) | (attempt & 0x03)
    return struct.pack("<IB", wire_timestamp, flags) + text.encode("utf-8")


def parse_text_plaintext(plaintext: bytes) -> tuple[int, int, int, str] | None:
    if len(plaintext) < 5:
        return None
    timestamp = struct.unpack_from("<I", plaintext, 0)[0]
    flags = plaintext[4]
    txt_type = (flags >> 2) & 0x3F
    attempt = flags & 0x03
    text = plaintext[5:].decode("utf-8", errors="replace")
    return timestamp, txt_type, attempt, text


def build_private_datagram(
    packet_type: int,
    local_identity: MeshcoreIdentity,
    recipient_public_key: bytes,
    plaintext: bytes,
    *,
    route: int = ROUTE_FLOOD,
    path_hashes: list[str] | tuple[str, ...] = (),
) -> bytes:
    secret = local_identity.calc_shared_secret(recipient_public_key)
    payload = recipient_public_key[:1] + local_identity.public_key[:1] + _encrypt_then_mac(secret, plaintext)
    return _build_packet(packet_type, payload, route=route, path_hashes=path_hashes)


def build_private_text_packet(
    local_identity: MeshcoreIdentity,
    recipient_public_key: bytes,
    text: str,
    *,
    timestamp: int | None = None,
    txt_type: int = TXT_TYPE_PLAIN,
    attempt: int = 0,
    route: int = ROUTE_FLOOD,
    path_hashes: list[str] | tuple[str, ...] = (),
) -> bytes:
    plaintext = build_text_plaintext(text, timestamp=timestamp, txt_type=txt_type, attempt=attempt)
    return build_private_datagram(
        TYPE_TXT_MSG,
        local_identity,
        recipient_public_key,
        plaintext,
        route=route,
        path_hashes=path_hashes,
    )


def build_login_packet(
    local_identity: MeshcoreIdentity,
    recipient_public_key: bytes,
    password: str,
    *,
    timestamp: int | None = None,
    route: int = ROUTE_FLOOD,
    path_hashes: list[str] | tuple[str, ...] = (),
) -> bytes:
    wire_timestamp = _next_wire_timestamp(timestamp)
    plaintext = struct.pack("<I", wire_timestamp) + password.encode("utf-8")[:15]
    secret = local_identity.calc_shared_secret(recipient_public_key)
    payload = recipient_public_key[:1] + local_identity.public_key + _encrypt_then_mac(secret, plaintext)
    return _build_packet(TYPE_ANON_REQ, payload, route=route, path_hashes=path_hashes)


def build_request_packet(
    local_identity: MeshcoreIdentity,
    recipient_public_key: bytes,
    request_data: bytes,
    *,
    tag: int | None = None,
    route: int = ROUTE_FLOOD,
    path_hashes: list[str] | tuple[str, ...] = (),
) -> tuple[int, bytes]:
    wire_tag = (time.time_ns() // 1000) & 0xFFFFFFFF if tag is None else tag & 0xFFFFFFFF
    plaintext = struct.pack("<I", wire_tag) + request_data
    packet = build_private_datagram(
        TYPE_REQ,
        local_identity,
        recipient_public_key,
        plaintext,
        route=route,
        path_hashes=path_hashes,
    )
    return wire_tag, packet


def build_neighbors_request_payload(
    *,
    count: int,
    offset: int = 0,
    order_by: int = 0,
    pubkey_prefix_length: int = 6,
    uniqueness_blob: bytes | None = None,
) -> bytes:
    blob = uniqueness_blob or hashlib.sha256(struct.pack("<d", time.time())).digest()[:4]
    return bytes([REQ_TYPE_GET_NEIGHBORS, 0x00, count & 0xFF]) + struct.pack("<H", offset & 0xFFFF) + bytes([
        order_by & 0xFF,
        pubkey_prefix_length & 0xFF,
    ]) + blob[:4]


def describe_packet(packet: bytes) -> PacketSummary:
    if len(packet) < 2:
        raise ValueError("packet too short")

    header = packet[0]
    route = header & 0x03
    packet_type = (header >> 2) & 0x0F
    index = 1
    transport_codes: tuple[int, int] | None = None
    if route in (ROUTE_TRANSPORT_FLOOD, ROUTE_TRANSPORT_DIRECT):
        if len(packet) < index + 4:
            raise ValueError("packet too short for transport codes")
        transport_codes = struct.unpack_from("<HH", packet, index)
        index += 4

    if len(packet) <= index:
        raise ValueError("packet missing path length")

    encoded_path_len = packet[index]
    index += 1
    path_hash_size = (encoded_path_len >> 6) + 1
    path_len = encoded_path_len & 0x3F
    path_bytes_len = path_hash_size * path_len
    payload_start = index + path_bytes_len
    if payload_start > len(packet):
        raise ValueError("invalid path length")

    path_hashes = [
        packet[i:i + path_hash_size].hex().upper()
        for i in range(index, payload_start, path_hash_size)
    ]

    return PacketSummary(
        route=route,
        route_name=ROUTE_NAMES.get(route, f"0x{route:02X}"),
        packet_type=packet_type,
        packet_type_name=TYPE_NAMES.get(packet_type, f"0x{packet_type:02X}"),
        transport_codes=transport_codes,
        path_len=path_len,
        path_hash_size=path_hash_size,
        path_hashes=path_hashes,
        payload=packet[payload_start:],
        raw=packet,
    )


def try_decode_group_text(packet: bytes, channels: list[ChannelDefinition] | tuple[ChannelDefinition, ...]) -> tuple[ChannelDefinition, PublicGroupText] | None:
    summary = describe_packet(packet)
    if summary.packet_type != TYPE_GRP_TXT or not summary.payload:
        return None

    for channel in channels:
        if summary.payload[0] != channel.channel_hash:
            continue
        try:
            plaintext = _decrypt_then_verify(channel.secret, summary.payload[1:])
        except ValueError:
            continue
        parsed = parse_text_plaintext(plaintext)
        if parsed is None:
            continue
        timestamp, txt_type, _, text = parsed
        return (
            channel,
            PublicGroupText(
                timestamp=timestamp,
                timestamp_iso=datetime.fromtimestamp(timestamp, tz=UTC).isoformat(),
                txt_type=txt_type,
                text=text,
                channel_hash=channel.channel_hash,
            ),
        )
    return None


def try_decode_private_datagram(
    packet: bytes,
    local_identity: MeshcoreIdentity,
    contacts: list[PeerContact] | tuple[PeerContact, ...],
) -> PrivateDatagram | None:
    summary = describe_packet(packet)
    if summary.packet_type in (TYPE_REQ, TYPE_RESPONSE, TYPE_TXT_MSG, TYPE_PATH):
        if len(summary.payload) < 2 + CIPHER_MAC_SIZE:
            return None
        dest_hash = summary.payload[:1]
        src_hash = summary.payload[1:2]
        if local_identity.public_key[:1] != dest_hash:
            return None
        ciphertext = summary.payload[2:]
        for contact in contacts:
            if contact.public_key[:1] != src_hash:
                continue
            try:
                plaintext = _decrypt_then_verify(local_identity.calc_shared_secret(contact.public_key), ciphertext)
            except ValueError:
                continue
            return PrivateDatagram(
                packet_type=summary.packet_type,
                packet_type_name=summary.packet_type_name,
                route_name=summary.route_name,
                sender_identity_hex=contact.identity_hex,
                sender_name=contact.name,
                sender_public_key=contact.public_key,
                path_hashes=summary.path_hashes,
                path_len=summary.path_len,
                plaintext=plaintext,
            )
        return None

    if summary.packet_type == TYPE_ANON_REQ:
        if len(summary.payload) < 1 + PUB_KEY_SIZE + CIPHER_MAC_SIZE:
            return None
        dest_hash = summary.payload[:1]
        if local_identity.public_key[:1] != dest_hash:
            return None
        sender_public_key = summary.payload[1:1 + PUB_KEY_SIZE]
        ciphertext = summary.payload[1 + PUB_KEY_SIZE:]
        try:
            plaintext = _decrypt_then_verify(local_identity.calc_shared_secret(sender_public_key), ciphertext)
        except ValueError:
            return None
        sender_hex = sender_public_key.hex()
        sender_name = None
        for contact in contacts:
            if contact.identity_hex == sender_hex:
                sender_name = contact.name
                break
        return PrivateDatagram(
            packet_type=summary.packet_type,
            packet_type_name=summary.packet_type_name,
            route_name=summary.route_name,
            sender_identity_hex=sender_hex,
            sender_name=sender_name,
            sender_public_key=sender_public_key,
            path_hashes=summary.path_hashes,
            path_len=summary.path_len,
            plaintext=plaintext,
        )
    return None


def parse_login_response(plaintext: bytes) -> LoginResponse | None:
    if len(plaintext) < 4:
        return None
    server_timestamp = struct.unpack_from("<I", plaintext, 0)[0]
    if len(plaintext) >= 6 and plaintext[4:6] == b"OK":
        return LoginResponse(
            success=True,
            server_timestamp=server_timestamp,
            keep_alive_secs=None,
            is_admin=False,
            permissions=None,
            firmware_level=None,
            raw_body_hex=plaintext[4:].hex(),
        )
    if len(plaintext) < 5:
        return None
    return LoginResponse(
        success=plaintext[4] == RESP_SERVER_LOGIN_OK,
        server_timestamp=server_timestamp,
        keep_alive_secs=plaintext[5] * 16 if len(plaintext) > 5 else None,
        is_admin=bool(plaintext[6]) if len(plaintext) > 6 else False,
        permissions=plaintext[7] if len(plaintext) > 7 else None,
        firmware_level=plaintext[12] if len(plaintext) > 12 else None,
        raw_body_hex=plaintext[4:].hex(),
    )


def parse_path_return_payload(plaintext: bytes) -> PathReturnPayload | None:
    if len(plaintext) < 2:
        return None
    encoded_path_len = plaintext[0]
    hash_size = (encoded_path_len >> 6) + 1
    hash_count = encoded_path_len & 0x3F
    path_bytes_len = hash_size * hash_count
    if len(plaintext) < 1 + path_bytes_len + 1:
        return None
    offset = 1
    path_hashes = [
        plaintext[i:i + hash_size].hex().upper()
        for i in range(offset, offset + path_bytes_len, hash_size)
    ]
    offset += path_bytes_len
    extra_type = plaintext[offset] & 0x0F
    offset += 1
    return PathReturnPayload(
        path_hashes=path_hashes,
        path_len=hash_count,
        extra_type=extra_type,
        extra_type_name=TYPE_NAMES.get(extra_type, f"0x{extra_type:02X}"),
        extra_payload=plaintext[offset:],
    )


def parse_tagged_response(plaintext: bytes) -> tuple[int, bytes] | None:
    if len(plaintext) < 4:
        return None
    return struct.unpack_from("<I", plaintext, 0)[0], plaintext[4:]


def parse_owner_info_response(body: bytes) -> OwnerInfoResult:
    raw_text = body.decode("utf-8", errors="replace").strip()
    lines = raw_text.splitlines()
    firmware_version = lines[0] if len(lines) > 0 else None
    node_name = lines[1] if len(lines) > 1 else None
    owner_info = "\n".join(lines[2:]) if len(lines) > 2 else None
    return OwnerInfoResult(
        raw_text=raw_text,
        firmware_version=firmware_version,
        node_name=node_name,
        owner_info=owner_info,
    )


def parse_acl_response(body: bytes) -> list[AclEntry]:
    entries: list[AclEntry] = []
    entry_size = 7
    for offset in range(0, len(body) - (len(body) % entry_size), entry_size):
        prefix = body[offset:offset + 6].hex().upper()
        permissions = body[offset + 6]
        entries.append(AclEntry(pubkey_prefix_hex=prefix, permissions=permissions))
    return entries


def parse_neighbors_response(body: bytes, pubkey_prefix_length: int) -> NeighborSnapshotResult | None:
    if len(body) < 4:
        return None
    total_count, result_count = struct.unpack_from("<HH", body, 0)
    entries: list[NeighborEntry] = []
    entry_size = pubkey_prefix_length + 4 + 1
    offset = 4
    while offset + entry_size <= len(body):
        prefix = body[offset:offset + pubkey_prefix_length].hex().upper()
        offset += pubkey_prefix_length
        last_heard_seconds = struct.unpack_from("<I", body, offset)[0]
        offset += 4
        snr = struct.unpack("b", body[offset:offset + 1])[0] / 4.0
        offset += 1
        entries.append(
            NeighborEntry(
                neighbor_hash_prefix=prefix,
                last_heard_seconds=last_heard_seconds,
                snr=snr,
            )
        )
    return NeighborSnapshotResult(total_count=total_count, result_count=result_count, entries=entries)


def decode_advert(packet: bytes) -> AdvertRecord | None:
    summary = describe_packet(packet)
    if summary.packet_type != TYPE_ADVERT or len(summary.payload) < 32 + 4 + 64:
        return None

    identity = summary.payload[:32]
    timestamp = struct.unpack("<I", summary.payload[32:36])[0]
    appdata = summary.payload[100:]

    role = None
    name = None
    latitude = None
    longitude = None
    if appdata:
        flags = appdata[0]
        role_map = {
            0x00: "Unknown",
            0x01: "Chat Node",
            0x02: "Repeater",
            0x03: "Room Server",
            0x04: "Sensor",
        }
        role = role_map.get(flags & 0x0F, f"ROLE_{flags & 0x0F}")
        index = 1
        if flags & 0x10 and len(appdata) >= index + 8:
            lat_i, lon_i = struct.unpack_from("<ii", appdata, index)
            candidate_latitude = lat_i / 1_000_000
            candidate_longitude = lon_i / 1_000_000
            if -90.0 <= candidate_latitude <= 90.0:
                latitude = candidate_latitude
            if -180.0 <= candidate_longitude <= 180.0:
                longitude = candidate_longitude
            index += 8
        if flags & 0x20 and len(appdata) >= index + 2:
            index += 2
        if flags & 0x40 and len(appdata) >= index + 2:
            index += 2
        if flags & 0x80 and index < len(appdata):
            raw_name = appdata[index:].split(b"\x00", 1)[0]
            decoded_name = raw_name.decode("utf-8", errors="replace").strip()
            name = decoded_name or None

    return AdvertRecord(
        identity_hex=identity.hex(),
        hash_prefix_hex=identity[:1].hex().upper(),
        timestamp=timestamp,
        name=name,
        role=role,
        latitude=latitude,
        longitude=longitude,
    )


def decode_trace(packet: bytes) -> TracePacket | None:
    summary = describe_packet(packet)
    if summary.packet_type != TYPE_TRACE or len(summary.payload) < 9:
        return None

    tag = struct.unpack("<I", summary.payload[:4])[0]
    auth_code = struct.unpack("<I", summary.payload[4:8])[0]
    flags = summary.payload[8]
    path_hash_size = 1 << (flags & 0x03)
    tail = summary.payload[9:]
    if path_hash_size <= 0 or len(tail) % (path_hash_size + 1) != 0:
        return None

    count = len(tail) // (path_hash_size + 1)
    snr_raw = tail[:count]
    hash_blob = tail[count:]
    path_hashes = [hash_blob[i:i + path_hash_size].hex().upper() for i in range(0, len(hash_blob), path_hash_size)]
    path_snrs = [struct.unpack("b", bytes([item]))[0] / 4.0 for item in snr_raw]
    return TracePacket(tag=tag, auth_code=auth_code, flags=flags, path_hashes=path_hashes, path_snrs=path_snrs)


def split_sender_and_content(text: str) -> tuple[str, str]:
    if ": " in text:
        sender, content = text.split(": ", 1)
        return sender.strip(), content.strip()
    return "unknown", text.strip()


def calculate_distance_km(origin: tuple[float, float] | None, target: tuple[float, float] | None) -> float | None:
    if origin is None or target is None:
        return None
    lat1, lon1 = origin
    lat2, lon2 = target

    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c