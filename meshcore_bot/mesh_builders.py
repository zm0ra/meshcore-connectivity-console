from __future__ import annotations

import struct
import time
from dataclasses import dataclass

from .channels import channel_hash
from .identity import LocalIdentity
from .mesh_crypto import encrypt_then_mac, mac_then_decrypt
from .mesh_packets import ADV_FEAT1_MASK, ADV_FEAT2_MASK, ADV_LATLON_MASK, ADV_NAME_MASK, PacketSummary, PayloadType, RouteType, parse_packet


PATH_HASH_SIZE = 1
MAX_PATH_SIZE = 64
_last_wire_timestamp = 0


@dataclass(slots=True)
class PacketEnvelope:
    packet: bytes
    summary: PacketSummary


@dataclass(slots=True)
class PathResponse:
    encoded_path_len: int
    path_bytes: bytes
    extra_type: int
    extra_payload: bytes
    source_hash: bytes
    destination_hash: bytes


@dataclass(slots=True)
class DecryptedDatagram:
    destination_hash: bytes
    source_hash: bytes
    plaintext: bytes


@dataclass(slots=True)
class GroupText:
    channel_hash: int
    timestamp: int
    text_type: int
    attempt: int
    text: str


class DatagramParseError(ValueError):
    pass


def next_wire_timestamp(timestamp: int | None = None) -> int:
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


def _build_advert_app_data(
    *,
    name: str | None,
    latitude: float | None,
    longitude: float | None,
    advert_type: int,
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


def encode_path_len(path_hash_size: int, path_hash_count: int) -> int:
    if path_hash_size < 1 or path_hash_size > 3:
        raise ValueError("path_hash_size must be 1..3")
    if path_hash_count < 0 or path_hash_count > 63:
        raise ValueError("path_hash_count must be 0..63")
    if path_hash_count * path_hash_size > MAX_PATH_SIZE:
        raise ValueError("encoded path would exceed MAX_PATH_SIZE")
    return ((path_hash_size - 1) << 6) | path_hash_count


def build_mesh_packet(
    *,
    route_type: RouteType,
    payload_type: PayloadType,
    payload: bytes,
    encoded_path_len: int = 0,
    path_bytes: bytes = b"",
    transport_codes: tuple[int, int] | None = None,
) -> PacketEnvelope:
    header = ((int(payload_type) & 0x0F) << 2) | (int(route_type) & 0x03)
    data = bytearray([header])
    if route_type in (RouteType.TRANSPORT_FLOOD, RouteType.TRANSPORT_DIRECT):
        if transport_codes is None:
            raise ValueError("transport_codes required for transport route type")
        data.extend(struct.pack("<HH", *transport_codes))
    data.append(encoded_path_len)
    data.extend(path_bytes)
    data.extend(payload)
    packet = bytes(data)
    return PacketEnvelope(packet=packet, summary=parse_packet(packet))


def build_datagram_payload(
    *,
    destination_public_key: bytes,
    source_identity: LocalIdentity,
    shared_secret: bytes,
    plaintext: bytes,
) -> bytes:
    payload = bytearray()
    payload.extend(destination_public_key[:PATH_HASH_SIZE])
    payload.extend(source_identity.public_hash(PATH_HASH_SIZE))
    payload.extend(encrypt_then_mac(shared_secret, plaintext))
    return bytes(payload)


def build_anon_request_payload(
    *,
    destination_public_key: bytes,
    source_identity: LocalIdentity,
    shared_secret: bytes,
    plaintext: bytes,
) -> bytes:
    payload = bytearray()
    payload.extend(destination_public_key[:PATH_HASH_SIZE])
    payload.extend(source_identity.public_key)
    payload.extend(encrypt_then_mac(shared_secret, plaintext))
    return bytes(payload)


def build_login_packet(
    *,
    identity: LocalIdentity,
    remote_public_key: bytes,
    guest_password: str,
    encoded_path_len: int = 0,
    path_bytes: bytes = b"",
) -> PacketEnvelope:
    shared_secret = identity.calc_shared_secret(remote_public_key)
    timestamp = next_wire_timestamp()
    plaintext = struct.pack("<I", timestamp) + guest_password.encode("utf-8")
    payload = build_anon_request_payload(
        destination_public_key=remote_public_key,
        source_identity=identity,
        shared_secret=shared_secret,
        plaintext=plaintext,
    )
    route_type = RouteType.DIRECT if encoded_path_len else RouteType.FLOOD
    return build_mesh_packet(
        route_type=route_type,
        payload_type=PayloadType.ANON_REQ,
        payload=payload,
        encoded_path_len=encoded_path_len,
        path_bytes=path_bytes,
    )


def build_advert_packet(
    *,
    identity: LocalIdentity,
    name: str,
    latitude: float | None = None,
    longitude: float | None = None,
    advert_type: int = 1,
    route_type: RouteType = RouteType.DIRECT,
    timestamp: int | None = None,
) -> PacketEnvelope:
    emitted_timestamp = next_wire_timestamp(timestamp)
    app_data = _build_advert_app_data(
        name=name,
        latitude=latitude,
        longitude=longitude,
        advert_type=advert_type,
    )
    signed_message = identity.public_key + struct.pack("<I", emitted_timestamp) + app_data
    signature = identity.sign(signed_message)
    payload = signed_message[:36] + signature + app_data
    return build_mesh_packet(route_type=route_type, payload_type=PayloadType.ADVERT, payload=payload)


def build_request_packet(
    *,
    identity: LocalIdentity,
    remote_public_key: bytes,
    plaintext: bytes,
    encoded_path_len: int,
    path_bytes: bytes,
) -> PacketEnvelope:
    shared_secret = identity.calc_shared_secret(remote_public_key)
    payload = build_datagram_payload(
        destination_public_key=remote_public_key,
        source_identity=identity,
        shared_secret=shared_secret,
        plaintext=plaintext,
    )
    route_type = RouteType.DIRECT if encoded_path_len else RouteType.FLOOD
    return build_mesh_packet(
        route_type=route_type,
        payload_type=PayloadType.REQ,
        payload=payload,
        encoded_path_len=encoded_path_len,
        path_bytes=path_bytes,
    )


def build_private_text_packet(
    *,
    identity: LocalIdentity,
    remote_public_key: bytes,
    message: str,
    timestamp: int | None = None,
    attempt: int = 0,
    encoded_path_len: int = 0,
    path_bytes: bytes = b"",
) -> PacketEnvelope:
    shared_secret = identity.calc_shared_secret(remote_public_key)
    wire_timestamp = next_wire_timestamp(timestamp)
    flags = attempt & 0x03
    plaintext = struct.pack("<IB", wire_timestamp, flags) + message.encode("utf-8")
    payload = build_datagram_payload(
        destination_public_key=remote_public_key,
        source_identity=identity,
        shared_secret=shared_secret,
        plaintext=plaintext,
    )
    route_type = RouteType.DIRECT if encoded_path_len else RouteType.FLOOD
    return build_mesh_packet(
        route_type=route_type,
        payload_type=PayloadType.TXT_MSG,
        payload=payload,
        encoded_path_len=encoded_path_len,
        path_bytes=path_bytes,
    )


def build_group_text_packet(
    *,
    sender_name: str,
    message: str,
    channel_secret: bytes,
    timestamp: int | None = None,
    attempt: int = 0,
) -> PacketEnvelope:
    wire_timestamp = next_wire_timestamp(timestamp)
    flags = attempt & 0x03
    plaintext = struct.pack("<IB", wire_timestamp, flags) + f"{sender_name}: {message}".encode("utf-8")
    payload = bytes([channel_hash(channel_secret)]) + encrypt_then_mac(channel_secret, plaintext)
    return build_mesh_packet(
        route_type=RouteType.FLOOD,
        payload_type=PayloadType.GRP_TXT,
        payload=payload,
    )


def parse_text_plaintext(plaintext: bytes) -> tuple[int, int, int, str] | None:
    if len(plaintext) < 5:
        return None
    timestamp = struct.unpack_from("<I", plaintext, 0)[0]
    flags = plaintext[4]
    text_type = (flags >> 2) & 0x3F
    attempt = flags & 0x03
    text = plaintext[5:].decode("utf-8", errors="replace").rstrip("\x00")
    return timestamp, text_type, attempt, text


def parse_group_text(summary: PacketSummary, *, channel_secret: bytes) -> GroupText | None:
    if summary.payload_type is not PayloadType.GRP_TXT or len(summary.payload) < 3:
        return None
    expected_channel_hash = channel_hash(channel_secret)
    if summary.payload[0] != expected_channel_hash:
        return None
    try:
        plaintext = mac_then_decrypt(channel_secret, summary.payload[1:])
    except ValueError:
        return None
    parsed = parse_text_plaintext(plaintext)
    if parsed is None:
        return None
    timestamp, text_type, attempt, text = parsed
    return GroupText(
        channel_hash=expected_channel_hash,
        timestamp=timestamp,
        text_type=text_type,
        attempt=attempt,
        text=text,
    )


def split_sender_and_message(text: str) -> tuple[str | None, str]:
    sender, separator, message = text.partition(":")
    if not separator:
        return None, text.strip()
    sender_name = sender.strip() or None
    return sender_name, message.strip()


def parse_encrypted_datagram(summary: PacketSummary, *, shared_secret: bytes) -> DecryptedDatagram:
    if summary.payload_type not in {PayloadType.REQ, PayloadType.RESPONSE, PayloadType.TXT_MSG, PayloadType.PATH}:
        raise DatagramParseError("unsupported payload type for encrypted datagram")
    if len(summary.payload) < 1 + 1 + 2:
        raise DatagramParseError("payload too short for datagram")
    destination_hash = summary.payload[:1]
    source_hash = summary.payload[1:2]
    plaintext = mac_then_decrypt(shared_secret, summary.payload[2:])
    return DecryptedDatagram(destination_hash=destination_hash, source_hash=source_hash, plaintext=plaintext)


def parse_anon_request(summary: PacketSummary, *, shared_secret: bytes) -> tuple[bytes, bytes, bytes]:
    if summary.payload_type is not PayloadType.ANON_REQ:
        raise DatagramParseError("not an anonymous request")
    if len(summary.payload) < 1 + 32 + 2:
        raise DatagramParseError("anonymous request too short")
    destination_hash = summary.payload[:1]
    sender_public_key = summary.payload[1:33]
    plaintext = mac_then_decrypt(shared_secret, summary.payload[33:])
    return destination_hash, sender_public_key, plaintext


def parse_path_response(summary: PacketSummary, *, shared_secret: bytes) -> PathResponse:
    decrypted = parse_encrypted_datagram(summary, shared_secret=shared_secret)
    plaintext = decrypted.plaintext
    if len(plaintext) < 6:
        raise DatagramParseError("path response plaintext too short")
    encoded_path_len = plaintext[0]
    path_hash_size = (encoded_path_len >> 6) + 1
    path_hash_count = encoded_path_len & 0x3F
    path_byte_len = path_hash_size * path_hash_count
    if len(plaintext) < 1 + path_byte_len + 1:
        raise DatagramParseError("truncated path response")
    path_bytes = plaintext[1 : 1 + path_byte_len]
    extra_type = plaintext[1 + path_byte_len]
    extra_payload = plaintext[2 + path_byte_len :]
    return PathResponse(
        encoded_path_len=encoded_path_len,
        path_bytes=path_bytes,
        extra_type=extra_type,
        extra_payload=extra_payload,
        source_hash=decrypted.source_hash,
        destination_hash=decrypted.destination_hash,
    )


def next_request_tag() -> int:
    return next_wire_timestamp()
