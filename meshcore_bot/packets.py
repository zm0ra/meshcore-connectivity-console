from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import hmac
import struct
import time

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


ROUTE_FLOOD = 0x01
ROUTE_TRANSPORT_FLOOD = 0x00
ROUTE_DIRECT = 0x02
ROUTE_TRANSPORT_DIRECT = 0x03
TYPE_GRP_TXT = 0x05
TXT_TYPE_PLAIN = 0x00
CIPHER_KEY_SIZE = 16
PUB_KEY_SIZE = 32
CIPHER_MAC_SIZE = 2

_last_wire_timestamp = 0

ROUTE_NAMES = {
    ROUTE_TRANSPORT_FLOOD: "TRANSPORT_FLOOD",
    ROUTE_FLOOD: "FLOOD",
    ROUTE_DIRECT: "DIRECT",
    ROUTE_TRANSPORT_DIRECT: "TRANSPORT_DIRECT",
}

TYPE_NAMES = {
    TYPE_GRP_TXT: "GRP_TXT",
}


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
class ChannelDefinition:
    name: str
    psk: str | None = None
    listen: bool = True

    @property
    def secret(self) -> bytes:
        return resolve_channel_secret(self.name, self.psk)

    @property
    def channel_hash(self) -> int:
        return hashlib.sha256(self.secret).digest()[0]


@dataclass(slots=True)
class PublicGroupText:
    timestamp: int
    timestamp_iso: str
    txt_type: int
    text: str
    channel_hash: int


def build_hashtag_channel_psk(name: str) -> str:
    channel_name = name.strip().lower()
    if not channel_name:
        raise ValueError("channel name cannot be empty")
    if not channel_name.startswith("#"):
        channel_name = f"#{channel_name}"
    secret = hashlib.sha256(channel_name.encode("utf-8")).digest()[:CIPHER_KEY_SIZE]
    return base64.b64encode(secret).decode("ascii")


def resolve_channel_secret(name: str, psk: str | None) -> bytes:
    if psk:
        return base64.b64decode(psk)
    return base64.b64decode(build_hashtag_channel_psk(name))


def build_group_text_packet(
    sender_name: str,
    message: str,
    *,
    channel_name: str,
    channel_psk: str | None = None,
    timestamp: int | None = None,
) -> bytes:
    secret = resolve_channel_secret(channel_name, channel_psk)
    channel_hash = hashlib.sha256(secret).digest()[0]
    wire_timestamp = _next_wire_timestamp(timestamp)

    plaintext = bytearray()
    plaintext.extend(struct.pack("<I", wire_timestamp))
    plaintext.append(TXT_TYPE_PLAIN)
    plaintext.extend(f"{sender_name}: {message}".encode("utf-8"))

    encrypted = _encrypt_then_mac(secret, bytes(plaintext))
    payload = bytes([channel_hash]) + encrypted
    return _build_packet(TYPE_GRP_TXT, payload, route=ROUTE_FLOOD)


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


def _build_packet(packet_type: int, payload: bytes, *, route: int) -> bytes:
    return bytes([((packet_type & 0x0F) << 2) | (route & 0x03), 0x00]) + payload


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


def try_decode_group_text(
    packet: bytes,
    channels: list[ChannelDefinition] | tuple[ChannelDefinition, ...],
) -> tuple[ChannelDefinition, PublicGroupText] | None:
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
        timestamp, txt_type, text = parsed
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


def _encrypt_then_mac(secret: bytes, plaintext: bytes) -> bytes:
    encrypted = _encrypt_aes128(secret, plaintext)
    mac = hmac.new(secret[:PUB_KEY_SIZE], encrypted, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
    return mac + encrypted


def _encrypt_aes128(secret: bytes, plaintext: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(secret[:CIPHER_KEY_SIZE]), modes.ECB())
    encryptor = cipher.encryptor()
    return encryptor.update(_pad_to_block_size(plaintext)) + encryptor.finalize()


def _pad_to_block_size(data: bytes, block_size: int = 16) -> bytes:
    padding_len = (-len(data)) % block_size
    return data + (b"\x00" * padding_len)


def _decrypt_aes128(secret: bytes, ciphertext: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(secret[:CIPHER_KEY_SIZE]), modes.ECB())
    decryptor = cipher.decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()


def _decrypt_then_verify(secret: bytes, ciphertext_with_mac: bytes) -> bytes:
    if len(ciphertext_with_mac) < CIPHER_MAC_SIZE:
        raise ValueError("ciphertext too short")
    received_mac = ciphertext_with_mac[:CIPHER_MAC_SIZE]
    ciphertext = ciphertext_with_mac[CIPHER_MAC_SIZE:]
    expected_mac = hmac.new(secret[:PUB_KEY_SIZE], ciphertext, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
    if received_mac != expected_mac:
        raise ValueError("message MAC mismatch")
    return _decrypt_aes128(secret, ciphertext).rstrip(b"\x00")


def parse_text_plaintext(plaintext: bytes) -> tuple[int, int, str] | None:
    if len(plaintext) < 5:
        return None
    timestamp = struct.unpack_from("<I", plaintext, 0)[0]
    txt_type = plaintext[4]
    text = plaintext[5:].decode("utf-8", errors="replace")
    return timestamp, txt_type, text