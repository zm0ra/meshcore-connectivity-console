from __future__ import annotations

import base64
import hashlib
import hmac
import struct
import time

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


ROUTE_FLOOD = 0x01
TYPE_GRP_TXT = 0x05
TXT_TYPE_PLAIN = 0x00
CIPHER_KEY_SIZE = 16
PUB_KEY_SIZE = 32
CIPHER_MAC_SIZE = 2

_last_wire_timestamp = 0


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