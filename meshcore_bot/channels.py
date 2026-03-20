from __future__ import annotations

import base64
import hashlib

PUBLIC_CHANNEL_PSK_BASE64 = "izOH6cXN6mrJ5e26oRXNcg=="


def derive_hashtag_secret(name: str) -> bytes:
    channel_name = name.strip()
    if not channel_name:
        raise ValueError("channel name cannot be empty")
    if not channel_name.startswith("#"):
        channel_name = f"#{channel_name}"
    return hashlib.sha256(channel_name.encode("utf-8")).digest()[:16]


def hashtag_psk_base64(name: str) -> str:
    return base64.b64encode(derive_hashtag_secret(name)).decode("ascii")


def decode_psk(psk_base64: str) -> bytes:
    secret = base64.b64decode(psk_base64)
    if len(secret) not in {16, 32}:
        raise ValueError("channel PSK must decode to 16 or 32 bytes")
    return secret


def channel_hash(secret: bytes) -> int:
    if len(secret) not in {16, 32}:
        raise ValueError("channel secret must be 16 or 32 bytes")
    return hashlib.sha256(secret).digest()[0]
