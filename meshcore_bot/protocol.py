from __future__ import annotations

import struct


MAGIC = b"\xC0\x3E"


def fletcher16(data: bytes) -> int:
    sum1 = 0
    sum2 = 0
    for byte in data:
        sum1 = (sum1 + byte) % 255
        sum2 = (sum2 + sum1) % 255
    return (sum2 << 8) | sum1


def encode_frame(payload: bytes, *, append_newline: bool = True) -> bytes:
    frame = MAGIC + struct.pack(">H", len(payload)) + payload + fletcher16(payload).to_bytes(2, "big")
    if append_newline:
        frame += b"\n"
    return frame