"""RS232Bridge framing used by the XIAO WiFi TCP wrapper."""

from __future__ import annotations

from dataclasses import dataclass
import struct

MAGIC = b"\xC0\x3E"
FRAME_DELIMITERS = {0x0A, 0x0D}


class ProtocolError(ValueError):
    """Raised when an RS232Bridge frame is malformed."""


def fletcher16(data: bytes) -> int:
    """Return the Fletcher-16 checksum as an integer."""
    sum1 = 0
    sum2 = 0
    for byte in data:
        sum1 = (sum1 + byte) % 255
        sum2 = (sum2 + sum1) % 255
    return (sum2 << 8) | sum1


def encode_frame(payload: bytes, append_newline: bool = True) -> bytes:
    """Wrap a serialized MeshCore packet in an RS232Bridge frame."""
    header = MAGIC + struct.pack(">H", len(payload))
    checksum = fletcher16(payload).to_bytes(2, "big")
    frame = header + payload + checksum
    if append_newline:
        frame += b"\n"
    return frame


def decode_frame(frame: bytes) -> bytes:
    """Validate a full RS232Bridge frame and return the payload."""
    if len(frame) < 6:
        raise ProtocolError("frame too short")
    if frame[:2] != MAGIC:
        raise ProtocolError("invalid magic")

    payload_len = struct.unpack(">H", frame[2:4])[0]
    needed = 4 + payload_len + 2
    if len(frame) not in (needed, needed + 1, needed + 2):
        raise ProtocolError("frame length mismatch")

    payload = frame[4:4 + payload_len]
    received = int.from_bytes(frame[4 + payload_len:needed], "big")
    expected = fletcher16(payload)
    if received != expected:
        raise ProtocolError(f"checksum mismatch: got 0x{received:04X}, want 0x{expected:04X}")

    trailing = frame[needed:]
    if any(byte not in FRAME_DELIMITERS for byte in trailing):
        raise ProtocolError("invalid trailing delimiter")
    return payload


@dataclass(slots=True)
class DecodedPayload:
    payload: bytes
    payload_len: int
    checksum: int


class RS232BridgeDecoder:
    """Incremental stream decoder with re-sync on the RS232Bridge magic bytes."""

    def __init__(self, max_payload_len: int = 512) -> None:
        self.max_payload_len = max_payload_len
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> list[DecodedPayload]:
        self._buffer.extend(chunk)
        decoded: list[DecodedPayload] = []

        while True:
            self._discard_leading_delimiters()
            if len(self._buffer) < 2:
                return decoded

            if self._buffer[:2] != MAGIC:
                if not self._resync_to_magic():
                    return decoded
                continue

            if len(self._buffer) < 4:
                return decoded

            payload_len = struct.unpack(">H", self._buffer[2:4])[0]
            if payload_len > self.max_payload_len:
                del self._buffer[0]
                continue

            needed = 4 + payload_len + 2
            if len(self._buffer) < needed:
                return decoded

            frame = bytes(self._buffer[:needed])
            del self._buffer[:needed]
            self._discard_leading_delimiters()

            try:
                payload = decode_frame(frame)
            except ProtocolError:
                continue

            decoded.append(
                DecodedPayload(
                    payload=payload,
                    payload_len=payload_len,
                    checksum=fletcher16(payload),
                )
            )

    def _discard_leading_delimiters(self) -> None:
        while self._buffer and self._buffer[0] in FRAME_DELIMITERS:
            del self._buffer[0]

    def _resync_to_magic(self) -> bool:
        pos = self._buffer.find(MAGIC, 1)
        if pos == -1:
            if self._buffer and self._buffer[-1] == MAGIC[0]:
                self._buffer[:] = self._buffer[-1:]
            else:
                self._buffer.clear()
            return False

        del self._buffer[:pos]
        return True