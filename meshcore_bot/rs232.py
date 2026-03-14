from __future__ import annotations

from dataclasses import dataclass
import struct


MAGIC = b"\xC0\x3E"
FRAME_DELIMITERS = {0x0A, 0x0D}


class ProtocolError(ValueError):
    pass


@dataclass(slots=True)
class DecodedFrame:
    payload: bytes
    payload_len: int
    checksum: int


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


def decode_frame(frame: bytes) -> DecodedFrame:
    if len(frame) < 6:
        raise ProtocolError("frame too short")
    if frame[:2] != MAGIC:
        raise ProtocolError("invalid magic")

    payload_len = struct.unpack(">H", frame[2:4])[0]
    needed = 4 + payload_len + 2
    if len(frame) < needed:
        raise ProtocolError("truncated frame")

    payload = frame[4 : 4 + payload_len]
    received = int.from_bytes(frame[4 + payload_len : needed], "big")
    expected = fletcher16(payload)
    if received != expected:
        raise ProtocolError("checksum mismatch")

    trailing = frame[needed:]
    if any(byte not in FRAME_DELIMITERS for byte in trailing):
        raise ProtocolError("invalid trailing delimiter")

    return DecodedFrame(payload=payload, payload_len=payload_len, checksum=expected)


class RS232BridgeDecoder:
    def __init__(self, max_payload_len: int = 255) -> None:
        self.max_payload_len = max_payload_len
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> list[DecodedFrame]:
        self._buffer.extend(chunk)
        frames: list[DecodedFrame] = []

        while True:
            self._discard_delimiters()
            if len(self._buffer) < 2:
                return frames

            if self._buffer[:2] != MAGIC:
                if not self._resync_to_magic():
                    return frames
                continue

            if len(self._buffer) < 4:
                return frames

            payload_len = struct.unpack(">H", self._buffer[2:4])[0]
            if payload_len > self.max_payload_len:
                del self._buffer[0]
                continue

            needed = 4 + payload_len + 2
            if len(self._buffer) < needed:
                return frames

            end = needed
            while end < len(self._buffer) and self._buffer[end] in FRAME_DELIMITERS:
                end += 1

            raw_frame = bytes(self._buffer[:end])
            del self._buffer[:end]

            try:
                frames.append(decode_frame(raw_frame))
            except ProtocolError:
                continue

    def _discard_delimiters(self) -> None:
        while self._buffer and self._buffer[0] in FRAME_DELIMITERS:
            del self._buffer[0]

    def _resync_to_magic(self) -> bool:
        pos = self._buffer.find(MAGIC, 1)
        if pos == -1:
            if self._buffer[-1:] == MAGIC[:1]:
                self._buffer[:] = self._buffer[-1:]
            else:
                self._buffer.clear()
            return False
        del self._buffer[:pos]
        return True
