from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .mesh_packets import PacketParseError, PacketSummary, parse_packet
from .rs232 import RS232BridgeDecoder, encode_frame


@dataclass(slots=True)
class ReceivedPacket:
    observed_at: str
    frame_hex: str
    packet_hex: str
    summary: PacketSummary


class MeshcoreTCPClient:
    def __init__(self, host: str, port: int, *, read_size: int = 4096) -> None:
        self.host = host
        self.port = port
        self.read_size = read_size
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._decoder = RS232BridgeDecoder(max_payload_len=255)
        self._packets: asyncio.Queue[ReceivedPacket] = asyncio.Queue()
        self._reader_task: asyncio.Task[Any] | None = None
        self._closed = False

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        self._reader_task = asyncio.create_task(self._read_loop(), name=f"meshcore-tcp:{self.host}:{self.port}")

    async def close(self) -> None:
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except BaseException:
                pass
            self._reader_task = None
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None

    async def send_packet(self, packet: bytes) -> str:
        if self._writer is None:
            raise RuntimeError("TCP client is not connected")
        frame = encode_frame(packet)
        self._writer.write(frame)
        await self._writer.drain()
        return frame.hex().upper()

    async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
        return await asyncio.wait_for(self._packets.get(), timeout=timeout)

    async def _read_loop(self) -> None:
        assert self._reader is not None
        while not self._closed:
            chunk = await self._reader.read(self.read_size)
            if not chunk:
                raise ConnectionError("connection closed by peer")
            for frame in self._decoder.feed(chunk):
                try:
                    summary = parse_packet(frame.payload)
                except PacketParseError:
                    continue
                observed_at = datetime.now(tz=UTC).isoformat()
                await self._packets.put(
                    ReceivedPacket(
                        observed_at=observed_at,
                        frame_hex=encode_frame(frame.payload, append_newline=False).hex().upper(),
                        packet_hex=frame.payload.hex().upper(),
                        summary=summary,
                    )
                )
