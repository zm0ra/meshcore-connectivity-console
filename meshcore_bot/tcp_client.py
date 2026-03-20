from __future__ import annotations

import asyncio
import logging
import time
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
        self.logger = logging.getLogger("meshcore-bot.tcp_client")
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._decoder = RS232BridgeDecoder(max_payload_len=255)
        self._packets: asyncio.Queue[ReceivedPacket | Exception] = asyncio.Queue()
        self._reader_task: asyncio.Task[Any] | None = None
        self._closed = False
        self._read_failure: Exception | None = None
        self._last_activity_monotonic: float | None = None
        self._last_rx_monotonic: float | None = None

    async def connect(self) -> None:
        self._closed = False
        self._read_failure = None
        self._packets = asyncio.Queue()
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        now = time.monotonic()
        self._last_activity_monotonic = now
        self._last_rx_monotonic = now
        self._reader_task = asyncio.create_task(self._read_loop(), name=f"meshcore-tcp:{self.host}:{self.port}")

    async def close(self, *, timeout: float = 2.0) -> None:
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except BaseException:
                pass
            self._reader_task = None
        if self._writer is not None:
            writer = self._writer
            self._writer = None
            self._reader = None
            writer.close()
            try:
                if timeout > 0:
                    await asyncio.wait_for(writer.wait_closed(), timeout=timeout)
                else:
                    await writer.wait_closed()
            except asyncio.TimeoutError:
                self.logger.warning("TCP writer close timed out host=%s port=%s", self.host, self.port)
                self._abort_writer(writer)
            except Exception:
                self._abort_writer(writer)

    def seconds_since_last_activity(self) -> float | None:
        if self._last_activity_monotonic is None:
            return None
        return max(0.0, time.monotonic() - self._last_activity_monotonic)

    def seconds_since_last_rx(self) -> float | None:
        if self._last_rx_monotonic is None:
            return None
        return max(0.0, time.monotonic() - self._last_rx_monotonic)

    async def send_packet(self, packet: bytes) -> str:
        self._raise_if_reader_failed()
        if self._writer is None:
            raise RuntimeError("TCP client is not connected")
        frame = encode_frame(packet)
        try:
            self._writer.write(frame)
            await self._writer.drain()
        except Exception as exc:
            self._read_failure = exc
            raise
        self._last_activity_monotonic = time.monotonic()
        self.logger.info(
            "[TCP-TX] host=%s port=%s packet=%s frame=%s",
            self.host,
            self.port,
            packet.hex().upper(),
            frame.hex().upper(),
        )
        return frame.hex().upper()

    async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
        self._raise_if_reader_failed()
        result = await asyncio.wait_for(self._packets.get(), timeout=timeout)
        if isinstance(result, Exception):
            self._read_failure = result
            raise result
        return result

    def _raise_if_reader_failed(self) -> None:
        if self._read_failure is not None:
            raise self._read_failure

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
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
                    self._last_rx_monotonic = time.monotonic()
                    self._last_activity_monotonic = self._last_rx_monotonic
                    frame_hex = encode_frame(frame.payload, append_newline=False).hex().upper()
                    self.logger.info(
                        "[TCP-RX] host=%s port=%s packet=%s frame=%s",
                        self.host,
                        self.port,
                        frame.payload.hex().upper(),
                        frame_hex,
                    )
                    await self._packets.put(
                        ReceivedPacket(
                            observed_at=observed_at,
                            frame_hex=frame_hex,
                            packet_hex=frame.payload.hex().upper(),
                            summary=summary,
                        )
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._closed:
                return
            self._read_failure = exc
            self.logger.warning("TCP reader failed host=%s port=%s: %s", self.host, self.port, exc)
            await self._packets.put(exc)

    def _abort_writer(self, writer: asyncio.StreamWriter) -> None:
        transport = getattr(writer, "transport", None)
        if transport is not None:
            transport.abort()
