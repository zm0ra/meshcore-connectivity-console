from __future__ import annotations

import asyncio
import json
from pathlib import Path

from .mesh_packets import parse_packet
from .tcp_client import ReceivedPacket


class GatewayTransportClient:
    def __init__(
        self,
        *,
        endpoint_name: str,
        control_socket_path: str | Path,
        event_socket_path: str | Path,
        traffic_class: str = "default",
    ) -> None:
        self.endpoint_name = endpoint_name
        self.control_socket_path = str(control_socket_path)
        self.event_socket_path = str(event_socket_path)
        self.traffic_class = traffic_class
        self._control_reader: asyncio.StreamReader | None = None
        self._control_writer: asyncio.StreamWriter | None = None
        self._event_reader: asyncio.StreamReader | None = None
        self._event_writer: asyncio.StreamWriter | None = None
        self._command_lock = asyncio.Lock()

    async def connect(self) -> None:
        self._control_reader, self._control_writer = await asyncio.open_unix_connection(self.control_socket_path)
        self._event_reader, self._event_writer = await asyncio.open_unix_connection(self.event_socket_path)

    async def close(self) -> None:
        if self._control_writer is not None:
            self._control_writer.close()
            await self._control_writer.wait_closed()
            self._control_writer = None
            self._control_reader = None
        if self._event_writer is not None:
            self._event_writer.close()
            await self._event_writer.wait_closed()
            self._event_writer = None
            self._event_reader = None

    async def send_packet(self, packet: bytes) -> str:
        if self._control_reader is None or self._control_writer is None:
            raise RuntimeError("gateway control socket is not connected")
        message = {
            "command": "send_packet",
            "endpoint_name": self.endpoint_name,
            "packet_hex": packet.hex().upper(),
            "traffic_class": self.traffic_class,
        }
        async with self._command_lock:
            self._control_writer.write((json.dumps(message, ensure_ascii=True) + "\n").encode("ascii"))
            await self._control_writer.drain()
            response_line = await self._control_reader.readline()
        if not response_line:
            raise ConnectionError("gateway control socket closed")
        response = json.loads(response_line.decode("utf-8"))
        if not response.get("ok"):
            raise RuntimeError(str(response.get("error") or "gateway send failed"))
        return str(response["frame_hex"])

    async def activate_quiet_window(self, *, seconds: float) -> None:
        if self._control_reader is None or self._control_writer is None:
            raise RuntimeError("gateway control socket is not connected")
        message = {
            "command": "set_quiet_window",
            "endpoint_name": self.endpoint_name,
            "seconds": max(0.0, float(seconds)),
            "traffic_class": self.traffic_class,
        }
        async with self._command_lock:
            self._control_writer.write((json.dumps(message, ensure_ascii=True) + "\n").encode("ascii"))
            await self._control_writer.drain()
            response_line = await self._control_reader.readline()
        if not response_line:
            raise ConnectionError("gateway control socket closed")
        response = json.loads(response_line.decode("utf-8"))
        if not response.get("ok"):
            raise RuntimeError(str(response.get("error") or "gateway quiet window failed"))

    async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
        if self._event_reader is None:
            raise RuntimeError("gateway event socket is not connected")
        while True:
            line = await asyncio.wait_for(self._event_reader.readline(), timeout=timeout)
            if not line:
                raise ConnectionError("gateway event socket closed")
            event = json.loads(line.decode("utf-8"))
            if event.get("type") != "packet" or event.get("endpoint_name") != self.endpoint_name:
                continue
            packet_hex = str(event["packet_hex"])
            packet = bytes.fromhex(packet_hex)
            return ReceivedPacket(
                observed_at=str(event["observed_at"]),
                frame_hex=str(event["frame_hex"]),
                packet_hex=packet_hex,
                summary=parse_packet(packet),
            )