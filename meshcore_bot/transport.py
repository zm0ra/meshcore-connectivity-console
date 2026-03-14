from __future__ import annotations

from typing import Protocol

from .tcp_client import ReceivedPacket


class PacketTransportClient(Protocol):
    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def send_packet(self, packet: bytes) -> str: ...

    async def receive_packet(self, *, timeout: float) -> ReceivedPacket: ...