from __future__ import annotations

import asyncio

from .packets import build_group_text_packet
from .protocol import encode_frame


class SerialTcpConnection:
    def __init__(self, host: str, port: int, *, timeout: float = 5.0) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.timeout,
        )

    async def close(self) -> None:
        if self._writer is None:
            return
        self._writer.close()
        await self._writer.wait_closed()
        self._reader = None
        self._writer = None

    async def send_channel_text(
        self,
        *,
        sender_name: str,
        channel_name: str,
        message: str,
        channel_psk: str | None = None,
    ) -> None:
        if self._writer is None:
            raise RuntimeError("serial@tcp connection is not open")
        payload = build_group_text_packet(
            sender_name,
            message,
            channel_name=channel_name,
            channel_psk=channel_psk,
        )
        self._writer.write(encode_frame(payload))
        await self._writer.drain()