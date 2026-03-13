from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

from .config import AppConfig
from .database import BotDatabase


class BotApplication:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(config.service.name)
        self.started_at = datetime.now(tz=UTC)
        self._shutdown_event = asyncio.Event()
        self._http_server: asyncio.base_events.Server | None = None
        self.database = BotDatabase(config.storage.database_path)

    async def run(self) -> None:
        self._prepare_directories()
        self.database.bootstrap_from_config(self.config)
        self.logger.info("starting %s", self.config.service.name)

        self._http_server = await asyncio.start_server(
            self._handle_http_client,
            host=self.config.web.host,
            port=self.config.web.port,
        )

        sockets = self._http_server.sockets or []
        bound = ", ".join(str(sock.getsockname()) for sock in sockets)
        self.logger.info("http server listening on %s", bound)

        async with self._http_server:
            await self._shutdown_event.wait()

    async def shutdown(self) -> None:
        if self._http_server is not None:
            self._http_server.close()
            await self._http_server.wait_closed()
        self._shutdown_event.set()
        self.logger.info("shutdown complete")

    async def _handle_http_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return

            parts = request_line.decode("ascii", errors="replace").strip().split()
            if len(parts) != 3:
                await self._write_response(writer, 400, {"error": "bad request"})
                return

            method, path, _version = parts

            while True:
                header_line = await reader.readline()
                if not header_line or header_line in {b"\r\n", b"\n"}:
                    break

            if method != "GET":
                await self._write_response(writer, 405, {"error": "method not allowed"})
                return

            if path == "/healthz":
                await self._write_response(writer, 200, self._health_payload())
                return

            if path == "/":
                await self._write_response(writer, 200, self._root_payload())
                return

            await self._write_response(writer, 404, {"error": "not found"})
        finally:
            writer.close()
            await writer.wait_closed()

    async def _write_response(
        self,
        writer: asyncio.StreamWriter,
        status_code: int,
        payload: dict[str, object],
    ) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8") + b"\n"
        reason = {
            200: "OK",
            400: "Bad Request",
            404: "Not Found",
            405: "Method Not Allowed",
        }.get(status_code, "OK")
        headers = [
            f"HTTP/1.1 {status_code} {reason}",
            "Content-Type: application/json; charset=utf-8",
            f"Content-Length: {len(body)}",
            "Connection: close",
            "",
            "",
        ]
        writer.write("\r\n".join(headers).encode("ascii") + body)
        await writer.drain()

    def _prepare_directories(self) -> None:
        self.config.storage.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.storage.logs_dir.mkdir(parents=True, exist_ok=True)

    def _health_payload(self) -> dict[str, object]:
        return {
            "status": "ok",
            "service": self.config.service.name,
            "started_at": self.started_at.isoformat(),
            "database": self.database.snapshot_overview(),
        }

    def _root_payload(self) -> dict[str, object]:
        return {
            "service": self.config.service.name,
            "bot": {
                "name": self.config.bot.name,
                "reply_prefix": self.config.bot.reply_prefix,
                "command_prefix": self.config.bot.command_prefix,
            },
            "status": "bootstrap",
            "started_at": self.started_at.isoformat(),
            "http": {
                "healthz": "/healthz",
            },
            "persistence": self.database.snapshot_overview(),
            "next_steps": [
                "RS232Bridge transport client",
                "MeshCore packet codec",
                "runtime state model",
                "radio ingestion",
                "command execution",
            ],
        }