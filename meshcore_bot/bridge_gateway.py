from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from .config import AppConfig, EndpointConfig
from .tcp_client import MeshcoreTCPClient, ReceivedPacket


@dataclass(slots=True)
class _EndpointRuntime:
    endpoint: EndpointConfig
    client: MeshcoreTCPClient | None = None
    connected_event: asyncio.Event | None = None


class BridgeGatewayService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(f"{config.service.name}.bridge_gateway")
        self._stop_event = asyncio.Event()
        self._control_server: asyncio.AbstractServer | None = None
        self._event_server: asyncio.AbstractServer | None = None
        self._subscribers: set[asyncio.StreamWriter] = set()
        self._subscribers_lock = asyncio.Lock()
        self._endpoint_runtimes = {
            endpoint.name: _EndpointRuntime(endpoint=endpoint, connected_event=asyncio.Event())
            for endpoint in config.endpoints
            if endpoint.enabled
        }
        self._tasks: list[asyncio.Task[None]] = []

    async def run(self) -> None:
        self._prepare_socket_path(self.config.gateway.control_socket_path)
        self._prepare_socket_path(self.config.gateway.event_socket_path)
        self._control_server = await asyncio.start_unix_server(
            self._handle_control_client,
            path=str(self.config.gateway.control_socket_path),
        )
        self._event_server = await asyncio.start_unix_server(
            self._handle_event_client,
            path=str(self.config.gateway.event_socket_path),
        )
        self._tasks = [
            asyncio.create_task(self._run_endpoint(runtime), name=f"bridge-gateway:{runtime.endpoint.name}")
            for runtime in self._endpoint_runtimes.values()
        ]
        try:
            await self._stop_event.wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []
        if self._control_server is not None:
            self._control_server.close()
            await self._control_server.wait_closed()
            self._control_server = None
        if self._event_server is not None:
            self._event_server.close()
            await self._event_server.wait_closed()
            self._event_server = None
        async with self._subscribers_lock:
            subscribers = list(self._subscribers)
            self._subscribers.clear()
        for writer in subscribers:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _run_endpoint(self, runtime: _EndpointRuntime) -> None:
        endpoint = runtime.endpoint
        assert runtime.connected_event is not None
        while not self._stop_event.is_set():
            client = MeshcoreTCPClient(endpoint.raw_host, endpoint.raw_port)
            try:
                await client.connect()
                runtime.client = client
                runtime.connected_event.set()
                self.logger.info("gateway connected to %s (%s:%s)", endpoint.name, endpoint.raw_host, endpoint.raw_port)
                while not self._stop_event.is_set():
                    packet = await client.receive_packet(timeout=60.0)
                    await self._broadcast_packet(endpoint.name, packet)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("gateway endpoint %s failed: %s", endpoint.name, exc)
                await asyncio.sleep(3.0)
            finally:
                runtime.connected_event.clear()
                runtime.client = None
                await client.close()

    async def _handle_control_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                response = await self._handle_control_message(line)
                writer.write((json.dumps(response, ensure_ascii=True) + "\n").encode("ascii"))
                await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _handle_control_message(self, line: bytes) -> dict[str, object]:
        try:
            payload = json.loads(line.decode("utf-8"))
        except Exception as exc:
            return {"ok": False, "error": f"invalid json: {exc}"}
        command = payload.get("command")
        if command != "send_packet":
            return {"ok": False, "error": f"unsupported command {command}"}
        endpoint_name = str(payload.get("endpoint_name") or "")
        runtime = self._endpoint_runtimes.get(endpoint_name)
        if runtime is None or runtime.connected_event is None:
            return {"ok": False, "error": f"unknown endpoint {endpoint_name}"}
        try:
            await asyncio.wait_for(runtime.connected_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return {"ok": False, "error": f"endpoint {endpoint_name} is not connected"}
        if runtime.client is None:
            return {"ok": False, "error": f"endpoint {endpoint_name} has no active client"}
        try:
            packet = bytes.fromhex(str(payload["packet_hex"]))
            frame_hex = await runtime.client.send_packet(packet)
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "frame_hex": frame_hex}

    async def _handle_event_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with self._subscribers_lock:
            self._subscribers.add(writer)
        try:
            await reader.read()
        finally:
            async with self._subscribers_lock:
                self._subscribers.discard(writer)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _broadcast_packet(self, endpoint_name: str, packet: ReceivedPacket) -> None:
        event = {
            "type": "packet",
            "endpoint_name": endpoint_name,
            "observed_at": packet.observed_at,
            "frame_hex": packet.frame_hex,
            "packet_hex": packet.packet_hex,
        }
        payload = (json.dumps(event, ensure_ascii=True) + "\n").encode("ascii")
        async with self._subscribers_lock:
            subscribers = list(self._subscribers)
        stale: list[asyncio.StreamWriter] = []
        for writer in subscribers:
            try:
                writer.write(payload)
                await writer.drain()
            except Exception:
                stale.append(writer)
        if stale:
            async with self._subscribers_lock:
                for writer in stale:
                    self._subscribers.discard(writer)

    def _prepare_socket_path(self, socket_path: Path) -> None:
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        if socket_path.exists():
            socket_path.unlink()