from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from .config import AppConfig, EndpointConfig
from .tcp_client import MeshcoreTCPClient, ReceivedPacket


@dataclass(slots=True)
class _EndpointRuntime:
    endpoint: EndpointConfig
    client: MeshcoreTCPClient | None = None
    connected_event: asyncio.Event | None = None
    send_lock: asyncio.Lock | None = None
    send_queue: asyncio.PriorityQueue[tuple[int, int, bytes, str, asyncio.Future[str]]] | None = None
    send_order: int = 0
    quiet_until_monotonic: float = 0.0


class BridgeGatewayService:
    RECEIVE_IDLE_TIMEOUT_SECS = 60.0

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(f"{config.service.name}.bridge_gateway")
        self._stop_event = asyncio.Event()
        self._control_server: asyncio.AbstractServer | None = None
        self._event_server: asyncio.AbstractServer | None = None
        self._subscribers: set[asyncio.StreamWriter] = set()
        self._subscribers_lock = asyncio.Lock()
        self._endpoint_runtimes = {
            endpoint.name: _EndpointRuntime(
                endpoint=endpoint,
                connected_event=asyncio.Event(),
                send_lock=asyncio.Lock(),
                send_queue=asyncio.PriorityQueue(),
            )
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
        self._tasks.extend(
            asyncio.create_task(self._run_sender(runtime), name=f"bridge-gateway-sender:{runtime.endpoint.name}")
            for runtime in self._endpoint_runtimes.values()
        )
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
                    try:
                        packet = await client.receive_packet(timeout=self.RECEIVE_IDLE_TIMEOUT_SECS)
                    except asyncio.TimeoutError:
                        watchdog_reason = await self._watchdog_reason(endpoint, client)
                        if watchdog_reason is not None:
                            raise ConnectionError(watchdog_reason)
                        continue
                    await self._broadcast_packet(endpoint.name, packet)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("gateway endpoint %s failed: %s", endpoint.name, exc)
                await asyncio.sleep(3.0)
            finally:
                runtime.connected_event.clear()
                runtime.client = None
                await self._close_tcp_client(client)

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
        endpoint_name = str(payload.get("endpoint_name") or "")
        runtime = self._endpoint_runtimes.get(endpoint_name)
        if runtime is None or runtime.connected_event is None or runtime.send_lock is None:
            return {"ok": False, "error": f"unknown endpoint {endpoint_name}"}
        if command == "set_quiet_window":
            seconds = max(0.0, float(payload.get("seconds") or 0.0))
            runtime.quiet_until_monotonic = max(runtime.quiet_until_monotonic, time.monotonic() + seconds)
            self.logger.info(
                "[GATEWAY-QUIET] endpoint=%s quiet_for=%.2fs until=%.3f requested_by=%s",
                endpoint_name,
                seconds,
                runtime.quiet_until_monotonic,
                str(payload.get("traffic_class") or "unknown"),
            )
            return {"ok": True}
        if command != "send_packet":
            return {"ok": False, "error": f"unsupported command {command}"}
        traffic_class = str(payload.get("traffic_class") or "default")
        try:
            await asyncio.wait_for(runtime.connected_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return {"ok": False, "error": f"endpoint {endpoint_name} is not connected"}
        if runtime.client is None:
            return {"ok": False, "error": f"endpoint {endpoint_name} has no active client"}
        try:
            packet = bytes.fromhex(str(payload["packet_hex"]))
            frame_hex = await self._enqueue_send_request(runtime, packet=packet, traffic_class=traffic_class)
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        self.logger.info(
            "[GATEWAY-TX] endpoint=%s traffic_class=%s host=%s port=%s packet=%s frame=%s",
            endpoint_name,
            traffic_class,
            runtime.endpoint.raw_host,
            runtime.endpoint.raw_port,
            packet.hex().upper(),
            frame_hex,
        )
        return {"ok": True, "frame_hex": frame_hex}

    async def _run_sender(self, runtime: _EndpointRuntime) -> None:
        assert runtime.connected_event is not None
        assert runtime.send_lock is not None
        assert runtime.send_queue is not None
        while not self._stop_event.is_set():
            try:
                _, _, packet, traffic_class, result_future = await runtime.send_queue.get()
            except asyncio.CancelledError:
                raise
            try:
                if traffic_class != "bot":
                    remaining = runtime.quiet_until_monotonic - time.monotonic()
                    if remaining > 0:
                        self.logger.info(
                            "[GATEWAY-DEFER] endpoint=%s traffic_class=%s sleep=%.2fs",
                            runtime.endpoint.name,
                            traffic_class,
                            remaining,
                        )
                        await asyncio.sleep(min(remaining, 0.5))
                        await self._requeue_send_request(runtime, packet=packet, traffic_class=traffic_class, result_future=result_future)
                        continue
                try:
                    await asyncio.wait_for(runtime.connected_event.wait(), timeout=5.0)
                except asyncio.TimeoutError as exc:
                    raise RuntimeError(f"endpoint {runtime.endpoint.name} is not connected") from exc
                if runtime.client is None:
                    raise RuntimeError(f"endpoint {runtime.endpoint.name} has no active client")
                async with runtime.send_lock:
                    frame_hex = await runtime.client.send_packet(packet)
            except asyncio.CancelledError:
                if not result_future.done():
                    result_future.cancel()
                raise
            except Exception as exc:
                if not result_future.done():
                    result_future.set_exception(exc)
            else:
                if not result_future.done():
                    result_future.set_result(frame_hex)

    async def _enqueue_send_request(self, runtime: _EndpointRuntime, *, packet: bytes, traffic_class: str) -> str:
        assert runtime.send_queue is not None
        loop = asyncio.get_running_loop()
        result_future: asyncio.Future[str] = loop.create_future()
        await self._requeue_send_request(runtime, packet=packet, traffic_class=traffic_class, result_future=result_future)
        return await result_future

    async def _requeue_send_request(
        self,
        runtime: _EndpointRuntime,
        *,
        packet: bytes,
        traffic_class: str,
        result_future: asyncio.Future[str],
    ) -> None:
        assert runtime.send_queue is not None
        priority = self._traffic_class_priority(traffic_class)
        order = runtime.send_order
        runtime.send_order += 1
        await runtime.send_queue.put((priority, order, packet, traffic_class, result_future))

    def _traffic_class_priority(self, traffic_class: str) -> int:
        normalized = traffic_class.strip().lower()
        if normalized == "bot":
            return 0
        if normalized == "probe":
            return 20
        return 10

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

    async def _watchdog_reason(self, endpoint: EndpointConfig, client: MeshcoreTCPClient) -> str | None:
        watchdog_secs = self.config.gateway.traffic_watchdog_secs
        if watchdog_secs <= 0:
            return None
        seconds_since_rx = self._seconds_since(client, "seconds_since_last_rx")
        seconds_since_activity = self._seconds_since(client, "seconds_since_last_activity")
        if seconds_since_rx is None and seconds_since_activity is None:
            return None
        if (
            (seconds_since_rx is not None and seconds_since_rx >= watchdog_secs)
            or (seconds_since_activity is not None and seconds_since_activity >= watchdog_secs)
        ):
            console_status = await self._probe_console_mirror(endpoint)
            return (
                "traffic watchdog fired "
                f"after activity_idle={self._format_idle_seconds(seconds_since_activity)} "
                f"rx_idle={self._format_idle_seconds(seconds_since_rx)} "
                f"mirror={console_status}"
            )
        return None

    async def _probe_console_mirror(self, endpoint: EndpointConfig) -> str:
        port = endpoint.console_mirror_port
        if port is None:
            return "not-configured"
        host = endpoint.console_mirror_host or endpoint.raw_host
        timeout = self.config.gateway.console_probe_timeout_secs
        try:
            if timeout > 0:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
            else:
                reader, writer = await asyncio.open_connection(host, port)
        except Exception as exc:
            return f"connect-failed:{exc}"
        try:
            if timeout <= 0:
                return "connected"
            try:
                data = await asyncio.wait_for(reader.read(1), timeout=timeout)
            except asyncio.TimeoutError:
                return "connected-idle"
            return "connected-data" if data else "connected-eof"
        finally:
            await self._close_stream_writer(writer)

    async def _close_tcp_client(self, client: MeshcoreTCPClient) -> None:
        close = getattr(client, "close")
        try:
            await close(timeout=self.config.gateway.close_timeout_secs)
        except TypeError:
            await close()

    async def _close_stream_writer(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        try:
            timeout = self.config.gateway.close_timeout_secs
            if timeout > 0:
                await asyncio.wait_for(writer.wait_closed(), timeout=timeout)
            else:
                await writer.wait_closed()
        except Exception:
            transport = getattr(writer, "transport", None)
            if transport is not None:
                transport.abort()

    def _seconds_since(self, client: object, attr_name: str) -> float | None:
        method = getattr(client, attr_name, None)
        if method is None:
            return None
        value = method()
        if value is None:
            return None
        return float(value)

    def _format_idle_seconds(self, value: float | None) -> str:
        if value is None:
            return "n/a"
        return f"{value:.1f}s"