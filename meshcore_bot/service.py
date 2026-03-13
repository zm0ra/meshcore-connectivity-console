from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import logging
from pathlib import Path
from typing import Any

from .config import AppConfig, EndpointConfig
from .database import BotDatabase
from .packets import ChannelDefinition, describe_packet, try_decode_group_text
from .protocol import DecodedPayload, RS232BridgeDecoder, encode_frame


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _split_sender_and_content(text: str) -> tuple[str, str]:
    if ": " in text:
        sender, content = text.split(": ", 1)
        return sender.strip() or "unknown", content.strip()
    return "unknown", text.strip()


@dataclass(slots=True)
class EndpointSession:
    config: EndpointConfig
    decoder: RS232BridgeDecoder
    connected: bool = False
    last_error: str | None = None
    last_seen_at: datetime | None = None
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None


class MeshcoreRuntimeService:
    def __init__(self, config: AppConfig, database: BotDatabase, log_file_path: Path) -> None:
        self.config = config
        self.database = database
        self.log_file_path = log_file_path
        self.logger = logging.getLogger(config.service.name)
        self.started_at = _utc_now()
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._seen_packet_ids: dict[str, datetime] = {}
        self._sessions = {
            endpoint.name: EndpointSession(config=endpoint, decoder=RS232BridgeDecoder(max_payload_len=255))
            for endpoint in config.endpoints
            if endpoint.enabled
        }
        self._channel_defs = tuple(
            ChannelDefinition(name=item.name, psk=item.psk, listen=item.listen)
            for item in config.channels
        )
        self._listen_channels = {item.name.lower() for item in config.channels if item.listen}
        self.total_packets_seen = 0
        self.total_group_text_seen = 0
        self.total_group_text_decoded = 0
        self.last_packet_summary: dict[str, Any] | None = None
        self.last_drop_reason: str | None = None

    async def start(self) -> None:
        self.database.bootstrap_from_config(self.config)
        for session in self._sessions.values():
            self._tasks.append(asyncio.create_task(self._run_endpoint(session), name=f"endpoint:{session.config.name}"))

    async def stop(self) -> None:
        self._stop_event.set()
        for session in self._sessions.values():
            if session.writer is not None:
                session.writer.close()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def send_channel_message(self, endpoint_name: str, channel_name: str, message: str) -> None:
        session = self._sessions.get(endpoint_name)
        if session is None or session.writer is None:
            raise RuntimeError(f"endpoint {endpoint_name} is not connected")
        channel = next((item for item in self._channel_defs if item.name == channel_name.lower()), None)
        if channel is None:
            raise RuntimeError(f"unknown channel {channel_name}")

        from .packets import build_group_text_packet

        payload = build_group_text_packet(
            self.database.get_bot_runtime_settings().get("name", self.config.bot.name),
            message,
            channel_name=channel.name,
            channel_psk=channel.psk,
        )
        session.writer.write(encode_frame(payload))
        await session.writer.drain()
        self.logger.info("tx %s #%s: %s", endpoint_name, channel.name, message)

    def snapshot(self) -> dict[str, Any]:
        endpoint_state = {item["name"]: item for item in self.database.list_endpoints()}
        for runtime in self.database.list_endpoint_runtime_states():
            merged = endpoint_state.setdefault(runtime["endpoint_name"], {"name": runtime["endpoint_name"]})
            merged.update(runtime)

        return {
            "started_at": self.started_at.isoformat(),
            "bot": self.database.get_bot_runtime_settings(),
            "endpoints": endpoint_state,
            "messages": self.database.recent_messages(limit=50),
            "packets": self.database.recent_radio_packets(limit=50),
            "diagnostics": {
                "total_packets_seen": self.total_packets_seen,
                "total_group_text_seen": self.total_group_text_seen,
                "total_group_text_decoded": self.total_group_text_decoded,
                "last_packet_summary": self.last_packet_summary,
                "last_drop_reason": self.last_drop_reason,
            },
            "persistence": self.database.snapshot_overview(),
        }

    async def _run_endpoint(self, session: EndpointSession) -> None:
        while not self._stop_event.is_set():
            try:
                reader, writer = await asyncio.open_connection(session.config.raw_host, session.config.raw_port)
                session.reader = reader
                session.writer = writer
                session.connected = True
                session.last_error = None
                self.database.upsert_endpoint_runtime_state(
                    endpoint_name=session.config.name,
                    connected=True,
                    last_connect_at=_utc_now().isoformat(),
                    last_error=None,
                )
                self.logger.info("connected raw endpoint %s %s:%s", session.config.name, session.config.raw_host, session.config.raw_port)

                while not self._stop_event.is_set():
                    chunk = await reader.read(4096)
                    if not chunk:
                        raise ConnectionError("connection closed by peer")
                    now = _utc_now()
                    session.last_seen_at = now
                    self.database.upsert_endpoint_runtime_state(
                        endpoint_name=session.config.name,
                        connected=True,
                        last_seen_at=now.isoformat(),
                    )
                    for decoded in session.decoder.feed(chunk):
                        await self._handle_decoded_payload(session, decoded)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                session.connected = False
                session.last_error = str(exc)
                self.database.upsert_endpoint_runtime_state(
                    endpoint_name=session.config.name,
                    connected=False,
                    last_disconnect_at=_utc_now().isoformat(),
                    last_error=str(exc),
                )
                self.logger.warning("endpoint %s error: %s", session.config.name, exc)
                await asyncio.sleep(3)
            finally:
                if session.writer is not None:
                    session.writer.close()
                    try:
                        await session.writer.wait_closed()
                    except Exception:
                        pass
                session.reader = None
                session.writer = None

    async def _handle_decoded_payload(self, session: EndpointSession, decoded: DecodedPayload) -> None:
        observed_at = _utc_now()
        try:
            summary = describe_packet(decoded.payload)
        except Exception as exc:
            self.database.record_radio_packet(
                endpoint_name=session.config.name,
                direction="rx",
                observed_at=observed_at,
                frame_valid=True,
                raw_frame_hex=encode_frame(decoded.payload, append_newline=False).hex(),
                payload_hex=decoded.payload.hex(),
                notes=f"parse error: {exc}",
            )
            return

        self.database.record_radio_packet(
            endpoint_name=session.config.name,
            direction="rx",
            observed_at=observed_at,
            frame_valid=True,
            route_name=summary.route_name,
            packet_type=summary.packet_type_name,
            path_len=summary.path_len,
            transport_codes=summary.transport_codes,
            raw_frame_hex=encode_frame(decoded.payload, append_newline=False).hex(),
            payload_hex=decoded.payload.hex(),
            source_kind="rs232bridge",
        )

        if self._is_duplicate_summary(summary):
            self.last_drop_reason = "duplicate message copy ignored"
            return

        self.total_packets_seen += 1
        self.last_packet_summary = {
            "endpoint": session.config.name,
            "route": summary.route_name,
            "packet_type": summary.packet_type_name,
            "path_len": summary.path_len,
            "payload_len": len(summary.payload),
            "transport_codes": summary.transport_codes,
        }

        decoded_group = try_decode_group_text(decoded.payload, self._channel_defs)
        if decoded_group is None:
            if summary.packet_type_name == "GRP_TXT":
                self.total_group_text_seen += 1
                self.last_drop_reason = "group text did not match configured channel definitions or failed decrypt"
            return

        self.total_group_text_seen += 1
        self.total_group_text_decoded += 1
        channel, group_message = decoded_group
        if channel.name.lower() not in self._listen_channels:
            self.last_drop_reason = f"decoded channel '{channel.name}' is not enabled for listening"
            return

        sender, content = _split_sender_and_content(group_message.text)
        self.database.record_message(
            endpoint_name=session.config.name,
            channel_name=channel.name,
            sender=sender,
            sender_identity_hex=None,
            content=content,
            packet_type=summary.packet_type_name,
            route_name=summary.route_name,
            path_len=summary.path_len,
            received_at=observed_at,
            raw_payload_hex=decoded.payload.hex(),
            source_kind="rs232bridge",
        )
        self.last_drop_reason = None
        self.logger.info("rx %s #%s %s: %s", session.config.name, channel.name, sender, content)

    def _is_duplicate_summary(self, summary: Any) -> bool:
        now = _utc_now()
        cutoff_seconds = 30
        expired = [
            key for key, seen_at in self._seen_packet_ids.items()
            if (now - seen_at).total_seconds() > cutoff_seconds
        ]
        for key in expired:
            self._seen_packet_ids.pop(key, None)

        dedupe_material = bytes([summary.packet_type]) + summary.payload
        payload_id = hashlib.sha256(dedupe_material).hexdigest()
        if payload_id in self._seen_packet_ids:
            return True
        self._seen_packet_ids[payload_id] = now
        return False