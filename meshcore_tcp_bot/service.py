"""Async bot runtime and endpoint listeners."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
import logging
import socket
import struct
from typing import Any

from .config import AppConfig, EndpointConfig, ManagementNodeConfig
from .console import (
    ConsoleTelemetryBuffer,
    normalize_console_reply,
    parse_console_acl_reply,
    parse_console_neighbors_reply,
    parse_console_owner_reply,
)
from .database import MeshcoreStore
from .identity import MeshcoreIdentity, load_or_create_identity, save_identity
from .management import RepeaterManagementRegistry
from .models import MeshMessage, NodeRecord, RuntimeSnapshot
from .packets import (
    AclEntry,
    ADV_TYPE_CHAT,
    ChannelDefinition,
    PeerContact,
    REQ_TYPE_GET_ACCESS_LIST,
    REQ_TYPE_GET_STATUS,
    REQ_TYPE_GET_OWNER_INFO,
    ROUTE_DIRECT,
    ROUTE_FLOOD,
    TYPE_PATH,
    TYPE_RESPONSE,
    TYPE_TXT_MSG,
    TXT_TYPE_PLAIN,
    _next_wire_timestamp,
    build_advert_packet,
    build_group_text_packet,
    build_login_packet,
    build_neighbors_request_payload,
    build_private_text_packet,
    build_request_packet,
    calculate_distance_km,
    decode_advert,
    decode_trace,
    describe_packet,
    parse_path_return_payload,
    parse_acl_response,
    parse_login_response,
    parse_neighbors_response,
    parse_owner_info_response,
    parse_tagged_response,
    parse_text_plaintext,
    split_sender_and_content,
    try_decode_group_text,
    try_decode_private_datagram,
)
from .protocol import RS232BridgeDecoder, encode_frame


COMPANION_CMD_SEND_CHANNEL_TXT_MSG = 0x03
COMPANION_CMD_GET_CHANNEL = 0x1F
COMPANION_RESP_OK = 0x00
COMPANION_RESP_ERR = 0x01
COMPANION_RESP_CHANNEL_INFO = 0x12
COMPANION_MAX_CHANNEL_SLOTS = 8


class EndpointSession:
    def __init__(self, config: EndpointConfig) -> None:
        self.config = config
        self.raw_reader: asyncio.StreamReader | None = None
        self.raw_writer: asyncio.StreamWriter | None = None
        self.cli_reader: asyncio.StreamReader | None = None
        self.cli_writer: asyncio.StreamWriter | None = None
        self.console_reader: asyncio.StreamReader | None = None
        self.console_writer: asyncio.StreamWriter | None = None
        self.cli_lock = asyncio.Lock()
        self.companion_lock = asyncio.Lock()
        self.channel_tx_lock = asyncio.Lock()
        self.decoder = RS232BridgeDecoder(max_payload_len=255)
        self.telemetry = ConsoleTelemetryBuffer(config.name)
        self.pending_companion: PendingCompanionCommand | None = None
        self.channel_index_by_name: dict[str, int] = {}
        self.last_channel_tx_monotonic: float = 0.0
        self.connected = False
        self.last_error: str | None = None
        self.last_seen_at: datetime | None = None
        self.last_self_advert_at: datetime | None = None
        self.last_cli_command_at: datetime | None = None
        self.last_cli_command: str | None = None
        self.last_cli_reply: str | None = None
        self.last_cli_error: str | None = None


@dataclass(slots=True)
class PendingManagementRequest:
    kind: str
    tag: int
    sent_at: datetime
    requester_role: str
    used_direct: bool = False
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PendingCompanionCommand:
    kind: str
    future: asyncio.Future[bytes]
    expected_channel_index: int | None = None


@dataclass(slots=True)
class ManagementTargetState:
    config: ManagementNodeConfig
    resolved_identity_hex: str | None = None
    learned_path_hashes: list[str] | None = None
    last_observed_advert_at: datetime | None = None
    last_successful_advert_at: datetime | None = None
    next_retry_at: datetime | None = None
    queue_reason: str | None = None
    current_role: str | None = None
    last_login_at: datetime | None = None
    last_status_at: datetime | None = None
    last_status_size: int = 0
    login_attempt_index: int = 0
    pending_login_role: str | None = None
    pending_login_password: str | None = None
    pending_login_at: datetime | None = None
    pending_request: PendingManagementRequest | None = None
    last_owner_at: datetime | None = None
    last_acl_at: datetime | None = None
    last_neighbors_at: datetime | None = None
    last_console_neighbors_at: datetime | None = None
    last_console_owner_at: datetime | None = None
    last_console_acl_at: datetime | None = None
    last_error: str | None = None
    owner_info: dict[str, Any] | None = None
    acl_entry_count: int = 0
    neighbor_count: int = 0


class MeshcoreTCPBotService:
    MIN_RESPONSE_DELAY_SECONDS = 1.0
    MIN_CHANNEL_TX_SPACING_SECONDS = 2.5
    RUNTIME_BOT_SETTINGS_KEY = "runtime.bot"
    RUNTIME_CHANNELS_KEY = "runtime.channels"
    RUNTIME_ENDPOINTS_KEY = "runtime.endpoints"
    RUNTIME_COMMANDS_KEY = "runtime.commands"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.logger = logging.getLogger("meshcore_tcp_bot.service")
        self.started_at = datetime.now(tz=UTC)
        self.identity, self.identity_created = load_or_create_identity(config.identity.file_path)
        self.store = MeshcoreStore(config.storage.database_path)
        self.runtime_bot_settings = self.store.ensure_json_setting(
            self.RUNTIME_BOT_SETTINGS_KEY,
            self._default_runtime_bot_settings(),
        )
        self.command_settings = self.store.ensure_json_setting(
            self.RUNTIME_COMMANDS_KEY,
            self._default_command_settings(),
        )
        runtime_channels = self.store.ensure_json_setting(
            self.RUNTIME_CHANNELS_KEY,
            self._serialize_channel_configs(config.channels),
        )
        runtime_endpoints = self.store.ensure_json_setting(
            self.RUNTIME_ENDPOINTS_KEY,
            self._serialize_endpoint_configs(config.endpoints),
        )
        self._apply_runtime_bot_settings(self.runtime_bot_settings)
        self.config.channels = tuple(self._deserialize_channel_config(item) for item in runtime_channels)
        self.config.endpoints = tuple(self._deserialize_endpoint_config(item) for item in runtime_endpoints)
        self.management_registry = RepeaterManagementRegistry(self.store, config.management_nodes)
        self.management_config = config.management
        self.channel_defs: tuple[ChannelDefinition, ...] = ()
        self.channel_by_name: dict[str, ChannelDefinition] = {}
        self.listen_channels: set[str] = set()
        self._refresh_channel_state()
        self.sessions = {item.name: EndpointSession(item) for item in config.endpoints if item.enabled}
        self.nodes_by_identity: dict[str, NodeRecord] = {
            item.identity_hex: item for item in self.store.load_nodes()
        }
        self.nodes_by_name: dict[str, str] = {}
        for item in self.nodes_by_identity.values():
            if item.name:
                self.nodes_by_name[item.name] = item.identity_hex
        self.messages: deque[MeshMessage] = deque(maxlen=config.bot.message_history_size)
        self.seen_packet_ids: dict[str, datetime] = {}
        self.total_packets_seen = 0
        self.total_group_text_seen = 0
        self.total_group_text_decoded = 0
        self.total_adverts_seen = 0
        self.last_packet_summary: dict[str, object] | None = None
        self.last_drop_reason: str | None = None
        self.management_packet_trace: deque[dict[str, object]] = deque(maxlen=80)
        self._started = False
        self._endpoint_tasks: dict[str, asyncio.Task] = {}
        self._admin_lock = asyncio.Lock()
        self.management_states = {
            item.name: ManagementTargetState(item, resolved_identity_hex=item.target_identity_hex)
            for item in self.management_registry.get_targets()
            if item.enabled
        }
        self._management_queue: deque[str] = deque()
        self._management_queued: set[str] = set()
        self._management_active_name: str | None = None
        self._management_wake_event = asyncio.Event()
        self._management_task: asyncio.Task | None = None
        self._tasks: list[asyncio.Task] = []
        self._stop_event = asyncio.Event()
        for state in self.management_states.values():
            self._resolve_management_identity(state)
            self._seed_management_state_from_known_nodes(state)
        for state in self._bootstrap_management_targets_from_known_nodes():
            if not (self._login_password_candidates(state, "guest") or self._login_password_candidates(state, "admin")):
                state.last_error = "auto-discovered repeater has no guest/admin credential configured"

    def _default_runtime_bot_settings(self) -> dict[str, Any]:
        return {
            "name": self.config.bot.name,
            "reply_prefix": self.config.bot.reply_prefix,
            "command_prefix": self.config.bot.command_prefix,
            "message_history_size": self.config.bot.message_history_size,
            "private_messages_enabled": True,
            "private_message_auto_response": f"{self.config.bot.reply_prefix}Private messages are enabled. Try {self.config.bot.command_prefix}help",
            "signal_history_limit": 32,
            "signal_history_target_limit": 12,
            "neighbor_snapshot_retention": 96,
        }

    def _default_command_settings(self) -> dict[str, dict[str, Any]]:
        return {
            "ping": {"enabled": True, "response_template": "pong"},
            "help": {"enabled": True, "response_template": "{reply_prefix}Commands: {command_list}"},
            "test": {"enabled": True, "response_template": "{reply_prefix}I saw: {sender} (hops={path_len}{snr_suffix}{rssi_suffix}{distance_suffix})"},
            "trace": {"enabled": True, "response_template": "{reply_prefix}Trace: {trace}"},
            "neighbors": {"enabled": True, "response_template": "{reply_prefix}{neighbors_summary}"},
        }

    @staticmethod
    def _serialize_endpoint_configs(endpoints: tuple[EndpointConfig, ...] | list[EndpointConfig]) -> list[dict[str, Any]]:
        return [asdict(item) for item in endpoints]

    @staticmethod
    def _serialize_channel_configs(channels: tuple[Any, ...] | list[Any]) -> list[dict[str, Any]]:
        return [asdict(item) for item in channels]

    @staticmethod
    def _deserialize_endpoint_config(item: dict[str, Any]) -> EndpointConfig:
        return EndpointConfig(
            name=str(item["name"]),
            raw_host=str(item["raw_host"]),
            raw_port=int(item.get("raw_port", 5002)),
            enabled=bool(item.get("enabled", True)),
            console_host=str(item["console_host"]) if item.get("console_host") else None,
            console_port=int(item["console_port"]) if item.get("console_port") is not None else None,
            console_mirror_host=str(item["console_mirror_host"]) if item.get("console_mirror_host") else None,
            console_mirror_port=int(item["console_mirror_port"]) if item.get("console_mirror_port") is not None else None,
            latitude=float(item["latitude"]) if item.get("latitude") is not None else None,
            longitude=float(item["longitude"]) if item.get("longitude") is not None else None,
        )

    @staticmethod
    def _deserialize_channel_config(item: dict[str, Any]):
        from .config import ChannelConfig

        return ChannelConfig(
            name=str(item["name"]).lower(),
            psk=str(item["psk"]) if item.get("psk") else None,
            listen=bool(item.get("listen", True)),
        )

    def _apply_runtime_bot_settings(self, settings: dict[str, Any]) -> None:
        self.config.bot.name = str(settings.get("name", self.config.bot.name))
        self.config.bot.reply_prefix = str(settings.get("reply_prefix", self.config.bot.reply_prefix))
        self.config.bot.command_prefix = str(settings.get("command_prefix", self.config.bot.command_prefix))
        self.config.bot.message_history_size = max(10, int(settings.get("message_history_size", self.config.bot.message_history_size)))

    def _refresh_channel_state(self) -> None:
        self.channel_defs = tuple(ChannelDefinition(name=item.name, psk=item.psk) for item in self.config.channels)
        self.channel_by_name = {item.name: item for item in self.channel_defs}
        self.listen_channels = {item.name.lower() for item in self.config.channels if getattr(item, "listen", True)}

    def _resize_message_history(self) -> None:
        if getattr(self, "messages", None) is None:
            return
        self.messages = deque(list(self.messages), maxlen=self.config.bot.message_history_size)

    def _runtime_setting_int(self, key: str, default_value: int, minimum: int = 1) -> int:
        try:
            value = int(self.runtime_bot_settings.get(key, default_value))
        except (TypeError, ValueError):
            value = default_value
        return max(minimum, value)

    def _private_messages_enabled(self) -> bool:
        return bool(self.runtime_bot_settings.get("private_messages_enabled", True))

    def _private_message_auto_response(self) -> str:
        return str(self.runtime_bot_settings.get("private_message_auto_response", "")).strip()

    def _known_command_names(self) -> list[str]:
        return [name for name, settings in self.command_settings.items() if settings.get("enabled", True)]

    def _safe_format(self, template: str, context: dict[str, Any]) -> str:
        safe_context: defaultdict[str, str] = defaultdict(str)
        for key, value in context.items():
            safe_context[key] = "" if value is None else str(value)
        try:
            return template.format_map(safe_context).strip()
        except Exception:
            return template.strip()

    def _command_context(self, message: MeshMessage) -> dict[str, Any]:
        snr_suffix = f", snr={message.snr:.1f}" if message.snr is not None else ""
        rssi_suffix = f", rssi={message.rssi}" if message.rssi is not None else ""
        distance_suffix = f", dist={message.distance_km:.2f}km" if message.distance_km is not None else ""
        trace_hops = [message.sender]
        trace_hops.extend(self._resolve_hops(message.path_hashes))
        trace_hops.append(message.endpoint_name)
        return {
            "bot_name": self.config.bot.name,
            "reply_prefix": self.config.bot.reply_prefix,
            "command_prefix": self.config.bot.command_prefix,
            "sender": message.sender,
            "channel_name": message.channel_name,
            "path_len": message.path_len,
            "snr": "" if message.snr is None else f"{message.snr:.1f}",
            "rssi": "" if message.rssi is None else str(message.rssi),
            "distance_km": "" if message.distance_km is None else f"{message.distance_km:.2f}",
            "snr_suffix": snr_suffix,
            "rssi_suffix": rssi_suffix,
            "distance_suffix": distance_suffix,
            "trace": " -> ".join(trace_hops),
            "neighbors_summary": self._neighbors_summary_text(message),
            "command_list": ", ".join(f"{self.config.bot.command_prefix}{name}" for name in self._known_command_names()),
        }

    def _compact_channel_help_response(self) -> str:
        command_names = [name for name in self._known_command_names() if name != "help"]
        return " ".join(f"{self.config.bot.command_prefix}{name}" for name in command_names)

    def _compact_channel_command_response(self, command: str, message: MeshMessage, default_response: str) -> str:
        if command == "help":
            return self._compact_channel_help_response()
        if command == "test":
            return f"ok hops={message.path_len}"
        if command == "trace":
            return f"trace hops={message.path_len}"
        if command == "neighbors":
            return "neighbors: see web"
        if default_response.startswith(self.config.bot.reply_prefix):
            return default_response[len(self.config.bot.reply_prefix):].strip()
        return default_response

    async def start(self) -> None:
        self._started = True
        for session in self.sessions.values():
            self._start_endpoint_task(session)
        self._ensure_management_loop_started()
        for state in self.management_states.values():
            self._queue_management_target(state, "startup")

    async def stop(self) -> None:
        self._started = False
        self._stop_event.set()
        for name in list(self._endpoint_tasks):
            await self._stop_endpoint_runtime(name)
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        self._endpoint_tasks.clear()
        for session in self.sessions.values():
            await self._close_session(session)

    async def wait_forever(self) -> None:
        await self._stop_event.wait()

    def _start_endpoint_task(self, session: EndpointSession) -> None:
        task = asyncio.create_task(self._run_endpoint(session), name=f"endpoint:{session.config.name}")
        self._endpoint_tasks[session.config.name] = task
        self._tasks.append(task)

    async def _stop_endpoint_runtime(self, name: str) -> None:
        task = self._endpoint_tasks.pop(name, None)
        session = self.sessions.get(name)
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if session is not None:
            await self._close_session(session)

    async def _run_endpoint(self, session: EndpointSession) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_session(session)
                readers = [asyncio.create_task(self._listen_raw(session), name=f"raw:{session.config.name}")]
                done, pending = await asyncio.wait(readers, return_when=asyncio.FIRST_EXCEPTION)
                for task in pending:
                    task.cancel()
                for task in done:
                    exc = task.exception()
                    if exc:
                        raise exc
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                session.last_error = str(exc)
                session.connected = False
                self.logger.warning("endpoint %s disconnected: %s", session.config.name, exc)
            finally:
                await self._close_session(session)
            await asyncio.sleep(2.0)

    async def _connect_session(self, session: EndpointSession) -> None:
        raw_reader, raw_writer = await asyncio.open_connection(session.config.raw_host, session.config.raw_port)
        session.raw_reader = raw_reader
        session.raw_writer = raw_writer
        sock = raw_writer.get_extra_info("socket")
        if isinstance(sock, socket.socket):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if session.config.console_host and session.config.console_port:
            try:
                cli_reader, cli_writer = await asyncio.open_connection(
                    session.config.console_host,
                    session.config.console_port,
                )
                session.cli_reader = cli_reader
                session.cli_writer = cli_writer
                await self._prime_console_cli(session)
                session.last_cli_error = None
            except Exception as exc:
                session.cli_reader = None
                session.cli_writer = None
                session.last_cli_error = str(exc)
        session.connected = True
        session.last_error = None
        self.logger.info("connected to endpoint %s", session.config.name)
        if self.config.bot.self_advert_enabled:
            self._tasks.append(
                asyncio.create_task(
                    self._send_self_advert(session, delay_seconds=1.0),
                    name=f"self-advert:{session.config.name}",
                )
            )
        if self.management_config.enabled:
            for state in self.management_states.values():
                if state.config.endpoint_name == session.config.name:
                    self._queue_management_target(state, "endpoint-connected")

    async def _close_session(self, session: EndpointSession) -> None:
        for writer in (session.raw_writer, session.cli_writer, session.console_writer):
            if writer is not None:
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()
        session.raw_reader = None
        session.raw_writer = None
        session.cli_reader = None
        session.cli_writer = None
        session.console_reader = None
        session.console_writer = None
        session.connected = False

    async def _listen_raw(self, session: EndpointSession) -> None:
        assert session.raw_reader is not None
        while not self._stop_event.is_set():
            chunk = await session.raw_reader.read(4096)
            if not chunk:
                raise ConnectionError("raw TCP stream closed")
            for decoded in session.decoder.feed(chunk):
                await self._handle_packet(session, decoded.payload)

    def _try_resolve_companion_response(self, session: EndpointSession, payload: bytes) -> bool:
        pending = session.pending_companion
        if pending is None or pending.future.done():
            return False

        if pending.kind == "send_channel":
            if payload == bytes([COMPANION_RESP_OK]):
                pending.future.set_result(payload)
                return True
            if len(payload) == 2 and payload[0] == COMPANION_RESP_ERR:
                pending.future.set_exception(RuntimeError(f"companion channel send failed with error code {payload[1]}"))
                return True
            return False

        if pending.kind == "get_channel":
            if len(payload) >= 34 and payload[0] == COMPANION_RESP_CHANNEL_INFO:
                channel_index = payload[1]
                if pending.expected_channel_index is None or channel_index == pending.expected_channel_index:
                    pending.future.set_result(payload)
                    return True
            if len(payload) == 2 and payload[0] == COMPANION_RESP_ERR:
                pending.future.set_exception(RuntimeError(f"companion get_channel failed with error code {payload[1]}"))
                return True
            return False

        return False

    async def _handle_packet(self, session: EndpointSession, payload: bytes) -> None:
        if self._try_resolve_companion_response(session, payload):
            return

        summary = describe_packet(payload)
        if summary.packet_type_name in {"ANON_REQ", "REQ", "RESPONSE", "PATH"}:
            self._record_management_packet_event("rx", session.config.name, payload)
        if self._is_duplicate_summary(summary):
            self.last_drop_reason = "duplicate message copy ignored"
            return

        self.total_packets_seen += 1
        session.last_seen_at = datetime.now(tz=UTC)
        self.last_packet_summary = {
            "endpoint": session.config.name,
            "route": summary.route_name,
            "packet_type": summary.packet_type_name,
            "path_len": summary.path_len,
            "payload_len": len(summary.payload),
            "transport_codes": summary.transport_codes,
        }

        advert = decode_advert(payload)
        if advert is not None:
            self.total_adverts_seen += 1
            self._update_node_from_advert(session.config, advert)
            return

        if await self._handle_private_packet(session, payload):
            return

        decoded_trace = decode_trace(payload)
        if decoded_trace is not None:
            self.logger.debug("trace packet on %s: %s", session.config.name, decoded_trace)
            return

        decoded = try_decode_group_text(payload, self.channel_defs)
        if decoded is None:
            if summary.packet_type_name == "GRP_TXT":
                self.total_group_text_seen += 1
                self.last_drop_reason = "group text did not match configured channel definitions or failed decrypt"
            return

        self.total_group_text_seen += 1
        self.total_group_text_decoded += 1
        channel, group_message = decoded
        channel_name = channel.name
        if channel_name.lower() not in self.listen_channels:
            self.last_drop_reason = f"decoded channel '{channel_name}' is not in listen_channels"
            return

        sender, content = split_sender_and_content(group_message.text)
        telemetry = session.telemetry.match(summary.route_name, summary.packet_type, len(summary.payload))
        distance_km = self._distance_for_sender(session.config, sender)
        message = MeshMessage(
            endpoint_name=session.config.name,
            channel_name=channel_name,
            channel_psk=channel.psk,
            sender=sender,
            sender_identity_hex=None,
            content=content,
            packet_type=summary.packet_type_name,
            route_name=summary.route_name,
            path_hashes=summary.path_hashes,
            path_len=summary.path_len,
            received_at=datetime.now(tz=UTC),
            channel_hash=group_message.channel_hash,
            snr=telemetry.snr if telemetry else None,
            rssi=telemetry.rssi if telemetry else None,
            distance_km=distance_km,
            raw_payload_hex=payload.hex(),
        )
        self.messages.appendleft(message)
        self.last_drop_reason = None
        self.logger.info("%s %s %s: %s", message.endpoint_name, message.channel_name, message.sender, message.content)
        await self._handle_command(session, message)

    async def _send_companion_command(
        self,
        session: EndpointSession,
        request: bytes,
        *,
        kind: str,
        expected_channel_index: int | None = None,
        timeout_seconds: float = 5.0,
    ) -> bytes:
        if session.raw_writer is None:
            raise RuntimeError(f"endpoint {session.config.name} is not connected")

        async with session.companion_lock:
            loop = asyncio.get_running_loop()
            future: asyncio.Future[bytes] = loop.create_future()
            session.pending_companion = PendingCompanionCommand(
                kind=kind,
                future=future,
                expected_channel_index=expected_channel_index,
            )
            session.raw_writer.write(encode_frame(request))
            await session.raw_writer.drain()
            try:
                return await asyncio.wait_for(future, timeout=timeout_seconds)
            finally:
                if session.pending_companion is not None and session.pending_companion.future is future:
                    session.pending_companion = None

    @staticmethod
    def _parse_companion_channel_info(payload: bytes) -> tuple[int, str]:
        channel_index = payload[1]
        channel_name = payload[2:34].split(b"\x00", 1)[0].decode("utf-8", errors="ignore").strip().lower()
        return channel_index, channel_name

    async def _resolve_channel_index(self, session: EndpointSession, channel_name: str) -> int | None:
        normalized_name = channel_name.strip().lower()
        if not normalized_name:
            return None
        cached_index = session.channel_index_by_name.get(normalized_name)
        if cached_index is not None:
            return cached_index

        for channel_index in range(COMPANION_MAX_CHANNEL_SLOTS):
            try:
                payload = await self._send_companion_command(
                    session,
                    bytes([COMPANION_CMD_GET_CHANNEL, channel_index]),
                    kind="get_channel",
                    expected_channel_index=channel_index,
                )
            except Exception:
                continue
            resolved_index, resolved_name = self._parse_companion_channel_info(payload)
            if resolved_name:
                session.channel_index_by_name[resolved_name] = resolved_index
            if resolved_name == normalized_name:
                return resolved_index

        return None

    async def _handle_private_packet(self, session: EndpointSession, payload: bytes) -> bool:
        decoded = try_decode_private_datagram(payload, self.identity, tuple(self._private_contacts()))
        if decoded is None:
            return False

        self.last_drop_reason = None
        if decoded.packet_type == TYPE_PATH:
            path_payload = parse_path_return_payload(decoded.plaintext)
            if path_payload is None:
                return True
            self._record_management_path(decoded.sender_identity_hex, session.config.name, path_payload.path_hashes)
            if path_payload.extra_type == TYPE_RESPONSE:
                nested = type("NestedDecoded", (), {
                    "sender_identity_hex": decoded.sender_identity_hex,
                    "plaintext": path_payload.extra_payload,
                })
                self._handle_management_response(session, nested)
            return True

        if decoded.packet_type == TYPE_RESPONSE:
            self._handle_management_response(session, decoded)
            return True

        if decoded.packet_type == TYPE_TXT_MSG:
            parsed = parse_text_plaintext(decoded.plaintext)
            if parsed is None:
                return True
            _, _, _, text = parsed
            sender_label = decoded.sender_name or self._display_name_for_identity(decoded.sender_identity_hex)
            message = MeshMessage(
                endpoint_name=session.config.name,
                channel_name="dm",
                channel_psk=None,
                sender=sender_label,
                sender_identity_hex=decoded.sender_identity_hex,
                content=text,
                packet_type=decoded.packet_type_name,
                route_name=decoded.route_name,
                path_hashes=decoded.path_hashes,
                path_len=decoded.path_len,
                received_at=datetime.now(tz=UTC),
                channel_hash=None,
                raw_payload_hex=payload.hex(),
            )
            self.messages.appendleft(message)
            self.logger.info("%s dm %s: %s", message.endpoint_name, message.sender, message.content)
            handled = await self._handle_command(session, message)
            if not handled:
                await self._maybe_auto_reply_private_message(session, message)
            return True

        return True

    def _is_duplicate_summary(self, summary) -> bool:
        now = datetime.now(tz=UTC)
        self._prune_seen_payloads(now)

        dedupe_material = bytes([summary.packet_type]) + summary.payload
        payload_id = hashlib.sha256(dedupe_material).hexdigest()
        if payload_id in self.seen_packet_ids:
            return True
        self.seen_packet_ids[payload_id] = now
        return False

    def _prune_seen_payloads(self, now: datetime) -> None:
        cutoff_seconds = 30
        expired = [key for key, seen_at in self.seen_packet_ids.items() if (now - seen_at).total_seconds() > cutoff_seconds]
        for key in expired:
            self.seen_packet_ids.pop(key, None)

    def _private_contacts(self) -> list[PeerContact]:
        contacts: dict[str, PeerContact] = {}
        for identity_hex, node in self.nodes_by_identity.items():
            contacts[identity_hex] = PeerContact(identity_hex=identity_hex, public_key=bytes.fromhex(identity_hex), name=node.name)
        for state in self.management_states.values():
            if state.config.target_identity_hex and state.config.target_identity_hex not in contacts:
                contacts[state.config.target_identity_hex] = PeerContact(
                    identity_hex=state.config.target_identity_hex,
                    public_key=bytes.fromhex(state.config.target_identity_hex),
                    name=state.config.name,
                )
        return list(contacts.values())

    def _update_node_from_advert(self, endpoint: EndpointConfig, advert) -> None:
        if advert.role not in ("Repeater", "Room Server"):
            return
        now = datetime.now(tz=UTC)
        existing = self.nodes_by_identity.get(advert.identity_hex)
        if existing is None:
            existing = NodeRecord(identity_hex=advert.identity_hex, hash_prefix_hex=advert.hash_prefix_hex)
            self.nodes_by_identity[advert.identity_hex] = existing
        existing.name = advert.name or existing.name
        existing.role = advert.role or existing.role
        existing.latitude = advert.latitude if advert.latitude is not None else existing.latitude
        existing.longitude = advert.longitude if advert.longitude is not None else existing.longitude
        existing.last_advert_at = now
        existing.last_seen_endpoint = endpoint.name
        if existing.name:
            self.nodes_by_name[existing.name] = existing.identity_hex
        self.store.upsert_advert(endpoint.name, advert, now)
        discovered_state = self._ensure_management_state_for_advert(endpoint, advert)
        for state in self.management_states.values():
            self._resolve_management_identity(state)
            self._mark_target_seen_on_advert(state, endpoint.name, advert.identity_hex, now)
        if discovered_state is not None and self.management_config.enabled:
            self._ensure_management_loop_started()
            self._queue_management_target(discovered_state, "auto-discovered")

    def _ensure_management_loop_started(self) -> None:
        if not self.management_config.enabled or not self.management_states:
            return
        if self._management_task is not None and not self._management_task.done():
            return
        self.logger.info("starting management loop for %d target(s)", len(self.management_states))
        self._management_task = asyncio.create_task(self._run_management_loop(), name="management")
        self._tasks.append(self._management_task)

    def _seed_management_state_from_known_nodes(self, state: ManagementTargetState) -> None:
        if state.resolved_identity_hex is None:
            return
        node = self.nodes_by_identity.get(state.resolved_identity_hex)
        if node is None or node.last_advert_at is None:
            return
        state.last_observed_advert_at = node.last_advert_at.astimezone(UTC)
        last_successful_neighbors_at = self.store.latest_successful_neighbor_snapshot_at(state.config.name)
        if last_successful_neighbors_at is not None and last_successful_neighbors_at >= state.last_observed_advert_at:
            state.last_successful_advert_at = state.last_observed_advert_at
            state.next_retry_at = None

    def _mark_target_seen_on_advert(
        self,
        state: ManagementTargetState,
        endpoint_name: str,
        advert_identity_hex: str,
        observed_at: datetime,
    ) -> None:
        if state.config.endpoint_name != endpoint_name:
            return
        target_identity_hex = state.resolved_identity_hex or state.config.target_identity_hex
        if target_identity_hex is None or target_identity_hex != advert_identity_hex:
            return
        previous_advert_at = state.last_observed_advert_at
        state.last_observed_advert_at = observed_at
        if previous_advert_at is None or observed_at > previous_advert_at:
            state.next_retry_at = None
            self._queue_management_target(state, "fresh-advert", force=True)

    def _target_needs_refresh(self, state: ManagementTargetState, now: datetime | None = None) -> bool:
        if state.resolved_identity_hex is None:
            return False
        if state.last_observed_advert_at is None:
            self._seed_management_state_from_known_nodes(state)
        if state.last_observed_advert_at is None:
            return False
        if state.last_successful_advert_at is not None and state.last_successful_advert_at >= state.last_observed_advert_at:
            return False
        current_time = now or datetime.now(tz=UTC)
        if state.next_retry_at is not None and current_time < state.next_retry_at:
            return False
        return True

    def _queue_management_target(self, state: ManagementTargetState, reason: str, *, force: bool = False) -> None:
        if not self.management_config.enabled:
            return
        if not force and not self._target_needs_refresh(state):
            return
        name = state.config.name
        state.queue_reason = reason
        if name in self._management_queued:
            self._management_wake_event.set()
            return
        self._management_queue.append(name)
        self._management_queued.add(name)
        self._management_wake_event.set()

    def _release_management_target(self, state: ManagementTargetState) -> None:
        if self._management_active_name == state.config.name:
            self._management_active_name = None
        self._management_wake_event.set()

    def _mark_management_refresh_success(self, state: ManagementTargetState, observed_at: datetime) -> None:
        state.last_successful_advert_at = state.last_observed_advert_at
        state.next_retry_at = None
        state.last_error = None
        state.queue_reason = None
        self._release_management_target(state)

    def _mark_management_refresh_deferred(self, state: ManagementTargetState, now: datetime, error_text: str | None = None) -> None:
        state.next_retry_at = now.replace(microsecond=0) + timedelta(seconds=self.management_config.retry_after_failed_poll_seconds)
        if error_text:
            state.last_error = error_text
        state.queue_reason = None
        self._release_management_target(state)

    def _distance_for_sender(self, endpoint: EndpointConfig, sender: str) -> float | None:
        if endpoint.latitude is None or endpoint.longitude is None:
            return None
        identity = self.nodes_by_name.get(sender)
        if identity is None:
            return None
        node = self.nodes_by_identity.get(identity)
        if node is None or node.latitude is None or node.longitude is None:
            return None
        return calculate_distance_km((endpoint.latitude, endpoint.longitude), (node.latitude, node.longitude))

    async def _handle_command(self, session: EndpointSession, message: MeshMessage) -> bool:
        content = message.content.strip()
        if not content.startswith(self.config.bot.command_prefix):
            return False

        if message.channel_name == "dm" and not self._private_messages_enabled():
            return False

        parts = content[len(self.config.bot.command_prefix):].split(None, 1)
        command = parts[0].lower() if parts else ""
        if not command:
            return False

        command_settings = self.command_settings.get(command)
        if command_settings is None or not command_settings.get("enabled", True):
            return False

        if command not in {"ping", "help", "test", "trace", "neighbors"}:
            return False

        response = self._safe_format(
            str(command_settings.get("response_template") or self._default_command_settings()[command]["response_template"]),
            self._command_context(message),
        )
        if message.channel_name != "dm":
            response = self._compact_channel_command_response(command, message, response)
        if not response:
            return False

        await self._apply_minimum_response_delay(message)
        if message.channel_name == "dm" and message.sender_identity_hex:
            await self.send_private_message(session.config.name, message.sender_identity_hex, response)
        else:
            await self.send_channel_message(
                session.config.name,
                message.channel_name,
                response,
                channel_psk=message.channel_psk,
            )
        return True

    async def _maybe_auto_reply_private_message(self, session: EndpointSession, message: MeshMessage) -> None:
        if message.channel_name != "dm" or not message.sender_identity_hex:
            return
        if not self._private_messages_enabled():
            return
        response = self._private_message_auto_response()
        if not response:
            return
        formatted = self._safe_format(
            response,
            {
                **self._command_context(message),
                "command_list": ", ".join(f"{self.config.bot.command_prefix}{name}" for name in self._known_command_names()),
            },
        )
        if not formatted:
            return
        await self._apply_minimum_response_delay(message)
        await self.send_private_message(session.config.name, message.sender_identity_hex, formatted)

    async def _apply_minimum_response_delay(self, message: MeshMessage) -> None:
        elapsed = (datetime.now(tz=UTC) - message.received_at).total_seconds()
        remaining = self.MIN_RESPONSE_DELAY_SECONDS - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)

    def _format_help_response(self) -> str:
        command_list = ", ".join(f"{self.config.bot.command_prefix}{name}" for name in self._known_command_names())
        return f"{self.config.bot.reply_prefix}Commands: {command_list}"

    def _neighbors_summary_text(self, message: MeshMessage) -> str:
        latest_topology = self.store.recent_neighbor_summary(limit=3)
        if latest_topology:
            parts = []
            for item in latest_topology:
                age = self._humanize_age(item.get("collected_at"))
                status = f"{item['neighbor_count']} nbrs"
                if not item.get("success"):
                    status = f"err: {item.get('error_text') or 'snapshot failed'}"
                parts.append(f"{item['target_name']} {status} {age}".strip())
            return "Neighbors: " + " | ".join(parts)

        repeaters = self.store.recent_repeaters(limit=4)
        if not repeaters:
            return "Neighbors: no repeater adverts stored yet"

        endpoint = self.sessions.get(message.endpoint_name)
        endpoint_origin = None
        if endpoint and endpoint.config.latitude is not None and endpoint.config.longitude is not None:
            endpoint_origin = (endpoint.config.latitude, endpoint.config.longitude)

        parts = []
        for row in repeaters:
            label = row.get("name") or row.get("hash_prefix_hex") or "unknown"
            part = label
            if row.get("latitude") is not None and row.get("longitude") is not None and endpoint_origin is not None:
                dist = calculate_distance_km(endpoint_origin, (row["latitude"], row["longitude"]))
                if dist is not None:
                    part += f" {dist:.1f}km"
            age = self._humanize_age(row.get("last_seen_at"))
            if age:
                part += f" {age}"
            parts.append(part)
        return "Nearby rpt: " + " | ".join(parts)

    def _format_test_response(self, message: MeshMessage) -> str:
        parts = [f"{self.config.bot.reply_prefix}I saw: {message.sender} (hops={message.path_len}"]
        if message.snr is not None:
            parts.append(f", snr={message.snr:.1f}")
        if message.rssi is not None:
            parts.append(f", rssi={message.rssi}")
        if message.distance_km is not None:
            parts.append(f", dist={message.distance_km:.2f}km")
        parts.append(")")
        return "".join(parts)

    def _format_trace_response(self, message: MeshMessage) -> str:
        hops = [message.sender]
        hops.extend(self._resolve_hops(message.path_hashes))
        hops.append(message.endpoint_name)
        return f"{self.config.bot.reply_prefix}Trace: {' -> '.join(hops)}"

    def _format_neighbors_response(self, message: MeshMessage) -> str:
        return f"{self.config.bot.reply_prefix}{self._neighbors_summary_text(message)}"

    def _humanize_age(self, iso_value: str | None) -> str:
        if not iso_value:
            return ""
        observed_at = datetime.fromisoformat(iso_value)
        delta = datetime.now(tz=UTC) - observed_at.astimezone(UTC)
        seconds = int(delta.total_seconds())
        if seconds < 60:
            return f"{seconds}s ago"
        if seconds < 3600:
            return f"{seconds // 60}m ago"
        if seconds < 86400:
            return f"{seconds // 3600}h ago"
        return f"{seconds // 86400}d ago"

    def _resolve_hops(self, hashes: list[str]) -> list[str]:
        resolved: list[str] = []
        by_prefix = {node.hash_prefix_hex.upper(): node for node in self.nodes_by_identity.values()}
        for item in hashes:
            node = by_prefix.get(item.upper())
            if node and node.name:
                resolved.append(node.name)
            else:
                resolved.append(item.upper())
        return resolved

    def _display_name_for_identity(self, identity_hex: str) -> str:
        node = self.nodes_by_identity.get(identity_hex)
        if node and node.name:
            return node.name
        return identity_hex[:12].upper()

    @staticmethod
    def _auto_discovery_password(password: str | None) -> str | None:
        if password is None or password == "":
            return None
        return password

    def _existing_management_state_for_identity_or_prefix(
        self,
        identity_hex: str | None,
        hash_prefix_hex: str | None,
    ) -> ManagementTargetState | None:
        normalized_identity = identity_hex.lower() if identity_hex else None
        normalized_prefix = hash_prefix_hex.upper() if hash_prefix_hex else None
        for state in self.management_states.values():
            state_identity = (state.config.target_identity_hex or state.resolved_identity_hex or "").lower() or None
            state_prefix = (state.config.target_hash_prefix or "").upper() or None
            if normalized_identity and state_identity == normalized_identity:
                return state
            if normalized_prefix and state_prefix == normalized_prefix:
                return state
        return None

    def _register_dynamic_management_target(
        self,
        *,
        endpoint_name: str,
        target_name: str,
        target_hash_prefix: str | None,
        target_identity_hex: str | None,
        notes: str,
    ) -> ManagementTargetState:
        registered = self.management_registry.register_dynamic_target(
            ManagementNodeConfig(
                name=target_name,
                endpoint_name=endpoint_name,
                target_hash_prefix=target_hash_prefix,
                target_identity_hex=target_identity_hex,
                guest_password=self._auto_discovery_password(self.management_config.auto_guest_password),
                admin_password=self._auto_discovery_password(self.management_config.auto_admin_password),
                prefer_role="guest",
                enabled=True,
                notes=notes,
            )
        )
        state = self.management_states.get(registered.name)
        if state is None:
            state = ManagementTargetState(registered, resolved_identity_hex=registered.target_identity_hex)
            self.management_states[registered.name] = state
        self._resolve_management_identity(state)
        self._seed_management_state_from_known_nodes(state)
        return state

    def _bootstrap_management_targets_from_known_nodes(self) -> list[ManagementTargetState]:
        discovered: list[ManagementTargetState] = []
        if not self.management_config.enabled:
            return discovered
        for node in self.nodes_by_identity.values():
            if node.role not in ("Repeater", "Room Server"):
                continue
            endpoint_name = node.last_seen_endpoint
            if not endpoint_name or endpoint_name not in self.sessions:
                continue
            if self._existing_management_state_for_identity_or_prefix(node.identity_hex, node.hash_prefix_hex):
                continue
            safe_name = (node.name or f"rpt-{node.hash_prefix_hex}").strip()
            target_name = safe_name
            if target_name in self.management_states:
                target_name = f"{safe_name}-{node.hash_prefix_hex.upper()}"
            state = self._register_dynamic_management_target(
                endpoint_name=endpoint_name,
                target_name=target_name,
                target_hash_prefix=node.hash_prefix_hex.upper(),
                target_identity_hex=node.identity_hex,
                notes="Auto-discovered from known repeater node",
            )
            discovered.append(state)
        return discovered

    def _ensure_management_state_for_neighbor(
        self,
        *,
        endpoint_name: str,
        neighbor_identity_hex: str | None,
        neighbor_hash_prefix: str | None,
        neighbor_label: str | None,
    ) -> ManagementTargetState | None:
        if not self.management_config.enabled:
            return None
        existing = self._existing_management_state_for_identity_or_prefix(neighbor_identity_hex, neighbor_hash_prefix)
        if existing is not None:
            return existing
        if neighbor_identity_hex is None and not neighbor_hash_prefix:
            return None

        node = self.nodes_by_identity.get(neighbor_identity_hex or "") if neighbor_identity_hex else None
        safe_name = (node.name if node and node.name else neighbor_label or f"rpt-{neighbor_hash_prefix or 'unknown'}").strip()
        target_name = safe_name
        suffix = (neighbor_hash_prefix or (neighbor_identity_hex or "")[:6] or "AUTO").upper()
        if target_name in self.management_states:
            target_name = f"{safe_name}-{suffix}"
        state = self._register_dynamic_management_target(
            endpoint_name=endpoint_name,
            target_name=target_name,
            target_hash_prefix=neighbor_hash_prefix.upper() if neighbor_hash_prefix else None,
            target_identity_hex=neighbor_identity_hex.lower() if neighbor_identity_hex else None,
            notes="Auto-discovered from repeater neighbor snapshot",
        )
        return state

    def _ensure_management_state_for_advert(self, endpoint: EndpointConfig, advert) -> ManagementTargetState | None:
        if not self.management_config.auto_discover_from_adverts:
            return None
        if advert.role not in ("Repeater", "Room Server"):
            return None

        existing = self._existing_management_state_for_identity_or_prefix(advert.identity_hex, advert.hash_prefix_hex)
        if existing is not None:
            return existing

        safe_name = (advert.name or f"rpt-{advert.hash_prefix_hex}").strip()
        target_name = safe_name
        if target_name in self.management_states:
            target_name = f"{safe_name}-{advert.hash_prefix_hex}"
        state = self._register_dynamic_management_target(
            endpoint_name=endpoint.name,
            target_name=target_name,
            target_hash_prefix=advert.hash_prefix_hex,
            target_identity_hex=advert.identity_hex,
            notes="Auto-discovered from repeater advert",
        )
        if not (self._login_password_candidates(state, "guest") or self._login_password_candidates(state, "admin")):
            state.last_error = "auto-discovered repeater has no guest/admin credential configured"
        self.logger.info("registered auto-discovered management target %s for %s", state.config.name, advert.identity_hex[:12])
        return state

    def _record_neighbor_snapshot(
        self,
        *,
        target_name: str,
        endpoint_name: str,
        requester_role: str | None,
        success: bool,
        error_text: str | None,
        neighbors: list[dict[str, Any]],
        collected_at: datetime | None = None,
    ) -> None:
        self.store.record_neighbor_snapshot(
            target_name=target_name,
            endpoint_name=endpoint_name,
            requester_role=requester_role,
            success=success,
            error_text=error_text,
            neighbors=neighbors,
            collected_at=collected_at,
        )
        self.store.prune_neighbor_history(
            target_name,
            self._runtime_setting_int("neighbor_snapshot_retention", 96, minimum=1),
        )

    def _refresh_management_states(self, *, renamed_from: str | None = None) -> None:
        previous_states = dict(self.management_states)
        refreshed: dict[str, ManagementTargetState] = {}
        for target in self.management_registry.get_targets():
            state = previous_states.get(target.name)
            if state is None and renamed_from:
                state = previous_states.get(renamed_from)
            if state is None:
                state = ManagementTargetState(target, resolved_identity_hex=target.target_identity_hex)
            else:
                state.config = target
            self._resolve_management_identity(state)
            self._seed_management_state_from_known_nodes(state)
            refreshed[target.name] = state
        self.management_states = refreshed
        valid_names = set(refreshed)
        self._management_queue = deque(name for name in self._management_queue if name in valid_names)
        self._management_queued = {name for name in self._management_queued if name in valid_names}
        if self._management_active_name not in valid_names:
            self._management_active_name = None
        if self.management_states:
            self._ensure_management_loop_started()
            for state in self.management_states.values():
                self._queue_management_target(state, "admin-update", force=True)

    def admin_snapshot(self) -> dict[str, Any]:
        return {
            "bot": dict(self.runtime_bot_settings),
            "commands": dict(self.command_settings),
            "channels": [asdict(item) for item in self.config.channels],
            "endpoints": [asdict(item) for item in self.config.endpoints],
            "management_targets": self.management_registry.list_targets(),
            "identity": {
                "public_key_hex": self.identity.public_key_hex,
                "private_key_hex": self.identity.private_key_hex,
                "hash_prefix_hex": self.identity.hash_prefix_hex(),
                "path": self.config.identity.file_path,
            },
            "admin": {
                "password_configured": bool(self.config.admin.password),
            },
        }

    async def update_general_settings(self, updates: dict[str, Any]) -> None:
        async with self._admin_lock:
            merged = dict(self.runtime_bot_settings)
            merged.update(updates)
            self.runtime_bot_settings = merged
            self._apply_runtime_bot_settings(merged)
            self.store.set_json_setting(self.RUNTIME_BOT_SETTINGS_KEY, merged)
            self._resize_message_history()

    async def update_command_settings(self, updates: dict[str, dict[str, Any]]) -> None:
        async with self._admin_lock:
            merged = self._default_command_settings()
            for name, settings in self.command_settings.items():
                if name in merged:
                    merged[name].update(settings)
            for name, settings in updates.items():
                if name not in merged:
                    continue
                merged[name].update(settings)
            self.command_settings = merged
            self.store.set_json_setting(self.RUNTIME_COMMANDS_KEY, merged)

    async def upsert_channel_config(self, channel_payload: dict[str, Any], *, old_name: str | None = None) -> None:
        from .config import ChannelConfig

        async with self._admin_lock:
            channel_name = str(channel_payload["name"]).strip().lower()
            if not channel_name:
                raise ValueError("channel name is required")
            channel = ChannelConfig(
                name=channel_name,
                psk=str(channel_payload["psk"]).strip() or None,
                listen=bool(channel_payload.get("listen", True)),
            )
            channels = [item for item in self.config.channels if not old_name or item.name != old_name.lower()]
            channels = [item for item in channels if item.name != channel.name]
            channels.append(channel)
            channels.sort(key=lambda item: item.name)
            self.config.channels = tuple(channels)
            self._refresh_channel_state()
            self.store.set_json_setting(self.RUNTIME_CHANNELS_KEY, self._serialize_channel_configs(self.config.channels))

    async def delete_channel_config(self, name: str) -> None:
        async with self._admin_lock:
            remaining = [item for item in self.config.channels if item.name != name.lower()]
            if not remaining:
                raise ValueError("at least one channel must remain configured")
            self.config.channels = tuple(remaining)
            self._refresh_channel_state()
            self.store.set_json_setting(self.RUNTIME_CHANNELS_KEY, self._serialize_channel_configs(self.config.channels))

    async def apply_endpoint_configs(self, endpoint_payloads: list[dict[str, Any]]) -> None:
        async with self._admin_lock:
            new_configs = [self._deserialize_endpoint_config(item) for item in endpoint_payloads]
            new_by_name = {item.name: item for item in new_configs if item.enabled}
            current_names = set(self.sessions)
            new_names = set(new_by_name)
            for removed_name in sorted(current_names - new_names):
                await self._stop_endpoint_runtime(removed_name)
                self.sessions.pop(removed_name, None)
            for name, config in new_by_name.items():
                existing = self.sessions.get(name)
                if existing is not None and existing.config == config:
                    continue
                if existing is not None:
                    await self._stop_endpoint_runtime(name)
                session = EndpointSession(config)
                self.sessions[name] = session
                if self._started:
                    self._start_endpoint_task(session)
            self.config.endpoints = tuple(new_configs)
            self.store.set_json_setting(self.RUNTIME_ENDPOINTS_KEY, endpoint_payloads)
            self._refresh_management_states()

    async def upsert_endpoint_config(self, endpoint_payload: dict[str, Any], *, old_name: str | None = None) -> None:
        endpoint_name = str(endpoint_payload["name"]).strip()
        if not endpoint_name:
            raise ValueError("endpoint name is required")
        raw_host = str(endpoint_payload["raw_host"]).strip()
        if not raw_host:
            raise ValueError("raw host is required")
        async with self._admin_lock:
            endpoints = [asdict(item) for item in self.config.endpoints if not old_name or item.name != old_name]
            endpoints = [item for item in endpoints if str(item["name"]) != endpoint_name]
            endpoints.append(
                {
                    "name": endpoint_name,
                    "raw_host": raw_host,
                    "raw_port": int(endpoint_payload.get("raw_port", 5002)),
                    "enabled": bool(endpoint_payload.get("enabled", True)),
                    "console_host": str(endpoint_payload.get("console_host") or "").strip() or None,
                    "console_port": int(endpoint_payload["console_port"]) if endpoint_payload.get("console_port") else None,
                    "console_mirror_host": str(endpoint_payload.get("console_mirror_host") or "").strip() or None,
                    "console_mirror_port": int(endpoint_payload["console_mirror_port"]) if endpoint_payload.get("console_mirror_port") else None,
                    "latitude": float(endpoint_payload["latitude"]) if endpoint_payload.get("latitude") not in (None, "") else None,
                    "longitude": float(endpoint_payload["longitude"]) if endpoint_payload.get("longitude") not in (None, "") else None,
                }
            )
        await self.apply_endpoint_configs(endpoints)

    async def delete_endpoint_config(self, name: str) -> None:
        active_targets = [item for item in self.management_registry.list_targets() if item.get("endpoint_name") == name]
        if active_targets:
            raise ValueError("remove or reassign management targets before deleting this endpoint")
        remaining = [asdict(item) for item in self.config.endpoints if item.name != name]
        if not remaining:
            raise ValueError("at least one endpoint must remain configured")
        await self.apply_endpoint_configs(remaining)

    async def upsert_management_target(self, target_payload: dict[str, Any], *, old_name: str | None = None) -> None:
        async with self._admin_lock:
            target_name = str(target_payload["name"]).strip()
            endpoint_name = str(target_payload["endpoint_name"]).strip()
            if not target_name:
                raise ValueError("target name is required")
            if endpoint_name not in {item.name for item in self.config.endpoints if item.enabled}:
                raise ValueError("target endpoint must reference an enabled endpoint")
            target = ManagementNodeConfig(
                name=target_name,
                endpoint_name=endpoint_name,
                target_hash_prefix=str(target_payload.get("target_hash_prefix") or "").strip().upper() or None,
                target_identity_hex=str(target_payload.get("target_identity_hex") or "").strip().lower() or None,
                guest_password=str(target_payload.get("guest_password") or ""),
                admin_password=str(target_payload.get("admin_password") or "").strip() or None,
                prefer_role=str(target_payload.get("prefer_role") or "guest").strip().lower() or "guest",
                enabled=bool(target_payload.get("enabled", True)),
                notes=str(target_payload.get("notes") or "").strip() or None,
            )
            self.management_registry.upsert_target(target, old_name=old_name)
            self._refresh_management_states(renamed_from=old_name)

    async def delete_management_target(self, name: str) -> None:
        async with self._admin_lock:
            self.management_registry.delete_target(name)
            self._refresh_management_states(renamed_from=name)

    async def regenerate_identity(self) -> None:
        async with self._admin_lock:
            identity = MeshcoreIdentity.generate()
            save_identity(self.config.identity.file_path, identity)
            self.identity = identity
            self.identity_created = False
            for state in self.management_states.values():
                state.current_role = None
                state.last_login_at = None
                state.pending_login_role = None
                state.pending_request = None
                state.learned_path_hashes = None
                self._queue_management_target(state, "identity-rotated", force=True)

    async def send_channel_message(self, endpoint_name: str, channel_name: str, text: str, *, channel_psk: str | None = None) -> None:
        session = self.sessions[endpoint_name]
        if session.raw_writer is None:
            raise RuntimeError(f"endpoint {endpoint_name} is not connected")
        channel = self.channel_by_name.get(channel_name.lower())
        if channel is None:
            raise RuntimeError(f"unknown channel {channel_name}")
        resolved_psk = channel_psk if channel_psk is not None else channel.psk

        payload = build_group_text_packet(
            self.config.bot.name,
            text,
            channel_psk=resolved_psk,
            channel_name=channel.name,
        )
        async with session.channel_tx_lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            remaining = self.MIN_CHANNEL_TX_SPACING_SECONDS - (now - session.last_channel_tx_monotonic)
            if remaining > 0:
                await asyncio.sleep(remaining)
            session.raw_writer.write(encode_frame(payload))
            await session.raw_writer.drain()
            session.last_channel_tx_monotonic = loop.time()

        sent_channel_hash = describe_packet(payload).payload[0] if payload else None
        self.logger.info(
            "queued channel reply on %s/%s hash=%s custom_psk=%s packet_len=%s text_len=%s spacing=%ss: %s",
            endpoint_name,
            channel_name,
            f"{sent_channel_hash:02X}" if sent_channel_hash is not None else "--",
            resolved_psk is not None,
            len(payload),
            len(text),
            self.MIN_CHANNEL_TX_SPACING_SECONDS,
            text,
        )

    async def send_private_message(self, endpoint_name: str, recipient_identity_hex: str, text: str) -> None:
        session = self.sessions[endpoint_name]
        if session.raw_writer is None:
            raise RuntimeError(f"endpoint {endpoint_name} is not connected")
        payload = build_private_text_packet(self.identity, bytes.fromhex(recipient_identity_hex), text)
        session.raw_writer.write(encode_frame(payload))
        await session.raw_writer.drain()
        self.logger.info("sent private reply on %s to %s: %s", endpoint_name, recipient_identity_hex[:12], text)

    async def _run_management_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._management_active_name is not None:
                state = self.management_states.get(self._management_active_name)
                if state is None:
                    self._management_active_name = None
                    continue
                try:
                    await asyncio.wait_for(
                        self._management_wake_event.wait(),
                        timeout=max(1, self.management_config.request_timeout_seconds),
                    )
                except asyncio.TimeoutError:
                    pass
                self._management_wake_event.clear()
                now = datetime.now(tz=UTC)
                self._expire_management_timeouts(state, now)
                if state.pending_login_role or state.pending_request:
                    continue
                if self._target_needs_refresh(state, now):
                    try:
                        await self._drive_management_target(state)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        state.last_error = str(exc)
                        self.logger.warning("management target %s error: %s", state.config.name, exc)
                        self._mark_management_refresh_deferred(state, now, str(exc))
                        continue
                    if state.pending_login_role or state.pending_request:
                        continue
                self._release_management_target(state)
                continue

            if not self._management_queue:
                try:
                    await asyncio.wait_for(
                        self._management_wake_event.wait(),
                        timeout=max(1, self.management_config.poll_interval_seconds),
                    )
                except asyncio.TimeoutError:
                    for state in self.management_states.values():
                        self._queue_management_target(state, "periodic-check")
                self._management_wake_event.clear()
                continue

            name = self._management_queue.popleft()
            self._management_queued.discard(name)
            state = self.management_states.get(name)
            if state is None:
                continue
            now = datetime.now(tz=UTC)
            if not self._target_needs_refresh(state, now):
                continue
            self._management_active_name = name
            try:
                await self._drive_management_target(state)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state.last_error = str(exc)
                self.logger.warning("management target %s error: %s", state.config.name, exc)
                self._mark_management_refresh_deferred(state, now, str(exc))
                continue
            if not state.pending_login_role and not state.pending_request:
                if self._target_needs_refresh(state, datetime.now(tz=UTC)):
                    self._mark_management_refresh_deferred(state, datetime.now(tz=UTC), state.last_error)
                else:
                    self._release_management_target(state)

    async def _drive_management_target(self, state: ManagementTargetState) -> None:
        session = self.sessions.get(state.config.endpoint_name)
        if session is None or session.raw_writer is None or not session.connected:
            return

        now = datetime.now(tz=UTC)
        self._resolve_management_identity(state)
        if state.resolved_identity_hex is None:
            return
        if not self._target_needs_refresh(state, now):
            return

        attempted_console_neighbors = self._should_poll_console_neighbors(session, state, now)
        console_neighbor_success = False
        console_aux_success = False
        if attempted_console_neighbors:
            console_neighbor_success = await self._poll_console_neighbors(session, state) or console_neighbor_success
            now = datetime.now(tz=UTC)
        if self._should_poll_console_owner(session, state, now):
            console_aux_success = await self._poll_console_owner(session, state) or console_aux_success
            now = datetime.now(tz=UTC)
        if self._should_poll_console_acl(session, state, now):
            console_aux_success = await self._poll_console_acl(session, state) or console_aux_success
            now = datetime.now(tz=UTC)
        if console_neighbor_success or (console_aux_success and not attempted_console_neighbors):
            self._mark_management_refresh_success(state, now)
            return

        self._expire_management_timeouts(state, now)
        if state.pending_login_role or state.pending_request:
            return

        desired_role = self._desired_management_role(state, now)
        if desired_role is None:
            state.last_error = "no usable guest/admin password configured"
            return

        if (
            state.last_login_at is None
            or state.current_role != desired_role
            or (now - state.last_login_at).total_seconds() >= self.management_config.login_interval_seconds
        ):
            await self._send_management_login(session, state, desired_role)
            return

        if state.last_status_at is None:
            await self._send_status_request(session, state)
            return

        if state.last_neighbors_at is None or (now - state.last_neighbors_at).total_seconds() >= self.management_config.neighbors_poll_interval_seconds:
            await self._send_neighbors_request(session, state)
            return

        if self._should_poll_acl(state, now):
            if state.current_role != "admin" and state.config.admin_password:
                await self._send_management_login(session, state, "admin")
                return
            if state.current_role == "admin":
                await self._send_acl_request(session, state)
                return

        if state.last_owner_at is None or (now - state.last_owner_at).total_seconds() >= self.management_config.owner_poll_interval_seconds:
            await self._send_owner_request(session, state)
            return

        self._mark_management_refresh_deferred(state, now, state.last_error)

    def _resolve_management_identity(self, state: ManagementTargetState) -> None:
        if state.config.target_identity_hex:
            state.resolved_identity_hex = state.config.target_identity_hex
            return
        prefix = (state.config.target_hash_prefix or "").lower()
        candidates = [identity for identity in self.nodes_by_identity if prefix and identity.startswith(prefix)]
        if not candidates:
            name_matches = [
                identity
                for identity, node in self.nodes_by_identity.items()
                if node.name and node.name.lower() == state.config.name.lower()
            ]
            candidates = name_matches
        if len(candidates) == 1:
            state.resolved_identity_hex = candidates[0]

    def _desired_management_role(self, state: ManagementTargetState, now: datetime) -> str | None:
        guest_candidates = self._login_password_candidates(state, "guest")
        admin_candidates = self._login_password_candidates(state, "admin")

        if state.last_login_at is None:
            if state.login_attempt_index < max(1, len(guest_candidates)) and guest_candidates:
                return "guest"
            if admin_candidates:
                return "admin"
        if state.current_role == "admin" and state.last_login_at is not None:
            if (now - state.last_login_at).total_seconds() < self.management_config.login_interval_seconds:
                return "admin"
        if self._should_poll_acl(state, now) and admin_candidates:
            return "admin"
        if state.config.prefer_role == "admin" and admin_candidates:
            return "admin"
        if guest_candidates:
            return "guest"
        if admin_candidates:
            return "admin"
        return None

    def _fallback_guest_passwords(self, state: ManagementTargetState) -> list[str]:
        if state.config.guest_password is not None:
            return [state.config.guest_password]
        return ["", "hello"]

    def _fallback_admin_passwords(self, state: ManagementTargetState) -> list[str]:
        if state.config.admin_password is not None:
            return [state.config.admin_password]
        temporary_admin_password = self.management_config.temporary_admin_password
        if not temporary_admin_password:
            return []
        upper_name = state.config.name.upper()
        for prefix in self.management_config.temporary_admin_name_prefixes:
            if upper_name.startswith(prefix.upper()):
                return [temporary_admin_password]
        return []

    def _login_password_candidates(self, state: ManagementTargetState, role: str) -> list[str]:
        if role == "admin":
            return self._fallback_admin_passwords(state)
        return self._fallback_guest_passwords(state)

    def _should_poll_acl(self, state: ManagementTargetState, now: datetime) -> bool:
        if not self._login_password_candidates(state, "admin"):
            return False
        return state.last_acl_at is None or (now - state.last_acl_at).total_seconds() >= self.management_config.acl_poll_interval_seconds

    def _should_poll_console_neighbors(self, session: EndpointSession, state: ManagementTargetState, now: datetime) -> bool:
        if session.cli_reader is None or session.cli_writer is None:
            return False
        if state.last_console_neighbors_at is None:
            return True
        return (now - state.last_console_neighbors_at).total_seconds() >= self.management_config.console_neighbors_poll_interval_seconds

    def _should_poll_console_owner(self, session: EndpointSession, state: ManagementTargetState, now: datetime) -> bool:
        if session.cli_reader is None or session.cli_writer is None:
            return False
        if state.last_console_owner_at is None:
            return True
        return (now - state.last_console_owner_at).total_seconds() >= self.management_config.owner_poll_interval_seconds

    def _should_poll_console_acl(self, session: EndpointSession, state: ManagementTargetState, now: datetime) -> bool:
        if not (session.config.console_mirror_host and session.config.console_mirror_port):
            return False
        if state.last_console_acl_at is None:
            return True
        return (now - state.last_console_acl_at).total_seconds() >= self.management_config.acl_poll_interval_seconds

    def _expire_management_timeouts(self, state: ManagementTargetState, now: datetime) -> None:
        timeout_seconds = self.management_config.request_timeout_seconds
        if state.pending_login_role and state.pending_login_at is not None:
            if (now - state.pending_login_at).total_seconds() > timeout_seconds:
                state.last_error = f"login timeout ({state.pending_login_role})"
                state.login_attempt_index += 1
                state.pending_login_role = None
                state.pending_login_password = None
                state.pending_login_at = None
                state.current_role = None
                state.next_retry_at = now + timedelta(seconds=self.management_config.retry_after_failed_poll_seconds)
        if state.pending_request is not None:
            if (now - state.pending_request.sent_at).total_seconds() > timeout_seconds:
                pending = state.pending_request
                state.pending_request = None
                if pending.used_direct:
                    state.learned_path_hashes = None
                    state.last_error = f"{pending.kind} request timeout (direct)"
                else:
                    state.last_error = f"{pending.kind} request timeout"
                if pending.kind == "neighbors":
                    self._record_neighbor_snapshot(
                        target_name=state.config.name,
                        endpoint_name=state.config.endpoint_name,
                        requester_role=pending.requester_role,
                        success=False,
                        error_text=state.last_error,
                        neighbors=[],
                        collected_at=now,
                    )
                    state.last_neighbors_at = now
                elif pending.kind == "owner":
                    state.last_owner_at = now
                elif pending.kind == "acl":
                    self.store.record_acl_snapshot(
                        target_name=state.config.name,
                        endpoint_name=state.config.endpoint_name,
                        requester_role=pending.requester_role,
                        success=False,
                        error_text=state.last_error,
                        entries=[],
                        collected_at=now,
                    )
                    state.last_acl_at = now
                state.next_retry_at = now + timedelta(seconds=self.management_config.retry_after_failed_poll_seconds)

    async def _send_management_login(self, session: EndpointSession, state: ManagementTargetState, role: str) -> None:
        candidates = self._login_password_candidates(state, role)
        if not candidates or state.resolved_identity_hex is None:
            return
        if self._should_send_self_advert(session):
            await self._send_self_advert(session, delay_seconds=0.0)
            await asyncio.sleep(1.0)
        password = candidates[state.login_attempt_index % len(candidates)]
        writer = session.raw_writer
        if writer is None:
            return
        use_direct = state.learned_path_hashes is not None
        route = ROUTE_DIRECT if use_direct else ROUTE_FLOOD
        packet = build_login_packet(
            self.identity,
            bytes.fromhex(state.resolved_identity_hex),
            password,
            route=route,
            path_hashes=tuple(state.learned_path_hashes or ()) if use_direct else (),
        )
        self._record_management_packet_event("tx", session.config.name, packet, note=f"login:{role}")
        writer.write(encode_frame(packet))
        await writer.drain()
        state.pending_login_role = role
        state.pending_login_password = password
        state.pending_login_at = datetime.now(tz=UTC)
        state.last_error = None
        attempt_label = "empty" if password == "" else ("hello" if password == "hello" else "configured")
        route_label = "direct" if use_direct else "flood"
        self.logger.info("sent %s login to %s using %s password via %s", role, state.config.name, attempt_label, route_label)
        self._tasks.append(
            asyncio.create_task(
                self._watch_management_login_timeout(state),
                name=f"mgmt-login-timeout:{state.config.name}",
            )
        )

    async def _watch_management_login_timeout(self, state: ManagementTargetState) -> None:
        sent_at = state.pending_login_at
        if sent_at is None:
            return
        await asyncio.sleep(max(1, self.management_config.request_timeout_seconds) + 0.25)
        if self._stop_event.is_set():
            return
        if state.pending_login_at != sent_at or state.pending_login_role is None:
            return
        self._expire_management_timeouts(state, datetime.now(tz=UTC))
        session = self.sessions.get(state.config.endpoint_name)
        if session is None or session.raw_writer is None or not session.connected:
            return
        self._management_wake_event.set()

    def _should_send_self_advert(self, session: EndpointSession) -> bool:
        if not self.config.bot.self_advert_enabled:
            return False
        if session.raw_writer is None or not session.connected:
            return False
        if session.last_self_advert_at is None:
            return True
        age_seconds = (datetime.now(tz=UTC) - session.last_self_advert_at).total_seconds()
        return age_seconds >= self.config.bot.self_advert_interval_seconds

    async def _send_self_advert(self, session: EndpointSession, *, delay_seconds: float) -> None:
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
        if session.raw_writer is None or not session.connected:
            return
        if not self._should_send_self_advert(session):
            return
        payload = build_advert_packet(
            self.identity,
            name=self.config.bot.name,
            latitude=session.config.latitude,
            longitude=session.config.longitude,
            advert_type=ADV_TYPE_CHAT,
        )
        session.raw_writer.write(encode_frame(payload))
        await session.raw_writer.drain()
        session.last_self_advert_at = datetime.now(tz=UTC)
        self.logger.info("sent self advert on %s as %s", session.config.name, self.config.bot.name)

    async def _prime_console_cli(self, session: EndpointSession) -> None:
        if session.cli_reader is None:
            return
        try:
            data = await self._read_console_until_idle(session.cli_reader, overall_timeout=2.0)
        except Exception:
            return
        text = data.decode("utf-8", errors="replace")
        session.last_cli_reply = normalize_console_reply(text, "")

    async def _read_console_until_idle(
        self,
        reader: asyncio.StreamReader,
        *,
        overall_timeout: float,
        idle_timeout: float = 0.35,
    ) -> bytes:
        chunks: list[bytes] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + overall_timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=min(idle_timeout, remaining))
            except asyncio.TimeoutError:
                break
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)

    async def _run_console_command(self, session: EndpointSession, command: str) -> str:
        if session.cli_reader is None or session.cli_writer is None:
            raise RuntimeError("clean CLI is not connected")
        async with session.cli_lock:
            session.cli_writer.write(command.encode("utf-8") + b"\r")
            await session.cli_writer.drain()
            data = await self._read_console_until_idle(
                session.cli_reader,
                overall_timeout=max(1, self.management_config.console_command_timeout_seconds),
            )
        transcript = data.decode("utf-8", errors="replace")
        reply = normalize_console_reply(transcript, command)
        session.last_cli_command_at = datetime.now(tz=UTC)
        session.last_cli_command = command
        session.last_cli_reply = reply
        session.last_cli_error = None
        return reply

    async def _run_console_mirror_command(self, session: EndpointSession, command: str) -> str:
        host = session.config.console_mirror_host
        port = session.config.console_mirror_port
        if not host or not port:
            raise RuntimeError("console mirror is not configured")
        reader, writer = await asyncio.open_connection(host, port)
        try:
            banner = await self._read_console_until_idle(reader, overall_timeout=1.0)
            writer.write(command.encode("utf-8") + b"\r")
            await writer.drain()
            reply = await self._read_console_until_idle(
                reader,
                overall_timeout=max(1, self.management_config.console_command_timeout_seconds),
            )
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

        transcript = (banner + reply).decode("utf-8", errors="replace")
        for line in transcript.replace("\r", "").split("\n"):
            stripped = line.strip()
            if stripped:
                session.telemetry.ingest_line(stripped)
        return normalize_console_reply(transcript, command)

    async def _poll_console_neighbors(self, session: EndpointSession, state: ManagementTargetState) -> bool:
        now = datetime.now(tz=UTC)
        try:
            reply = await self._run_console_command(session, "neighbors")
        except Exception as exc:
            session.last_cli_error = str(exc)
            state.last_console_neighbors_at = now
            state.last_error = f"console neighbors failed: {exc}"
            return False

        parsed_neighbors = parse_console_neighbors_reply(reply)
        neighbor_rows = []
        for entry in parsed_neighbors:
            prefix_hex = str(entry["neighbor_hash_prefix"])
            identity_hex = self._resolve_neighbor_identity(prefix_hex.lower())
            label = self._display_name_for_identity(identity_hex) if identity_hex else prefix_hex
            neighbor_rows.append(
                {
                    "neighbor_hash_prefix": prefix_hex,
                    "neighbor_identity_hex": identity_hex,
                    "snr": entry["snr"],
                    "rssi": None,
                    "last_heard_seconds": entry["last_heard_seconds"],
                    "label": label,
                }
            )
        self._record_neighbor_snapshot(
            target_name=state.config.name,
            endpoint_name=state.config.endpoint_name,
            requester_role="console",
            success=True,
            error_text=None,
            neighbors=neighbor_rows,
            collected_at=now,
        )
        for row in neighbor_rows:
            discovered_state = self._ensure_management_state_for_neighbor(
                endpoint_name=state.config.endpoint_name,
                neighbor_identity_hex=row.get("neighbor_identity_hex"),
                neighbor_hash_prefix=row.get("neighbor_hash_prefix"),
                neighbor_label=row.get("label"),
            )
            if discovered_state is not None:
                self._queue_management_target(discovered_state, "neighbor-discovered")
        state.neighbor_count = len(neighbor_rows)
        state.last_neighbors_at = now
        state.last_console_neighbors_at = now
        state.last_error = None
        self.logger.info("console neighbors from %s count=%d", state.config.name, len(neighbor_rows))
        return True

    async def _poll_console_owner(self, session: EndpointSession, state: ManagementTargetState) -> bool:
        now = datetime.now(tz=UTC)
        try:
            firmware_version = parse_console_owner_reply(await self._run_console_command(session, "ver")) or None
            node_name = parse_console_owner_reply(await self._run_console_command(session, "get name")) or None
            owner_text = parse_console_owner_reply(await self._run_console_command(session, "get owner.info")) or None
        except Exception as exc:
            session.last_cli_error = str(exc)
            state.last_console_owner_at = now
            state.last_error = f"console owner failed: {exc}"
            return False

        owner_info = owner_text.replace("|", "\n") if owner_text else None
        raw_text = "\n".join(item for item in (firmware_version, node_name, owner_info) if item)
        self.store.record_owner_snapshot(
            target_name=state.config.name,
            endpoint_name=state.config.endpoint_name,
            requester_role="console",
            firmware_version=firmware_version,
            node_name=node_name,
            owner_info=owner_info,
            raw_text=raw_text,
            collected_at=now,
        )
        state.owner_info = {
            "firmware_version": firmware_version,
            "node_name": node_name,
            "owner_info": owner_info,
            "raw_text": raw_text,
        }
        state.last_owner_at = now
        state.last_console_owner_at = now
        state.last_error = None
        return True

    async def _poll_console_acl(self, session: EndpointSession, state: ManagementTargetState) -> bool:
        now = datetime.now(tz=UTC)
        try:
            reply = await self._run_console_mirror_command(session, "get acl")
            entries = parse_console_acl_reply(reply)
            self.store.record_acl_snapshot(
                target_name=state.config.name,
                endpoint_name=state.config.endpoint_name,
                requester_role="console",
                success=True,
                error_text=None,
                entries=entries,
                collected_at=now,
            )
            state.acl_entry_count = len(entries)
            state.last_acl_at = now
            state.last_console_acl_at = now
            state.last_error = None
            return True
        except Exception as exc:
            self.store.record_acl_snapshot(
                target_name=state.config.name,
                endpoint_name=state.config.endpoint_name,
                requester_role="console",
                success=False,
                error_text=str(exc),
                entries=[],
                collected_at=now,
            )
            state.last_console_acl_at = now
            state.last_error = f"console acl failed: {exc}"
            return False

    def _record_management_packet_event(self, direction: str, endpoint_name: str, payload: bytes, note: str | None = None) -> None:
        try:
            summary = describe_packet(payload)
        except Exception:
            return
        self.management_packet_trace.appendleft(
            {
                "observed_at": datetime.now(tz=UTC).isoformat(),
                "direction": direction,
                "endpoint_name": endpoint_name,
                "packet_type": summary.packet_type_name,
                "route": summary.route_name,
                "path_hashes": summary.path_hashes,
                "path_len": summary.path_len,
                "payload_len": len(summary.payload),
                "payload_hex": summary.payload.hex()[:256],
                "raw_hex": payload.hex()[:256],
                "note": note,
            }
        )

    async def _send_owner_request(self, session: EndpointSession, state: ManagementTargetState) -> None:
        await self._send_management_request(session, state, "owner", bytes([REQ_TYPE_GET_OWNER_INFO]))

    async def _send_status_request(self, session: EndpointSession, state: ManagementTargetState) -> None:
        await self._send_management_request(
            session,
            state,
            "status",
            bytes([REQ_TYPE_GET_STATUS, 0x00, 0x00, 0x00, 0x00]),
            route=ROUTE_FLOOD,
        )

    async def _send_acl_request(self, session: EndpointSession, state: ManagementTargetState) -> None:
        await self._send_management_request(session, state, "acl", bytes([REQ_TYPE_GET_ACCESS_LIST, 0x00, 0x00, 0x00, 0x00]))

    async def _send_neighbors_request(self, session: EndpointSession, state: ManagementTargetState) -> None:
        payload = build_neighbors_request_payload(
            count=self.management_config.neighbors_request_count,
            pubkey_prefix_length=self.management_config.neighbors_prefix_length,
        )
        await self._send_management_request(
            session,
            state,
            "neighbors",
            payload,
            meta={"prefix_length": self.management_config.neighbors_prefix_length},
        )

    async def _send_management_request(
        self,
        session: EndpointSession,
        state: ManagementTargetState,
        kind: str,
        payload: bytes,
        *,
        meta: dict[str, Any] | None = None,
        route: int = ROUTE_FLOOD,
    ) -> None:
        if state.resolved_identity_hex is None:
            return
        writer = session.raw_writer
        if writer is None:
            return
        use_direct = state.learned_path_hashes is not None
        tag, packet = build_request_packet(
            self.identity,
            bytes.fromhex(state.resolved_identity_hex),
            payload,
            route=ROUTE_DIRECT if use_direct else route,
            path_hashes=tuple(state.learned_path_hashes or ()) if use_direct else (),
        )
        self._record_management_packet_event("tx", session.config.name, packet, note=f"request:{kind}")
        writer.write(encode_frame(packet))
        await writer.drain()
        state.pending_request = PendingManagementRequest(
            kind=kind,
            tag=tag,
            sent_at=datetime.now(tz=UTC),
            requester_role=state.current_role or "unknown",
            used_direct=use_direct,
            meta=meta or {},
        )
        state.last_error = None
        route_label = "direct" if use_direct or route == ROUTE_DIRECT else "flood"
        self.logger.info("sent %s request to %s tag=%08x via %s", kind, state.config.name, tag, route_label)
        self._tasks.append(
            asyncio.create_task(
                self._watch_management_request_timeout(state),
                name=f"mgmt-request-timeout:{state.config.name}:{kind}",
            )
        )

    async def _watch_management_request_timeout(self, state: ManagementTargetState) -> None:
        pending = state.pending_request
        if pending is None:
            return
        sent_at = pending.sent_at
        await asyncio.sleep(max(1, self.management_config.request_timeout_seconds) + 0.25)
        if self._stop_event.is_set():
            return
        current = state.pending_request
        if current is None or current.sent_at != sent_at:
            return
        self._expire_management_timeouts(state, datetime.now(tz=UTC))
        session = self.sessions.get(state.config.endpoint_name)
        if session is None or session.raw_writer is None or not session.connected:
            return
        self._management_wake_event.set()

    def _record_management_path(self, sender_identity_hex: str, endpoint_name: str, path_hashes: list[str]) -> None:
        state = self._find_management_state(sender_identity_hex, endpoint_name)
        if state is None:
            return
        if state.learned_path_hashes == path_hashes:
            return
        state.learned_path_hashes = list(path_hashes)
        self.logger.info("learned path for %s: %s", state.config.name, "-".join(path_hashes) if path_hashes else "zero-hop")

    def _handle_management_response(self, session: EndpointSession, decoded) -> None:
        state = self._find_management_state(decoded.sender_identity_hex, session.config.name)
        if state is None:
            return
        now = datetime.now(tz=UTC)

        if state.pending_login_role is not None:
            response = parse_login_response(decoded.plaintext)
            state.pending_login_role, pending_role = None, state.pending_login_role
            pending_password = state.pending_login_password
            state.pending_login_password = None
            state.pending_login_at = None
            if response is None or not response.success:
                state.current_role = None
                state.last_error = "login rejected"
                state.login_attempt_index += 1
                if pending_password == "hello":
                    state.last_error = "login rejected (empty/hello tried)"
                state.next_retry_at = now + timedelta(seconds=self.management_config.retry_after_failed_poll_seconds)
                self._management_wake_event.set()
                return
            inferred_role = "admin" if response.is_admin or ((response.permissions or 0) & 0x03) == 0x01 else (pending_role or "guest")
            state.current_role = inferred_role
            state.last_login_at = now
            state.login_attempt_index = 0
            state.last_error = None
            self.logger.info("login ok for %s as %s", state.config.name, inferred_role)
            self._management_wake_event.set()
            return

        parsed = parse_tagged_response(decoded.plaintext)
        if parsed is None or state.pending_request is None:
            return
        tag, body = parsed
        pending = state.pending_request
        if pending.tag != tag:
            return
        state.pending_request = None

        if pending.kind == "owner":
            owner = parse_owner_info_response(body)
            self.store.record_owner_snapshot(
                target_name=state.config.name,
                endpoint_name=state.config.endpoint_name,
                requester_role=pending.requester_role,
                firmware_version=owner.firmware_version,
                node_name=owner.node_name,
                owner_info=owner.owner_info,
                raw_text=owner.raw_text,
                collected_at=now,
            )
            state.owner_info = asdict(owner)
            state.last_owner_at = now
            state.last_error = None
            self._merge_owner_info(state, owner)
            self._management_wake_event.set()
            return

        if pending.kind == "status":
            state.last_status_at = now
            state.last_status_size = len(body)
            state.last_error = None
            self.logger.info("status response from %s len=%d", state.config.name, len(body))
            self._management_wake_event.set()
            return

        if pending.kind == "acl":
            acl_entries = parse_acl_response(body)
            self.store.record_acl_snapshot(
                target_name=state.config.name,
                endpoint_name=state.config.endpoint_name,
                requester_role=pending.requester_role,
                success=True,
                error_text=None,
                entries=[asdict(item) for item in acl_entries],
                collected_at=now,
            )
            state.acl_entry_count = len(acl_entries)
            state.last_acl_at = now
            state.last_error = None
            self._management_wake_event.set()
            return

        if pending.kind == "neighbors":
            prefix_length = int(pending.meta.get("prefix_length", 6))
            result = parse_neighbors_response(body, prefix_length)
            if result is None:
                state.last_error = "invalid neighbors response"
                state.next_retry_at = now + timedelta(seconds=self.management_config.retry_after_failed_poll_seconds)
                self._management_wake_event.set()
                return
            neighbor_rows = []
            for entry in result.entries:
                identity_hex = self._resolve_neighbor_identity(entry.neighbor_hash_prefix)
                label = self._display_name_for_identity(identity_hex) if identity_hex else entry.neighbor_hash_prefix
                neighbor_rows.append(
                    {
                        "neighbor_hash_prefix": entry.neighbor_hash_prefix,
                        "neighbor_identity_hex": identity_hex,
                        "snr": entry.snr,
                        "rssi": None,
                        "last_heard_seconds": entry.last_heard_seconds,
                        "label": label,
                    }
                )
            self._record_neighbor_snapshot(
                target_name=state.config.name,
                endpoint_name=state.config.endpoint_name,
                requester_role=pending.requester_role,
                success=True,
                error_text=None,
                neighbors=neighbor_rows,
                collected_at=now,
            )
            state.neighbor_count = len(neighbor_rows)
            state.last_neighbors_at = now
            state.last_error = None
            self._mark_management_refresh_success(state, now)
            return

    def _merge_owner_info(self, state: ManagementTargetState, owner) -> None:
        if state.resolved_identity_hex is None:
            return
        node = self.nodes_by_identity.get(state.resolved_identity_hex)
        if node is None:
            return
        if owner.node_name:
            node.name = owner.node_name
            self.nodes_by_name[owner.node_name] = state.resolved_identity_hex

    def _resolve_neighbor_identity(self, prefix_hex: str) -> str | None:
        matches = [identity for identity in self.nodes_by_identity if identity.startswith(prefix_hex.lower())]
        if len(matches) == 1:
            return matches[0]
        return None

    def _find_management_state(self, identity_hex: str, endpoint_name: str) -> ManagementTargetState | None:
        for state in self.management_states.values():
            if state.config.endpoint_name != endpoint_name:
                continue
            if state.resolved_identity_hex == identity_hex:
                return state
            if state.config.target_identity_hex == identity_hex:
                return state
        return None

    def _management_state_summary(self, state: ManagementTargetState) -> dict[str, Any]:
        return {
            "name": state.config.name,
            "endpoint_name": state.config.endpoint_name,
            "resolved_identity_hex": state.resolved_identity_hex,
            "last_observed_advert_at": state.last_observed_advert_at.isoformat() if state.last_observed_advert_at else None,
            "last_successful_advert_at": state.last_successful_advert_at.isoformat() if state.last_successful_advert_at else None,
            "next_retry_at": state.next_retry_at.isoformat() if state.next_retry_at else None,
            "queue_reason": state.queue_reason,
            "learned_path_hashes": list(state.learned_path_hashes) if state.learned_path_hashes is not None else None,
            "current_role": state.current_role,
            "last_login_at": state.last_login_at.isoformat() if state.last_login_at else None,
            "last_status_at": state.last_status_at.isoformat() if state.last_status_at else None,
            "last_status_size": state.last_status_size,
            "pending_login_role": state.pending_login_role,
            "pending_request": state.pending_request.kind if state.pending_request else None,
            "last_owner_at": state.last_owner_at.isoformat() if state.last_owner_at else None,
            "last_acl_at": state.last_acl_at.isoformat() if state.last_acl_at else None,
            "last_neighbors_at": state.last_neighbors_at.isoformat() if state.last_neighbors_at else None,
            "last_console_neighbors_at": state.last_console_neighbors_at.isoformat() if state.last_console_neighbors_at else None,
            "last_console_owner_at": state.last_console_owner_at.isoformat() if state.last_console_owner_at else None,
            "last_console_acl_at": state.last_console_acl_at.isoformat() if state.last_console_acl_at else None,
            "owner_info": state.owner_info,
            "acl_entry_count": state.acl_entry_count,
            "neighbor_count": state.neighbor_count,
            "last_error": state.last_error,
        }

    def _node_status_map(self) -> dict[str, dict[str, Any]]:
        statuses: dict[str, dict[str, Any]] = {}
        for state in self.management_states.values():
            identity_hex = state.resolved_identity_hex or state.config.target_identity_hex
            if identity_hex is None:
                continue
            statuses[identity_hex] = {
                "target_name": state.config.name,
                "data_fetch_ok": bool(
                    state.last_successful_advert_at is not None
                    and state.last_observed_advert_at is not None
                    and state.last_successful_advert_at >= state.last_observed_advert_at
                ),
                "last_successful_advert_at": state.last_successful_advert_at.isoformat() if state.last_successful_advert_at else None,
                "next_retry_at": state.next_retry_at.isoformat() if state.next_retry_at else None,
                "last_error": state.last_error,
                "is_queued": state.config.name in self._management_queued,
                "is_active": self._management_active_name == state.config.name,
            }
        return statuses

    def _is_reasonable_map_coordinate(self, latitude: float | None, longitude: float | None) -> bool:
        if latitude is None or longitude is None:
            return False
        if abs(latitude) < 0.01 and abs(longitude) < 0.01:
            return False
        if not (-85.0 <= latitude <= 85.0 and -180.0 <= longitude <= 180.0):
            return False
        return True

    def _build_management_links(self, latest_neighbor_details: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        links: list[dict[str, Any]] = []
        if latest_neighbor_details is None:
            latest_neighbor_details = self.store.latest_neighbor_details(limit_targets=12)
        for snapshot in latest_neighbor_details:
            state = self.management_states.get(snapshot["target_name"])
            if state is None:
                continue
            source_node = self.nodes_by_identity.get(state.resolved_identity_hex or "")
            source_lat = source_node.latitude if source_node and source_node.latitude is not None else None
            source_lon = source_node.longitude if source_node and source_node.longitude is not None else None
            if not self._is_reasonable_map_coordinate(source_lat, source_lon):
                endpoint = self.sessions.get(snapshot["endpoint_name"])
                if endpoint and endpoint.config.latitude is not None and endpoint.config.longitude is not None:
                    source_lat = endpoint.config.latitude
                    source_lon = endpoint.config.longitude
            if not self._is_reasonable_map_coordinate(source_lat, source_lon):
                continue

            source_name = source_node.name if source_node and source_node.name else snapshot["target_name"]
            for edge in snapshot["edges"]:
                target_identity_hex = edge.get("neighbor_identity_hex") or self._resolve_neighbor_identity(edge["neighbor_hash_prefix"])
                if target_identity_hex is None:
                    continue
                target_node = self.nodes_by_identity.get(target_identity_hex)
                if target_node is None:
                    continue
                if target_node.role not in ("Repeater", "Room Server"):
                    continue
                if not self._is_reasonable_map_coordinate(target_node.latitude, target_node.longitude):
                    continue
                source_latitude = source_lat
                source_longitude = source_lon
                target_latitude = target_node.latitude
                target_longitude = target_node.longitude
                if (
                    source_latitude is None
                    or source_longitude is None
                    or target_latitude is None
                    or target_longitude is None
                ):
                    continue
                snr = edge.get("snr")
                target_last_advert_at = target_node.last_advert_at.isoformat() if target_node.last_advert_at else None
                link_distance_km = calculate_distance_km(
                    (source_latitude, source_longitude),
                    (target_latitude, target_longitude),
                )
                links.append(
                    {
                        "source_name": source_name,
                        "source_identity_hex": state.resolved_identity_hex,
                        "source_latitude": source_latitude,
                        "source_longitude": source_longitude,
                        "target_name": target_node.name or edge.get("label") or edge["neighbor_hash_prefix"],
                        "target_identity_hex": target_identity_hex,
                        "target_latitude": target_latitude,
                        "target_longitude": target_longitude,
                        "snr": snr,
                        "rssi": edge.get("rssi"),
                        "last_heard_seconds": edge.get("last_heard_seconds"),
                        "collected_at": snapshot["collected_at"],
                        "target_last_advert_at": target_last_advert_at,
                        "distance_km": link_distance_km,
                        "quality": self._classify_link_quality(snr),
                    }
                )
        return links

    def _build_signal_history(self, limit_targets: int = 12, limit_snapshots: int = 32, latest_neighbor_details: list[dict[str, Any]] | None = None) -> dict[str, list[dict[str, Any]]]:
        limit_targets = self._runtime_setting_int("signal_history_target_limit", limit_targets, minimum=1)
        limit_snapshots = self._runtime_setting_int("signal_history_limit", limit_snapshots, minimum=2)
        history: dict[str, list[dict[str, Any]]] = {}
        if latest_neighbor_details is None:
            latest_neighbor_details = self.store.latest_neighbor_details(limit_targets=limit_targets)
        for snapshot in latest_neighbor_details:
            target_name = snapshot["target_name"]
            rows = self.store.neighbor_signal_history(target_name, limit_snapshots=limit_snapshots)
            history[target_name] = rows
        return history

    def _classify_link_quality(self, snr: float | None) -> str:
        if snr is None:
            return "unknown"
        if snr >= 10:
            return "excellent"
        if snr >= 5:
            return "good"
        if snr >= 0:
            return "fair"
        return "poor"

    def snapshot(self) -> RuntimeSnapshot:
        endpoint_data: dict[str, dict[str, object]] = {}
        for name, session in self.sessions.items():
            endpoint_data[name] = {
                "connected": session.connected,
                "raw_host": session.config.raw_host,
                "raw_port": session.config.raw_port,
                "console_port": session.config.console_port,
                "console_mirror_port": session.config.console_mirror_port,
                "last_error": session.last_error,
                "last_seen_at": session.last_seen_at.isoformat() if session.last_seen_at else None,
                "last_cli_command_at": session.last_cli_command_at.isoformat() if session.last_cli_command_at else None,
                "last_cli_command": session.last_cli_command,
                "last_cli_reply": session.last_cli_reply,
                "last_cli_error": session.last_cli_error,
                "recent_console_lines": session.telemetry.recent_lines(),
            }
        node_status_map = self._node_status_map()
        nodes: list[dict[str, Any]] = []
        for node in sorted(self.nodes_by_identity.values(), key=lambda item: item.name or item.identity_hex):
            item = asdict(node)
            item.update(node_status_map.get(node.identity_hex, {
                "target_name": None,
                "data_fetch_ok": False,
                "last_successful_advert_at": None,
                "next_retry_at": None,
                "last_error": None,
                "is_queued": False,
                "is_active": False,
            }))
            nodes.append(item)
        messages = [asdict(item) for item in list(self.messages)]
        diagnostics = {
            "total_packets_seen": self.total_packets_seen,
            "total_group_text_seen": self.total_group_text_seen,
            "total_group_text_decoded": self.total_group_text_decoded,
            "total_adverts_seen": self.total_adverts_seen,
            "last_packet_summary": self.last_packet_summary,
            "last_drop_reason": self.last_drop_reason,
            "management_active_target": self._management_active_name,
            "management_queue": list(self._management_queue),
            "management_packet_trace": list(self.management_packet_trace),
        }
        # Cache the latest_neighbor_details to avoid duplicate database queries
        cached_neighbor_details = self.store.latest_neighbor_details(limit_targets=12)
        return RuntimeSnapshot(
            started_at=self.started_at,
            endpoints=endpoint_data,
            nodes=nodes,
            messages=messages,
            diagnostics=diagnostics,
            identity={
                "public_key_hex": self.identity.public_key_hex,
                "hash_prefix_hex": self.identity.hash_prefix_hex(),
                "file_path": self.config.identity.file_path,
                "created_this_run": self.identity_created,
            },
            persistence=self.store.snapshot_overview(),
            management={
                "targets": self.management_registry.list_targets(),
                "sessions": [self._management_state_summary(state) for state in self.management_states.values()],
                "latest_neighbors": self.store.recent_neighbor_summary(limit=10),
                "latest_owners": self.store.recent_owner_summary(limit=6),
                "latest_acl": self.store.recent_acl_summary(limit=6),
                "map_links": self._build_management_links(cached_neighbor_details),
                "signal_history": self._build_signal_history(latest_neighbor_details=cached_neighbor_details),
            },
        )