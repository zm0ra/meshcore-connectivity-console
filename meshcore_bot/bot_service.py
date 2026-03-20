from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .channels import derive_hashtag_secret
from .config import AppConfig, EndpointConfig
from .database import BotDatabase, utc_now_iso
from .gateway_client import GatewayTransportClient
from .identity import LocalIdentity
from .mesh_builders import (
    GroupText,
    build_group_text_packet,
    parse_group_text,
    split_sender_and_message,
)
from .mesh_packets import PayloadType, RouteType
from .tcp_client import ReceivedPacket
from .transport import PacketTransportClient


@dataclass(slots=True)
class ChannelBinding:
    name: str
    secret: bytes


@dataclass(slots=True)
class PendingReply:
    task: asyncio.Task[None]
    echo_event: asyncio.Event
    expected_texts: frozenset[str]
    planned_attempts: int
    sent_attempts: int = 0


class ChannelCommandBotService:
    RECEIVE_IDLE_TIMEOUT_SECS = 60.0

    def __init__(
        self,
        config: AppConfig,
        database: BotDatabase,
        *,
        transport_factory: Callable[[EndpointConfig], PacketTransportClient] | None = None,
    ) -> None:
        self.config = config
        self.database = database
        self.identity = LocalIdentity.load_or_create(config.identity.key_file_path)
        self.logger = logging.getLogger(f"{config.service.name}.bot")
        self.sender_name = config.bot.sender_name or config.service.name
        self.channel_bindings = tuple(
            ChannelBinding(name=channel_name, secret=derive_hashtag_secret(channel_name))
            for channel_name in config.bot.channels
        )
        self._channel_secrets = {binding.name: binding.secret for binding in self.channel_bindings}
        self._enabled_commands = tuple(dict.fromkeys(command.lower() for command in config.bot.enabled_commands))
        self.HELP_RESPONSE = " ".join(self._enabled_commands)
        self.MIN_RESPONSE_DELAY_SECS = config.bot.min_response_delay_secs
        self.COMMAND_DEDUP_TTL_SECS = config.bot.command_dedup_ttl_secs
        self.QUIET_WINDOW_SECS = config.bot.quiet_window_secs
        self.RESPONSE_ATTEMPTS = max(1, config.bot.response_attempts)
        self.RESPONSE_ATTEMPTS_MAX = max(self.RESPONSE_ATTEMPTS, config.bot.response_attempts_max)
        self.ECHO_ACK_TIMEOUT_SECS = max(0.0, config.bot.echo_ack_timeout_secs)
        self.RESPONSE_RETRY_DELAY_SECS = config.bot.response_retry_delay_secs
        self.RESPONSE_RETRY_BACKOFF_MULTIPLIER = max(1.0, config.bot.response_retry_backoff_multiplier)
        self.RESPONSE_RETRY_MAX_DELAY_SECS = max(0.0, config.bot.response_retry_max_delay_secs)
        self.INCLUDE_TEST_SIGNAL = config.bot.include_test_signal
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._transport_factory = transport_factory or self._build_gateway_transport
        self._send_locks: dict[str, asyncio.Lock] = {}
        self._recent_commands: dict[tuple[str, str, int, str], datetime] = {}
        self._pending_replies: dict[tuple[str, str, int, str], PendingReply] = {}
        self._adaptive_response_attempts: dict[tuple[str, str], int] = {}

    async def run(self) -> None:
        self.database.initialize()
        if not self.config.bot.enabled:
            self.logger.warning("bot worker disabled in config")
            await self._stop_event.wait()
            return
        if not self.channel_bindings:
            self.logger.warning("bot worker has no configured channels")
            await self._stop_event.wait()
            return
        enabled_endpoints = [endpoint for endpoint in self.config.endpoints if endpoint.enabled]
        self._tasks = [
            asyncio.create_task(self._run_endpoint(endpoint), name=f"bot:{endpoint.name}")
            for endpoint in enabled_endpoints
        ]
        if not self._tasks:
            self.logger.warning("no enabled endpoints configured for bot")
            await self._stop_event.wait()
            return
        await asyncio.gather(*self._tasks)

    async def stop(self) -> None:
        self._stop_event.set()
        for pending in self._pending_replies.values():
            pending.task.cancel()
        for task in self._tasks:
            task.cancel()
        all_tasks = [*self._tasks, *(pending.task for pending in self._pending_replies.values())]
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

    def _build_gateway_transport(self, endpoint: EndpointConfig) -> PacketTransportClient:
        return GatewayTransportClient(
            endpoint_name=endpoint.name,
            control_socket_path=self.config.gateway.control_socket_path,
            event_socket_path=self.config.gateway.event_socket_path,
            traffic_class="bot",
        )

    async def _run_endpoint(self, endpoint: EndpointConfig) -> None:
        while not self._stop_event.is_set():
            client = self._transport_factory(endpoint)
            try:
                await client.connect()
                self.logger.info("[BOT-CONNECT] endpoint=%s host=%s port=%s", endpoint.name, endpoint.raw_host, endpoint.raw_port)
                while not self._stop_event.is_set():
                    try:
                        packet = await client.receive_packet(timeout=self.RECEIVE_IDLE_TIMEOUT_SECS)
                    except asyncio.TimeoutError:
                        continue
                    await self._handle_packet(endpoint, client, packet)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("bot endpoint %s failed: %s", endpoint.name, exc)
                await asyncio.sleep(3.0)
            finally:
                self._cancel_pending_replies_for_endpoint(endpoint.name)
                await client.close()

    async def _handle_packet(self, endpoint: EndpointConfig, client: PacketTransportClient, packet: ReceivedPacket) -> None:
        if packet.summary.payload_type is not PayloadType.GRP_TXT:
            return
        await self._handle_channel_packet(endpoint, client, packet)

    async def _handle_channel_packet(self, endpoint: EndpointConfig, client: PacketTransportClient, packet: ReceivedPacket) -> None:
        decoded = self._decode_channel_packet(packet.summary)
        if decoded is None:
            return
        channel_name, group_text = decoded
        sender_name, message = split_sender_and_message(group_text.text)
        if sender_name is None:
            return
        if sender_name == self.sender_name:
            self._acknowledge_pending_reply(endpoint.name, channel_name, group_text.timestamp, group_text.text)
            return
        if not message.startswith("!"):
            return
        command = message.strip().split(None, 1)[0].lower()
        if command not in self._enabled_commands:
            return
        command_key = (endpoint.name, channel_name, group_text.timestamp, group_text.text)
        if self._is_duplicate_command(command_key):
            self.logger.debug(
                "[BOT-DUP] endpoint=%s channel=%s ts=%s text=%s",
                endpoint.name,
                channel_name,
                group_text.timestamp,
                message,
            )
            return
        self.logger.info(
            "[BOT-RX] endpoint=%s channel=%s from=%s path_len=%s ts=%s text=%s",
            endpoint.name,
            channel_name,
            sender_name,
            packet.summary.path_len,
            group_text.timestamp,
            message,
        )
        await self._activate_quiet_window(client, channel_name)
        await asyncio.sleep(self.MIN_RESPONSE_DELAY_SECS)
        reply = self._build_reply(
            sender_name=sender_name,
            message=message,
            path_len=packet.summary.path_len,
            sent_at=datetime.now(tz=UTC),
        )
        if reply is None:
            return
        self._schedule_channel_reply(endpoint, client, channel_name, reply)

    def _is_duplicate_command(self, command_key: tuple[str, str, int, str]) -> bool:
        now = datetime.now(tz=UTC)
        cutoff = now - timedelta(seconds=self.COMMAND_DEDUP_TTL_SECS)
        stale_keys = [key for key, seen_at in self._recent_commands.items() if seen_at < cutoff]
        for stale_key in stale_keys:
            del self._recent_commands[stale_key]
        if command_key in self._recent_commands:
            return True
        self._recent_commands[command_key] = now
        return False

    def _decode_channel_packet(self, summary) -> tuple[str, GroupText] | None:
        for binding in self.channel_bindings:
            decoded = parse_group_text(summary, channel_secret=binding.secret)
            if decoded is not None:
                return binding.name, decoded
        return None

    def _build_reply(self, *, sender_name: str, message: str, path_len: int, sent_at: datetime) -> str | None:
        command = message.strip().split(None, 1)[0].lower()
        mention = f"@[{sender_name}]"
        sent_at_text = sent_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        if command == "!help":
            return self.HELP_RESPONSE
        if command == "!ping":
            return f"pong {mention} {sent_at_text}"
        if command != "!test":
            return None
        signal = self.database.latest_repeater_signal_by_name(sender_name) if self.INCLUDE_TEST_SIGNAL else None
        parts = [mention, f"hops: {path_len}", sent_at_text]
        if signal is not None:
            last_snr = signal.get("last_snr")
            last_rssi = signal.get("last_rssi")
            if isinstance(last_snr, int | float):
                parts.append(f"SNR: {float(last_snr):.1f}")
            if isinstance(last_rssi, int | float):
                parts.append(f"RSSI: {int(last_rssi)}")
        return " ".join(parts)

    async def _activate_quiet_window(self, client: PacketTransportClient, channel_name: str) -> None:
        activate = getattr(client, "activate_quiet_window", None)
        if activate is None or self.QUIET_WINDOW_SECS <= 0:
            return
        await activate(seconds=self.QUIET_WINDOW_SECS)
        self.logger.info("[BOT-PRIORITY] channel=%s quiet_window=%.2fs", channel_name, self.QUIET_WINDOW_SECS)

    def _schedule_channel_reply(self, endpoint: EndpointConfig, client: PacketTransportClient, channel_name: str, reply: str) -> None:
        wire_timestamp = int(datetime.now(tz=UTC).timestamp()) & 0xFFFFFFFF
        full_text = f"{self.sender_name}: {reply}"
        reply_key = (endpoint.name, channel_name, wire_timestamp, full_text)
        echo_event = asyncio.Event()
        planned_attempts = self._planned_response_attempts(endpoint.name, channel_name)
        expected_texts = frozenset(
            f"{self.sender_name}: {self._format_reply_attempt(reply, attempt + 1, planned_attempts)}"
            for attempt in range(planned_attempts)
        )
        task = asyncio.create_task(
            self._send_channel_reply(endpoint, client, channel_name, reply, wire_timestamp, reply_key, echo_event, planned_attempts),
            name=f"bot-reply:{endpoint.name}:{wire_timestamp}",
        )
        self._pending_replies[reply_key] = PendingReply(
            task=task,
            echo_event=echo_event,
            expected_texts=expected_texts,
            planned_attempts=planned_attempts,
        )

    def _acknowledge_pending_reply(self, endpoint_name: str, channel_name: str, wire_timestamp: int, decoded_text: str) -> None:
        reply_key, pending = self._find_pending_reply(endpoint_name, channel_name, wire_timestamp, decoded_text)
        if pending is None or reply_key is None:
            return
        pending.echo_event.set()
        if not pending.task.done():
            pending.task.cancel()
        self.logger.info(
            "[BOT-ECHO] endpoint=%s channel=%s ts=%s attempts=%s",
            endpoint_name,
            channel_name,
            wire_timestamp,
            pending.sent_attempts,
        )

    def _find_pending_reply(
        self,
        endpoint_name: str,
        channel_name: str,
        wire_timestamp: int,
        decoded_text: str,
    ) -> tuple[tuple[str, str, int, str] | None, PendingReply | None]:
        for reply_key, pending in self._pending_replies.items():
            if reply_key[0] != endpoint_name or reply_key[1] != channel_name or reply_key[2] != wire_timestamp:
                continue
            if decoded_text in pending.expected_texts:
                return reply_key, pending
        return None, None

    def _format_reply_attempt(self, reply: str, attempt_number: int, total_attempts: int) -> str:
        return f"{reply} tx {attempt_number}/{total_attempts}"

    def _planned_response_attempts(self, endpoint_name: str, channel_name: str) -> int:
        return self._adaptive_response_attempts.get((endpoint_name, channel_name), self.RESPONSE_ATTEMPTS)

    def _record_reply_outcome(
        self,
        *,
        endpoint_name: str,
        channel_name: str,
        success: bool,
        attempts_used: int,
        planned_attempts: int,
    ) -> None:
        key = (endpoint_name, channel_name)
        current = self._adaptive_response_attempts.get(key, self.RESPONSE_ATTEMPTS)
        if success:
            if attempts_used <= 1:
                next_attempts = max(self.RESPONSE_ATTEMPTS, current - 1)
            else:
                next_attempts = max(self.RESPONSE_ATTEMPTS, min(self.RESPONSE_ATTEMPTS_MAX, attempts_used + 1))
        else:
            next_attempts = min(self.RESPONSE_ATTEMPTS_MAX, max(current, planned_attempts) + 1)
        self._adaptive_response_attempts[key] = next_attempts

    def _retry_delay_for_attempt(self, attempt_number: int) -> float:
        if self.RESPONSE_RETRY_DELAY_SECS <= 0:
            return 0.0
        exponent = max(0, attempt_number - 1)
        delay = self.RESPONSE_RETRY_DELAY_SECS * (self.RESPONSE_RETRY_BACKOFF_MULTIPLIER ** exponent)
        if self.RESPONSE_RETRY_MAX_DELAY_SECS > 0:
            delay = min(delay, self.RESPONSE_RETRY_MAX_DELAY_SECS)
        return delay

    def _cancel_pending_replies_for_endpoint(self, endpoint_name: str) -> None:
        for reply_key, pending in list(self._pending_replies.items()):
            if reply_key[0] != endpoint_name:
                continue
            if not pending.task.done():
                pending.task.cancel()
            del self._pending_replies[reply_key]

    async def _send_channel_reply(
        self,
        endpoint: EndpointConfig,
        client: PacketTransportClient,
        channel_name: str,
        reply: str,
        wire_timestamp: int,
        reply_key: tuple[str, str, int, str],
        echo_event: asyncio.Event,
        planned_attempts: int,
    ) -> None:
        try:
            for attempt in range(planned_attempts):
                if attempt > 0:
                    await self._activate_quiet_window(client, channel_name)
                attempt_reply = self._format_reply_attempt(reply, attempt + 1, planned_attempts)
                envelope = build_group_text_packet(
                    sender_name=self.sender_name,
                    message=attempt_reply,
                    channel_secret=self._channel_secrets[channel_name],
                    timestamp=wire_timestamp,
                    attempt=attempt,
                )
                await self._send_envelope(
                    endpoint,
                    client,
                    envelope.packet,
                    envelope.summary.payload_type,
                    envelope.summary.route_type,
                    notes=f"channel reply on {channel_name}: {attempt_reply} attempt={attempt}",
                )
                pending = self._pending_replies.get(reply_key)
                if pending is not None:
                    pending.sent_attempts = attempt + 1
                self.logger.info(
                    "[BOT-TX] endpoint=%s channel=%s attempt=%s/%s text=%s",
                    endpoint.name,
                    channel_name,
                    attempt + 1,
                    planned_attempts,
                    attempt_reply,
                )
                if echo_event.is_set():
                    self._record_reply_outcome(
                        endpoint_name=endpoint.name,
                        channel_name=channel_name,
                        success=True,
                        attempts_used=attempt + 1,
                        planned_attempts=planned_attempts,
                    )
                    self.logger.info(
                        "[BOT-MONITOR] endpoint=%s channel=%s ts=%s status=echo-seen attempt=%s",
                        endpoint.name,
                        channel_name,
                        wire_timestamp,
                        attempt + 1,
                    )
                    return
                if attempt + 1 < planned_attempts:
                    try:
                        await asyncio.wait_for(echo_event.wait(), timeout=self.ECHO_ACK_TIMEOUT_SECS)
                    except asyncio.TimeoutError:
                        next_delay = self._retry_delay_for_attempt(attempt + 1)
                        self.logger.warning(
                            "[BOT-NO-ECHO] endpoint=%s channel=%s ts=%s attempt=%s/%s timeout=%.2fs next_delay=%.2fs",
                            endpoint.name,
                            channel_name,
                            wire_timestamp,
                            attempt + 1,
                            planned_attempts,
                            self.ECHO_ACK_TIMEOUT_SECS,
                            next_delay,
                        )
                        if next_delay > 0:
                            await asyncio.sleep(next_delay)
                    else:
                        self._record_reply_outcome(
                            endpoint_name=endpoint.name,
                            channel_name=channel_name,
                            success=True,
                            attempts_used=attempt + 1,
                            planned_attempts=planned_attempts,
                        )
                        self.logger.info(
                            "[BOT-MONITOR] endpoint=%s channel=%s ts=%s status=echo-seen attempt=%s",
                            endpoint.name,
                            channel_name,
                            wire_timestamp,
                            attempt + 1,
                        )
                        return
            self._record_reply_outcome(
                endpoint_name=endpoint.name,
                channel_name=channel_name,
                success=False,
                attempts_used=planned_attempts,
                planned_attempts=planned_attempts,
            )
            self.logger.warning(
                "[BOT-UNCONFIRMED] endpoint=%s channel=%s ts=%s attempts=%s",
                endpoint.name,
                channel_name,
                wire_timestamp,
                planned_attempts,
            )
        except asyncio.CancelledError:
            self.logger.info("[BOT-TX-CANCEL] endpoint=%s channel=%s ts=%s", endpoint.name, channel_name, wire_timestamp)
            raise
        finally:
            self._pending_replies.pop(reply_key, None)

    async def _send_envelope(
        self,
        endpoint: EndpointConfig,
        client: PacketTransportClient,
        packet: bytes,
        payload_type: PayloadType,
        route_type: RouteType,
        *,
        notes: str,
    ) -> None:
        lock = self._send_locks.setdefault(endpoint.name, asyncio.Lock())
        async with lock:
            await client.send_packet(packet)
        self.database.insert_raw_packet(
            endpoint_name=endpoint.name,
            observed_at=utc_now_iso(),
            direction="tx",
            transport="gateway",
            mesh_packet_hex=packet.hex().upper(),
            payload_type=int(payload_type),
            route_type=int(route_type),
            notes=notes,
        )