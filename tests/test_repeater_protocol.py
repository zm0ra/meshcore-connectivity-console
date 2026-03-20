import asyncio
import json
import struct
import sys

from datetime import UTC, datetime, timedelta
from dataclasses import replace
from typing import Any, cast
from unittest.mock import AsyncMock
from unittest.mock import patch

from meshcore_bot import __main__ as cli_main
from meshcore_bot.config import AppConfig, BotConfig, EndpointConfig, GatewayConfig, IdentityConfig, ProbeConfig, ServiceConfig, StorageConfig, WebConfig
from meshcore_bot.bot_service import ChannelCommandBotService
from meshcore_bot.bridge_gateway import BridgeGatewayService
from meshcore_bot.database import BotDatabase
from meshcore_bot.identity import LocalIdentity
from meshcore_bot.ingest_service import AdvertIngestService
from meshcore_bot.mesh_builders import (
    build_advert_packet,
    build_datagram_payload,
    build_group_text_packet,
    build_login_packet,
    build_mesh_packet,
    build_private_text_packet,
    next_request_tag,
    next_wire_timestamp,
    parse_anon_request,
    parse_encrypted_datagram,
    parse_group_text,
    parse_text_plaintext,
)
from meshcore_bot.mesh_packets import AdvertType, PayloadType, RouteType, parse_advert, parse_packet
from meshcore_bot.channels import channel_hash, derive_hashtag_secret, hashtag_psk_base64
from meshcore_bot.config import load_config, save_raw_config
from meshcore_bot.endpoint_console import normalize_console_reply, parse_console_neighbors_reply, parse_console_text_reply, run_console_command
from meshcore_bot.probe_service import LocalConsoleEndpointResolver, ProbeTimeoutError, GuestProbeWorker, is_recent_observation, is_within_hour_window, select_login_candidates, select_login_route_attempts
from meshcore_bot.repeater_protocol import (
    build_path_discovery_request,
    parse_login_response,
    parse_neighbours_response,
    parse_owner_info_response,
    parse_status_response,
)
from meshcore_bot.tcp_client import MeshcoreTCPClient
from meshcore_bot.tcp_client import ReceivedPacket


class FakeTCPClient:
    def __init__(self, received_packets: list[ReceivedPacket]) -> None:
        self.received_packets = list(received_packets)
        self.sent_packets: list[bytes] = []
        self.quiet_windows: list[float] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def send_packet(self, packet: bytes) -> str:
        self.sent_packets.append(packet)
        return packet.hex().upper()

    async def activate_quiet_window(self, *, seconds: float) -> None:
        self.quiet_windows.append(seconds)

    async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
        if not self.received_packets:
            raise asyncio.TimeoutError()
        return self.received_packets.pop(0)


def build_received_packet(*, advert_type: AdvertType = AdvertType.REPEATER, name: str = "test-rpt") -> ReceivedPacket:
    advert_packet = build_advert_packet(identity=LocalIdentity.generate(), name=name, advert_type=int(advert_type))
    return ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=advert_packet.packet.hex().upper(),
        packet_hex=advert_packet.packet.hex().upper(),
        summary=advert_packet.summary,
    )


def build_test_app_config(tmp_path) -> AppConfig:
    return AppConfig(
        service=ServiceConfig(name="meshcore-bot", log_level="INFO"),
        storage=StorageConfig(database_path=tmp_path / "meshcore-bot.db"),
        identity=IdentityConfig(key_file_path=tmp_path / "identity.bin"),
        probe=ProbeConfig(
            key_file_path=None,
            admin_password="qweqwe",
            admin_password_name_prefixes=("SZN_",),
            admin_password_pubkey_prefixes=(),
            guest_password="",
            default_guest_password="",
            guest_password_name_prefixes=(),
            guest_password_pubkey_prefixes=(),
            pre_login_advert_name="441CFEA26666",
            pre_login_advert_delay_secs=0.0,
            advert_reprobe_success_cooldown_secs=60.0,
            advert_reprobe_failure_cooldown_secs=300.0,
            advert_probe_min_interval_secs=10.0,
            advert_path_change_cooldown_secs=300.0,
            automatic_probe_max_per_day=3,
            scheduled_reprobe_interval_secs=28800.0,
            night_failed_retry_start_hour=1,
            night_failed_retry_end_hour=7,
            night_failed_retry_interval_secs=3600.0,
            poll_interval_secs=2.0,
            request_timeout_secs=1.0,
            route_freshness_secs=1800.0,
            neighbours_page_size=15,
            neighbours_prefix_len=4,
        ),
        bot=BotConfig(
            enabled=True,
            sender_name="",
            channels=("#bot-test",),
            enabled_commands=("!ping", "!test", "!help"),
            min_response_delay_secs=1.0,
            response_attempts=2,
            response_attempts_max=30,
            echo_ack_timeout_secs=0.0,
            response_retry_delay_secs=1.75,
            response_retry_backoff_multiplier=1.0,
            response_retry_max_delay_secs=10.0,
            quiet_window_secs=8.0,
            command_dedup_ttl_secs=30.0,
            include_test_signal=True,
        ),
        web=WebConfig(host="127.0.0.1", port=8080),
        gateway=GatewayConfig(
            control_socket_path=tmp_path / "gateway-control.sock",
            event_socket_path=tmp_path / "gateway-events.sock",
        ),
        endpoints=(EndpointConfig(name="test-endpoint", raw_host="127.0.0.1", raw_port=5002, enabled=True),),
    )


def build_multi_endpoint_test_app_config(tmp_path) -> AppConfig:
    base = build_test_app_config(tmp_path)
    return replace(
        base,
        endpoints=(
            EndpointConfig(name="RPT_Okolna", raw_host="127.0.0.1", raw_port=5002, enabled=True),
            EndpointConfig(name="RPT_Przesocin", raw_host="127.0.0.1", raw_port=5003, enabled=True),
            EndpointConfig(name="RPT_Zapas", raw_host="127.0.0.1", raw_port=5004, enabled=True),
        ),
    )


def build_local_console_test_app_config(tmp_path) -> AppConfig:
    base = build_test_app_config(tmp_path)
    return replace(
        base,
        endpoints=(
            EndpointConfig(
                name="RPT_Okolna",
                raw_host="127.0.0.1",
                raw_port=5002,
                enabled=True,
                local_node_name="SZN_STO_OMNI_RPT",
                console_mirror_host="127.0.0.2",
                console_mirror_port=5003,
            ),
            EndpointConfig(name="RPT_Przesocin", raw_host="127.0.0.1", raw_port=5003, enabled=True, console_mirror_host="127.0.0.3", console_mirror_port=5003),
        ),
    )


def test_endpoint_console_probe_target_prefers_console_mirror() -> None:
    endpoint = EndpointConfig(
        name="RPT_Przesocin",
        raw_host="172.30.252.58",
        raw_port=5002,
        enabled=True,
        console_port=5001,
        console_mirror_host="172.30.252.58",
        console_mirror_port=5003,
    )

    assert endpoint.console_probe_target() == ("172.30.252.58", 5003)


def test_endpoint_console_probe_target_falls_back_to_console_port() -> None:
    endpoint = EndpointConfig(
        name="RPT_Okolna",
        raw_host="172.30.105.24",
        raw_port=5002,
        enabled=True,
        console_port=5001,
    )

    assert endpoint.console_probe_target() == ("172.30.105.24", 5001)


def test_hashtag_channel_secret_is_deterministic() -> None:
    secret = derive_hashtag_secret("#test")
    assert secret.hex() == "9cd8fcf22a47333b591d96a2b848b73f"
    assert len(hashtag_psk_base64("#test")) > 0
    assert channel_hash(secret) == 0xD9


def test_parse_login_response() -> None:
    payload = struct.pack("<IBBBB4sB", 1234, 0, 0, 1, 3, b"ABCD", 2)
    parsed = parse_login_response(payload)
    assert parsed.server_time == 1234
    assert parsed.response_code == 0
    assert parsed.is_admin_legacy is True
    assert parsed.permissions == 3
    assert parsed.firmware_capability_level == 2


def test_parse_owner_info_response() -> None:
    payload = struct.pack("<I", 55) + b"v1.14.0\nrpt-main\nOwner text"
    parsed = parse_owner_info_response(payload)
    assert parsed.request_tag == 55
    assert parsed.firmware_version == "v1.14.0"
    assert parsed.node_name == "rpt-main"
    assert parsed.owner_info == "Owner text"


def test_parse_status_response() -> None:
    payload = struct.pack(
        "<IHHhhIIIIIIIIHhHHII",
        77,
        4200,
        3,
        -110,
        -67,
        10,
        20,
        30,
        40,
        50,
        60,
        70,
        80,
        0x12,
        14,
        5,
        6,
        90,
        7,
    )
    parsed = parse_status_response(payload)
    assert parsed.request_tag == 77
    assert parsed.batt_milli_volts == 4200
    assert parsed.last_snr == 3.5


def test_normalize_console_reply_strips_nested_prompt_markers() -> None:
    transcript = "MeshCore repeater console\r\n> get name\r\n  -> > SZN_STO_OMNI_RPT\r\n> "
    normalized = normalize_console_reply(transcript, "get name")
    assert normalized == "SZN_STO_OMNI_RPT"
    assert parse_console_text_reply(normalized) == "SZN_STO_OMNI_RPT"


def test_parse_console_neighbors_reply_parses_console_rows() -> None:
    transcript = (
        "> neighbors\n"
        "  -> 01C97DDB:238:12\n"
        "35D4F997:275:-10\n"
        "F238FEE0:713:-33\n"
        "DFA33F82:1846:21\n"
        "4E50AFA0:3151:35\n"
        "025656AD:3244:-25\n"
        "481BB67F:3572:29\n"
        "> "
    )
    normalized = normalize_console_reply(transcript, "neighbors")
    parsed = parse_console_neighbors_reply(normalized)
    assert [item["neighbor_hash_prefix"] for item in parsed] == [
        "01C97DDB",
        "35D4F997",
        "F238FEE0",
        "DFA33F82",
        "4E50AFA0",
        "025656AD",
        "481BB67F",
    ]
    assert parsed[0]["last_heard_seconds"] == 238
    assert parsed[0]["snr"] == 3.0
    assert parsed[1]["snr"] == -2.5


def test_parse_console_text_reply_ignores_placeholder_tokens() -> None:
    assert parse_console_text_reply("->") == ""
    assert parse_console_text_reply(">") == ""
    assert parse_console_text_reply("-none-") == ""


def test_run_console_command_waits_for_payload_after_prompt() -> None:
    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(b"MeshCore repeater console mirror\r\n")
        await writer.drain()
        await reader.readline()
        writer.write(b"> neighbors\r\n  -> ")
        await writer.drain()
        await asyncio.sleep(0.15)
        writer.write(b"01C97DDB:238:12\r\n35D4F997:275:-10\r\n> ")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def scenario() -> None:
        server = await asyncio.start_server(handle_client, host="127.0.0.1", port=0)
        port = server.sockets[0].getsockname()[1]
        try:
            reply = await run_console_command("127.0.0.1", port, "neighbors", timeout=1.0)
        finally:
            server.close()
            await server.wait_closed()
        assert reply == "01C97DDB:238:12\n35D4F997:275:-10"

    asyncio.run(scenario())


def test_parse_neighbours_response() -> None:
    payload = struct.pack("<IHH", 99, 2, 2)
    payload += bytes.fromhex("A1B2C3D4") + struct.pack("<Ib", 15, 8)
    payload += bytes.fromhex("01020304") + struct.pack("<Ib", 30, -12)
    parsed = parse_neighbours_response(payload, pubkey_prefix_len=4)
    assert parsed.request_tag == 99
    assert parsed.neighbours_count == 2
    assert parsed.results_count == 2
    assert parsed.entries[0].pubkey_prefix_hex == "A1B2C3D4"
    assert parsed.entries[0].snr == 2.0
    assert parsed.entries[1].snr == -3.0


def test_build_path_discovery_request_matches_companion_shape() -> None:
    payload = build_path_discovery_request(0x11223344, random_bytes=b"ABCD")
    assert payload == bytes.fromhex("4433221103FE00000041424344")


def test_select_login_candidates_prefers_szn_admin_password() -> None:
    config = ProbeConfig(
        key_file_path=None,
        admin_password="qweqwe",
        admin_password_name_prefixes=("SZN_",),
        admin_password_pubkey_prefixes=(),
        guest_password="",
        default_guest_password="",
        guest_password_name_prefixes=(),
        guest_password_pubkey_prefixes=(),
        pre_login_advert_name="441CFEA26666",
        pre_login_advert_delay_secs=1.0,
        advert_reprobe_success_cooldown_secs=60.0,
        advert_reprobe_failure_cooldown_secs=300.0,
        advert_probe_min_interval_secs=10.0,
        advert_path_change_cooldown_secs=300.0,
        automatic_probe_max_per_day=3,
        scheduled_reprobe_interval_secs=28800.0,
        night_failed_retry_start_hour=1,
        night_failed_retry_end_hour=7,
        night_failed_retry_interval_secs=3600.0,
        poll_interval_secs=2.0,
        request_timeout_secs=8.0,
        route_freshness_secs=1800.0,
        neighbours_page_size=15,
        neighbours_prefix_len=4,
    )
    selected = select_login_candidates(
        config=config,
        remote_pubkey=bytes.fromhex("35D4F9975A2B0E57A48B5BBCCC9F71144CCC7F06BDB8CDAD91054A7A72B0868C"),
        repeater_name="SZN_BKO_DIR_STRGD_RPT ",
    )
    assert selected == [("admin", "qweqwe"), ("guest", "")]


def test_select_login_candidates_fall_back_to_empty_guest_for_non_szn() -> None:
    config = ProbeConfig(
        key_file_path=None,
        admin_password="qweqwe",
        admin_password_name_prefixes=("SZN_",),
        admin_password_pubkey_prefixes=("35D4F9975A2B",),
        guest_password="",
        default_guest_password="",
        guest_password_name_prefixes=(),
        guest_password_pubkey_prefixes=(),
        pre_login_advert_name="441CFEA26666",
        pre_login_advert_delay_secs=1.0,
        advert_reprobe_success_cooldown_secs=60.0,
        advert_reprobe_failure_cooldown_secs=300.0,
        advert_probe_min_interval_secs=10.0,
        advert_path_change_cooldown_secs=300.0,
        automatic_probe_max_per_day=3,
        scheduled_reprobe_interval_secs=28800.0,
        night_failed_retry_start_hour=1,
        night_failed_retry_end_hour=7,
        night_failed_retry_interval_secs=3600.0,
        poll_interval_secs=2.0,
        request_timeout_secs=8.0,
        route_freshness_secs=1800.0,
        neighbours_page_size=15,
        neighbours_prefix_len=4,
    )
    selected = select_login_candidates(
        config=config,
        remote_pubkey=bytes.fromhex("21D3857C81C3A41BC5030ADF2F7A878CFF6C91910F6BCD499AD74B4A2186850F"),
        repeater_name="Police Dir. 348°",
    )
    assert selected == [("guest", "")]


def test_select_login_candidates_puts_learned_login_first() -> None:
    config = ProbeConfig(
        key_file_path=None,
        admin_password="qweqwe",
        admin_password_name_prefixes=("SZN_",),
        admin_password_pubkey_prefixes=(),
        guest_password="hello",
        default_guest_password="",
        guest_password_name_prefixes=("RPT_",),
        guest_password_pubkey_prefixes=(),
        pre_login_advert_name="441CFEA26666",
        pre_login_advert_delay_secs=1.0,
        advert_reprobe_success_cooldown_secs=60.0,
        advert_reprobe_failure_cooldown_secs=300.0,
        advert_probe_min_interval_secs=10.0,
        advert_path_change_cooldown_secs=300.0,
        automatic_probe_max_per_day=3,
        scheduled_reprobe_interval_secs=28800.0,
        night_failed_retry_start_hour=1,
        night_failed_retry_end_hour=7,
        night_failed_retry_interval_secs=3600.0,
        poll_interval_secs=2.0,
        request_timeout_secs=8.0,
        route_freshness_secs=1800.0,
        neighbours_page_size=15,
        neighbours_prefix_len=4,
    )
    selected = select_login_candidates(
        config=config,
        remote_pubkey=bytes.fromhex("21D3857C81C3A41BC5030ADF2F7A878CFF6C91910F6BCD499AD74B4A2186850F"),
        repeater_name="RPT_Test",
        preferred_login=("guest", "hello"),
    )
    assert selected == [("guest", "hello"), ("guest", "")]


def test_build_advert_packet_roundtrip() -> None:
    identity = LocalIdentity.generate()
    packet = build_advert_packet(identity=identity, name="441CFEA26666", advert_type=int(AdvertType.CHAT))
    assert packet.summary.payload_type is PayloadType.ADVERT
    assert packet.summary.route_type is RouteType.DIRECT
    advert = parse_advert(packet.summary)
    assert advert.public_key == identity.public_key
    assert advert.advert_type is AdvertType.CHAT
    assert advert.name == "441CFEA26666"


def test_build_advert_packet_supports_flood_and_monotonic_timestamp() -> None:
    identity = LocalIdentity.generate()
    first = build_advert_packet(
        identity=identity,
        name="meshcore-bot",
        advert_type=int(AdvertType.CHAT),
        route_type=RouteType.FLOOD,
        timestamp=123456,
    )
    second = build_advert_packet(
        identity=identity,
        name="meshcore-bot",
        advert_type=int(AdvertType.CHAT),
        route_type=RouteType.FLOOD,
        timestamp=123456,
    )
    assert first.summary.route_type is RouteType.FLOOD
    first_advert = parse_advert(first.summary)
    second_advert = parse_advert(second.summary)
    assert first_advert.timestamp == 123456
    assert second_advert.timestamp == 123456


def test_next_wire_timestamp_is_monotonic() -> None:
    first = next_wire_timestamp(100)
    second = next_wire_timestamp(99)
    third = next_wire_timestamp(101)
    assert first == 100
    assert second == 99
    assert third == 101


def test_group_text_packet_roundtrip() -> None:
    secret = derive_hashtag_secret("#bot-test")
    packet = build_group_text_packet(sender_name="alice", message="!ping", channel_secret=secret, attempt=2)
    parsed = parse_group_text(packet.summary, channel_secret=secret)
    assert parsed is not None
    assert parsed.text == "alice: !ping"
    assert parsed.attempt == 2


def test_bot_service_replies_to_ping(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    secret = derive_hashtag_secret("#bot-test")
    incoming = build_group_text_packet(sender_name="alice", message="!ping", channel_secret=secret)
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 0.0
    service.RESPONSE_RETRY_DELAY_SECS = 0.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.01)

    asyncio.run(exercise())

    assert len(fake_client.sent_packets) == service.RESPONSE_ATTEMPTS
    assert fake_client.quiet_windows == [service.QUIET_WINDOW_SECS] * service.RESPONSE_ATTEMPTS
    decoded_packets = [parse_group_text(parse_packet(packet), channel_secret=secret) for packet in fake_client.sent_packets]
    assert all(decoded is not None for decoded in decoded_packets)
    attempts = [cast(Any, decoded).attempt for decoded in decoded_packets]
    assert attempts == [0, 1]
    texts = [cast(Any, decoded).text for decoded in decoded_packets]
    assert all(text.startswith("meshcore-bot: pong @[alice] ") for text in texts)
    assert texts[0].endswith("UTC tx 1/2")
    assert texts[1].endswith("UTC tx 2/2")


def test_bot_service_replies_to_help(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    secret = derive_hashtag_secret("#bot-test")
    incoming = build_group_text_packet(sender_name="alice", message="!help", channel_secret=secret)
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 0.0
    service.RESPONSE_RETRY_DELAY_SECS = 0.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.01)

    asyncio.run(exercise())

    assert len(fake_client.sent_packets) == service.RESPONSE_ATTEMPTS
    decoded_packets = [parse_group_text(parse_packet(packet), channel_secret=secret) for packet in fake_client.sent_packets]
    assert all(decoded is not None for decoded in decoded_packets)
    texts = [cast(Any, decoded).text for decoded in decoded_packets]
    assert texts == [
        "meshcore-bot: !ping !test !help tx 1/2",
        "meshcore-bot: !ping !test !help tx 2/2",
    ]


def test_bot_service_ignores_unconfigured_channel(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    incoming = build_group_text_packet(sender_name="alice", message="!ping", channel_secret=derive_hashtag_secret("#other"))
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 0.0
    service.RESPONSE_RETRY_DELAY_SECS = 0.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.01)

    asyncio.run(exercise())

    assert fake_client.sent_packets == []


def test_bot_service_replies_on_second_configured_channel(tmp_path) -> None:
    config = replace(
        build_test_app_config(tmp_path),
        bot=replace(build_test_app_config(tmp_path).bot, channels=("#bot-test", "#mesh-alerts")),
    )
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    secret = derive_hashtag_secret("#mesh-alerts")
    incoming = build_group_text_packet(sender_name="alice", message="!ping", channel_secret=secret)
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 0.0
    service.RESPONSE_RETRY_DELAY_SECS = 0.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.01)

    asyncio.run(exercise())

    assert len(fake_client.sent_packets) == service.RESPONSE_ATTEMPTS
    decoded_packets = [parse_group_text(parse_packet(packet), channel_secret=secret) for packet in fake_client.sent_packets]
    assert all(decoded is not None for decoded in decoded_packets)


def test_bot_service_replies_to_test_with_signal_when_known(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name=config.endpoints[0].name,
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=repeater_identity.public_key,
        advert_name="alice",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=123,
        path_len=0,
        path_hex="",
        raw_packet_hex="AA",
    )
    probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name=config.endpoints[0].name)
    database.complete_probe_run(
        probe_run_id,
        repeater_id=repeater_id,
        result="success",
        guest_login_ok=True,
        guest_permissions=3,
        firmware_capability_level=2,
        login_server_time=123,
        error_message=None,
    )
    database.save_status_snapshot(
        probe_run_id=probe_run_id,
        status={
            "batt_milli_volts": 4100,
            "curr_tx_queue_len": 1,
            "noise_floor": -110,
            "last_rssi": -87,
            "n_packets_recv": 1,
            "n_packets_sent": 2,
            "total_air_time_secs": 3,
            "total_up_time_secs": 4,
            "n_sent_flood": 5,
            "n_sent_direct": 6,
            "n_recv_flood": 7,
            "n_recv_direct": 8,
            "err_events": 0,
            "last_snr": 4.5,
            "n_direct_dups": 0,
            "n_flood_dups": 0,
            "total_rx_air_time_secs": 9,
            "n_recv_errors": 0,
        },
    )
    secret = derive_hashtag_secret("#bot-test")
    incoming = build_group_text_packet(sender_name="alice", message="!test", channel_secret=secret)
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 0.0
    service.RESPONSE_RETRY_DELAY_SECS = 0.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.01)

    asyncio.run(exercise())

    assert len(fake_client.sent_packets) == service.RESPONSE_ATTEMPTS
    decoded_packets = [parse_group_text(parse_packet(packet), channel_secret=secret) for packet in fake_client.sent_packets]
    assert all(decoded is not None for decoded in decoded_packets)
    attempts = [cast(Any, decoded).attempt for decoded in decoded_packets]
    assert attempts == [0, 1]
    texts = [cast(Any, decoded).text for decoded in decoded_packets]
    assert all(text.startswith("meshcore-bot: @[alice] hops: 0 ") for text in texts)
    assert all("SNR: 4.5" in text for text in texts)
    assert all("RSSI: -87" in text for text in texts)
    assert texts[0].endswith("RSSI: -87 tx 1/2")
    assert texts[1].endswith("RSSI: -87 tx 2/2")


def test_bot_service_ignores_duplicate_flood_copies(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    secret = derive_hashtag_secret("#bot-test")
    incoming = build_group_text_packet(sender_name="alice", message="!ping", channel_secret=secret, timestamp=123456)
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 0.0
    service.RESPONSE_RETRY_DELAY_SECS = 0.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.01)

    asyncio.run(exercise())

    assert len(fake_client.sent_packets) == service.RESPONSE_ATTEMPTS


def test_bot_service_cancels_retry_after_seeing_own_echo(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    secret = derive_hashtag_secret("#bot-test")
    incoming = build_group_text_packet(sender_name="alice", message="!ping", channel_secret=secret, timestamp=123456)
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 1.0
    service.RESPONSE_RETRY_DELAY_SECS = 1.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.01)
        sent_summary = parse_packet(fake_client.sent_packets[0])
        echoed = ReceivedPacket(
            observed_at=datetime.now(tz=UTC).isoformat(),
            frame_hex=fake_client.sent_packets[0].hex().upper(),
            packet_hex=fake_client.sent_packets[0].hex().upper(),
            summary=sent_summary,
        )
        await service._handle_packet(config.endpoints[0], fake_client, echoed)
        await asyncio.sleep(0.05)

    asyncio.run(exercise())

    assert len(fake_client.sent_packets) == 1


def test_bot_service_retries_only_after_missing_echo(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    secret = derive_hashtag_secret("#bot-test")
    incoming = build_group_text_packet(sender_name="alice", message="!ping", channel_secret=secret, timestamp=123456)
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=incoming.packet.hex().upper(),
        packet_hex=incoming.packet.hex().upper(),
        summary=incoming.summary,
    )
    fake_client = FakeTCPClient([])
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: fake_client)
    service.MIN_RESPONSE_DELAY_SECS = 0.0
    service.ECHO_ACK_TIMEOUT_SECS = 0.01
    service.RESPONSE_RETRY_DELAY_SECS = 0.0

    async def exercise() -> None:
        await service._handle_packet(config.endpoints[0], fake_client, received)
        await asyncio.sleep(0.05)

    asyncio.run(exercise())

    assert len(fake_client.sent_packets) == service.RESPONSE_ATTEMPTS
    assert fake_client.quiet_windows == [service.QUIET_WINDOW_SECS, service.QUIET_WINDOW_SECS]


def test_bot_service_retry_delay_uses_backoff_multiplier(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: FakeTCPClient([]))
    service.RESPONSE_RETRY_DELAY_SECS = 1.5
    service.RESPONSE_RETRY_BACKOFF_MULTIPLIER = 1.7
    service.RESPONSE_RETRY_MAX_DELAY_SECS = 10.0

    assert round(service._retry_delay_for_attempt(1), 2) == 1.50
    assert round(service._retry_delay_for_attempt(2), 2) == 2.55
    assert round(service._retry_delay_for_attempt(3), 2) == 4.33


def test_bot_service_retry_delay_respects_max_cap(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: FakeTCPClient([]))
    service.RESPONSE_RETRY_DELAY_SECS = 2.0
    service.RESPONSE_RETRY_BACKOFF_MULTIPLIER = 2.0
    service.RESPONSE_RETRY_MAX_DELAY_SECS = 10.0

    assert round(service._retry_delay_for_attempt(1), 2) == 2.00
    assert round(service._retry_delay_for_attempt(2), 2) == 4.00
    assert round(service._retry_delay_for_attempt(3), 2) == 8.00
    assert round(service._retry_delay_for_attempt(4), 2) == 10.00


def test_bot_service_increases_future_attempt_budget_after_unconfirmed_reply(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: FakeTCPClient([]))
    service.RESPONSE_ATTEMPTS = 2
    service.RESPONSE_ATTEMPTS_MAX = 4

    service._record_reply_outcome(
        endpoint_name="test-endpoint",
        channel_name="#bot-test",
        success=False,
        attempts_used=2,
        planned_attempts=2,
    )

    assert service._planned_response_attempts("test-endpoint", "#bot-test") == 3

    service._record_reply_outcome(
        endpoint_name="test-endpoint",
        channel_name="#bot-test",
        success=False,
        attempts_used=3,
        planned_attempts=3,
    )

    assert service._planned_response_attempts("test-endpoint", "#bot-test") == 4

    service._record_reply_outcome(
        endpoint_name="test-endpoint",
        channel_name="#bot-test",
        success=False,
        attempts_used=4,
        planned_attempts=4,
    )

    assert service._planned_response_attempts("test-endpoint", "#bot-test") == 4


def test_bot_service_reduces_future_attempt_budget_after_echo_success(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = ChannelCommandBotService(config, database, transport_factory=lambda endpoint: FakeTCPClient([]))
    service.RESPONSE_ATTEMPTS = 2
    service.RESPONSE_ATTEMPTS_MAX = 30
    service._adaptive_response_attempts[("test-endpoint", "#bot-test")] = 6

    service._record_reply_outcome(
        endpoint_name="test-endpoint",
        channel_name="#bot-test",
        success=True,
        attempts_used=1,
        planned_attempts=6,
    )

    assert service._planned_response_attempts("test-endpoint", "#bot-test") == 5

    service._record_reply_outcome(
        endpoint_name="test-endpoint",
        channel_name="#bot-test",
        success=True,
        attempts_used=2,
        planned_attempts=5,
    )

    assert service._planned_response_attempts("test-endpoint", "#bot-test") == 3


def test_private_text_packet_roundtrip() -> None:
    local_identity = LocalIdentity.generate()
    remote_identity = LocalIdentity.generate()
    packet = build_private_text_packet(
        identity=local_identity,
        remote_public_key=remote_identity.public_key,
        message="!ping",
        attempt=1,
    )
    decrypted = parse_encrypted_datagram(
        packet.summary,
        shared_secret=remote_identity.calc_shared_secret(local_identity.public_key),
    )
    parsed = parse_text_plaintext(decrypted.plaintext)
    assert parsed is not None
    timestamp, text_type, attempt, text = parsed
    assert timestamp > 1_600_000_000
    assert text_type == 0
    assert attempt == 1
    assert text == "!ping"


def test_build_login_packet_uses_time_like_timestamp() -> None:
    local_identity = LocalIdentity.generate()
    remote_identity = LocalIdentity.generate()
    packet = build_login_packet(
        identity=local_identity,
        remote_public_key=remote_identity.public_key,
        guest_password="qweqwe",
    )
    shared_secret = local_identity.calc_shared_secret(remote_identity.public_key)
    _, sender_public_key, plaintext = parse_anon_request(packet.summary, shared_secret=shared_secret)
    timestamp = struct.unpack_from("<I", plaintext, 0)[0]
    assert sender_public_key == local_identity.public_key
    assert timestamp > 1_600_000_000
    assert plaintext[4:].startswith(b"qweqwe")


def test_next_request_tag_uses_monotonic_time_like_values() -> None:
    baseline = next_wire_timestamp(1_773_473_000)
    tag = next_request_tag()
    later = next_request_tag()
    assert baseline == 1_773_473_000
    assert tag > baseline
    assert later > tag


def test_is_recent_observation_accepts_fresh_timestamp() -> None:
    now = datetime(2026, 3, 14, 8, 30, tzinfo=UTC)
    observed_at = (now - timedelta(minutes=5)).isoformat()
    assert is_recent_observation(observed_at, 1800.0, now=now)


def test_is_recent_observation_rejects_stale_timestamp() -> None:
    now = datetime(2026, 3, 14, 8, 30, tzinfo=UTC)
    observed_at = (now - timedelta(minutes=45)).isoformat()
    assert not is_recent_observation(observed_at, 1800.0, now=now)


def test_select_login_route_attempts_prefers_known_routes_before_flood() -> None:
    attempts = select_login_route_attempts(known_paths=[(2, bytes.fromhex("3548"))], local_zero_hop_visible=True)
    assert attempts == [(2, bytes.fromhex("3548")), (0, b"")]


def test_select_login_route_attempts_uses_known_direct_paths_in_order() -> None:
    attempts = select_login_route_attempts(
        known_paths=[(2, bytes.fromhex("35EF")), (2, bytes.fromhex("354E"))],
        local_zero_hop_visible=False,
    )
    assert attempts == [(2, bytes.fromhex("35EF")), (2, bytes.fromhex("354E")), (0, b"")]


def test_select_login_route_attempts_deduplicates_known_paths_before_flood() -> None:
    attempts = select_login_route_attempts(
        known_paths=[(2, bytes.fromhex("35EF")), (2, bytes.fromhex("35EF")), (1, bytes.fromhex("35"))],
        local_zero_hop_visible=False,
    )
    assert attempts == [(2, bytes.fromhex("35EF")), (1, bytes.fromhex("35")), (0, b"")]


def test_select_login_route_attempts_returns_empty_without_route_or_local_visibility() -> None:
    assert select_login_route_attempts(known_paths=[], local_zero_hop_visible=False) == []


def test_select_login_route_attempts_uses_flood_when_only_local_visibility_exists() -> None:
    assert select_login_route_attempts(known_paths=[], local_zero_hop_visible=True) == [(0, b"")]


def test_load_config_accepts_repo_relative_path_outside_repo_root(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    config = load_config("config/config.toml")
    assert config.gateway.control_socket_path.name == "control.sock"
    assert config.endpoints
    assert config.endpoints[0].name


def test_discover_repeater_path_uses_flood_and_saves_learned_route(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()

    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="ZST Grzedzice2 Dir.295",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=0,
        path_hex="",
        raw_packet_hex="00",
    )
    probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name="test-endpoint")
    worker = GuestProbeWorker(config, database)
    shared_secret = worker.identity.calc_shared_secret(remote_identity.public_key)
    discovery_tag = 0x10203040
    learned_path_len = 2
    learned_path_bytes = bytes.fromhex("354E")

    discovery_response_plaintext = (
        bytes([learned_path_len])
        + learned_path_bytes
        + bytes([int(PayloadType.RESPONSE)])
        + struct.pack("<I", discovery_tag)
        + b"DISC"
    )
    discovery_response = build_mesh_packet(
        route_type=RouteType.DIRECT,
        payload_type=PayloadType.PATH,
        payload=build_datagram_payload(
            destination_public_key=worker.identity.public_key,
            source_identity=remote_identity,
            shared_secret=shared_secret,
            plaintext=discovery_response_plaintext,
        ),
    )
    fake_client = FakeTCPClient(
        [
            ReceivedPacket(
                observed_at=datetime.now(tz=UTC).isoformat(),
                frame_hex=discovery_response.packet.hex().upper(),
                packet_hex=discovery_response.packet.hex().upper(),
                summary=discovery_response.summary,
            )
        ]
    )

    with patch("meshcore_bot.probe_service.next_request_tag", return_value=discovery_tag), patch(
        "meshcore_bot.probe_service.os.urandom", return_value=b"ABCD"
    ):
        discovered = asyncio.run(
            worker._discover_repeater_path(
                client=cast(Any, fake_client),
                endpoint_name="test-endpoint",
                probe_run_id=probe_run_id,
                repeater_id=repeater_id,
                remote_pubkey=remote_identity.public_key,
                shared_secret=shared_secret,
            )
        )

    assert discovered == (learned_path_len, learned_path_bytes)
    assert len(fake_client.sent_packets) == 1

    sent_summary = parse_packet(fake_client.sent_packets[0])
    assert sent_summary.route_type is RouteType.FLOOD
    assert sent_summary.payload_type is PayloadType.REQ
    sent_plaintext = parse_encrypted_datagram(sent_summary, shared_secret=shared_secret).plaintext
    expected_plaintext = build_path_discovery_request(discovery_tag, random_bytes=b"ABCD")
    assert sent_plaintext[: len(expected_plaintext)] == expected_plaintext
    assert sent_plaintext[len(expected_plaintext) :] == b"\x00\x00\x00"

    latest_path = database.latest_repeater_path(repeater_id=repeater_id)
    assert latest_path is not None
    assert latest_path["out_path_len"] == learned_path_len
    assert latest_path["out_path_hex"] == learned_path_bytes.hex().upper()
    assert latest_path["source"] == "path_discovery"


def test_send_with_tagged_response_retries_after_timeout(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    worker = GuestProbeWorker(config, database)
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="retry-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    packet = build_mesh_packet(route_type=RouteType.DIRECT, payload_type=PayloadType.REQ, payload=b"x")
    worker._send_and_record = AsyncMock(return_value=None)
    worker._await_tagged_response = AsyncMock(
        side_effect=[
            ProbeTimeoutError("first timeout"),
            (b"ok", 1, bytes.fromhex("35")),
        ]
    )

    result = asyncio.run(
        worker._send_with_tagged_response_retries(
            client=cast(Any, FakeTCPClient([])),
            endpoint_name="test-endpoint",
            probe_run_id=1,
            repeater_id=repeater_id,
            remote_pubkey=remote_identity.public_key,
            shared_secret=worker.identity.calc_shared_secret(remote_identity.public_key),
            packet=packet,
            expected_tag=123,
            notes="get_neighbours offset=0",
            current_path_len=1,
            current_path_bytes=bytes.fromhex("35"),
            max_attempts=2,
        )
    )

    assert result == (b"ok", 1, bytes.fromhex("35"))
    assert worker._send_and_record.await_count == 2
    assert worker._await_tagged_response.await_count == 2


def test_bridge_gateway_ignores_idle_receive_timeout_without_reconnect(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    received_packet = build_received_packet()

    class FakeGatewayTCPClient:
        instances: list[FakeGatewayTCPClient] = []

        def __init__(self, host: str, port: int) -> None:
            self.host = host
            self.port = port
            self.connect_calls = 0
            self.close_calls = 0
            self.receive_calls = 0
            FakeGatewayTCPClient.instances.append(self)

        async def connect(self) -> None:
            self.connect_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

        async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
            self.receive_calls += 1
            if self.receive_calls == 1:
                raise asyncio.TimeoutError()
            return received_packet

    service = BridgeGatewayService(config)
    runtime = service._endpoint_runtimes["test-endpoint"]

    async def stop_after_broadcast(endpoint_name: str, packet: ReceivedPacket) -> None:
        assert endpoint_name == "test-endpoint"
        assert packet is received_packet
        service._stop_event.set()

    with patch("meshcore_bot.bridge_gateway.MeshcoreTCPClient", FakeGatewayTCPClient):
        service._broadcast_packet = AsyncMock(side_effect=stop_after_broadcast)
        asyncio.run(service._run_endpoint(runtime))

    assert len(FakeGatewayTCPClient.instances) == 1
    client = FakeGatewayTCPClient.instances[0]
    assert client.connect_calls == 1
    assert client.receive_calls == 2
    assert client.close_calls == 1
    assert service._broadcast_packet.await_count == 1


def test_bridge_gateway_reconnects_after_watchdog_idle_period(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    config = replace(config, gateway=replace(config.gateway, traffic_watchdog_secs=0.02))
    received_packet = build_received_packet()

    class FakeGatewayTCPClient:
        instances: list[FakeGatewayTCPClient] = []

        def __init__(self, host: str, port: int) -> None:
            self.host = host
            self.port = port
            self.connect_calls = 0
            self.close_calls = 0
            self.receive_calls = 0
            self.connected_at = 0.0
            FakeGatewayTCPClient.instances.append(self)

        async def connect(self) -> None:
            self.connect_calls += 1
            self.connected_at = asyncio.get_running_loop().time()

        async def close(self) -> None:
            self.close_calls += 1

        def seconds_since_last_activity(self) -> float:
            return asyncio.get_running_loop().time() - self.connected_at

        def seconds_since_last_rx(self) -> float:
            return asyncio.get_running_loop().time() - self.connected_at

        async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
            self.receive_calls += 1
            if len(FakeGatewayTCPClient.instances) == 1:
                await asyncio.sleep(0.03)
                raise asyncio.TimeoutError()
            return received_packet

    service = BridgeGatewayService(config)
    runtime = service._endpoint_runtimes["test-endpoint"]

    async def stop_after_broadcast(endpoint_name: str, packet: ReceivedPacket) -> None:
        assert endpoint_name == "test-endpoint"
        assert packet is received_packet
        service._stop_event.set()

    with patch("meshcore_bot.bridge_gateway.MeshcoreTCPClient", FakeGatewayTCPClient):
        service._broadcast_packet = AsyncMock(side_effect=stop_after_broadcast)
        service._probe_console_mirror = AsyncMock(return_value="not-configured")
        asyncio.run(service._run_endpoint(runtime))

    assert len(FakeGatewayTCPClient.instances) == 2
    assert FakeGatewayTCPClient.instances[0].connect_calls == 1
    assert FakeGatewayTCPClient.instances[0].close_calls == 1
    assert FakeGatewayTCPClient.instances[1].connect_calls == 1
    assert FakeGatewayTCPClient.instances[1].close_calls == 1
    assert service._probe_console_mirror.await_count == 1
    assert service._broadcast_packet.await_count == 1


def test_bridge_gateway_reconnects_after_connection_error(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    received_packet = build_received_packet()

    class FakeGatewayTCPClient:
        instances: list[FakeGatewayTCPClient] = []

        def __init__(self, host: str, port: int) -> None:
            self.host = host
            self.port = port
            self.connect_calls = 0
            self.close_calls = 0
            self.receive_calls = 0
            FakeGatewayTCPClient.instances.append(self)

        async def connect(self) -> None:
            self.connect_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

        async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
            self.receive_calls += 1
            if len(FakeGatewayTCPClient.instances) == 1:
                raise ConnectionError("connection closed by peer")
            return received_packet

    service = BridgeGatewayService(config)
    runtime = service._endpoint_runtimes["test-endpoint"]

    async def stop_after_broadcast(endpoint_name: str, packet: ReceivedPacket) -> None:
        assert endpoint_name == "test-endpoint"
        assert packet is received_packet
        service._stop_event.set()

    with patch("meshcore_bot.bridge_gateway.MeshcoreTCPClient", FakeGatewayTCPClient):
        service._broadcast_packet = AsyncMock(side_effect=stop_after_broadcast)
        asyncio.run(service._run_endpoint(runtime))

    assert len(FakeGatewayTCPClient.instances) == 2
    assert FakeGatewayTCPClient.instances[0].connect_calls == 1
    assert FakeGatewayTCPClient.instances[0].close_calls == 1
    assert FakeGatewayTCPClient.instances[1].connect_calls == 1
    assert FakeGatewayTCPClient.instances[1].close_calls == 1
    assert service._broadcast_packet.await_count == 1


def test_bridge_gateway_prioritizes_bot_traffic_over_probe_backlog(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    service = BridgeGatewayService(config)
    runtime = service._endpoint_runtimes["test-endpoint"]

    class FakeGatewayTCPClient:
        def __init__(self) -> None:
            self.sent_packets: list[bytes] = []

        async def send_packet(self, packet: bytes) -> str:
            self.sent_packets.append(packet)
            await asyncio.sleep(0.01)
            return packet.hex().upper()

    async def scenario() -> None:
        assert runtime.connected_event is not None
        runtime.client = cast(Any, FakeGatewayTCPClient())
        runtime.connected_event.set()
        sender_task = asyncio.create_task(service._run_sender(runtime))
        try:
            probe_one = bytes.fromhex("01")
            probe_two = bytes.fromhex("02")
            bot_packet = bytes.fromhex("AA")

            probe_one_task = asyncio.create_task(
                service._handle_control_message(
                    ('{"command":"send_packet","endpoint_name":"test-endpoint","packet_hex":"01","traffic_class":"probe"}\n').encode("ascii")
                )
            )
            await asyncio.sleep(0)
            probe_two_task = asyncio.create_task(
                service._handle_control_message(
                    ('{"command":"send_packet","endpoint_name":"test-endpoint","packet_hex":"02","traffic_class":"probe"}\n').encode("ascii")
                )
            )
            await asyncio.sleep(0)
            bot_task = asyncio.create_task(
                service._handle_control_message(
                    ('{"command":"send_packet","endpoint_name":"test-endpoint","packet_hex":"AA","traffic_class":"bot"}\n').encode("ascii")
                )
            )

            probe_one_result, probe_two_result, bot_result = await asyncio.gather(probe_one_task, probe_two_task, bot_task)
            assert probe_one_result["ok"] is True
            assert probe_two_result["ok"] is True
            assert bot_result["ok"] is True
            assert cast(Any, runtime.client).sent_packets == [probe_one, bot_packet, probe_two]
        finally:
            service._stop_event.set()
            sender_task.cancel()
            await asyncio.gather(sender_task, return_exceptions=True)

    asyncio.run(scenario())


def test_meshcore_tcp_client_surfaces_remote_disconnect_without_hanging() -> None:
    async def scenario() -> None:
        async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle_client, host="127.0.0.1", port=0)
        port = server.sockets[0].getsockname()[1]
        client = MeshcoreTCPClient("127.0.0.1", port)
        try:
            await client.connect()
            try:
                await client.receive_packet(timeout=1.0)
            except ConnectionError as exc:
                assert "connection closed by peer" in str(exc)
            else:
                raise AssertionError("expected receive_packet to surface connection closure")

            try:
                await client.send_packet(b"\x00")
            except ConnectionError as exc:
                assert "connection closed by peer" in str(exc)
            else:
                raise AssertionError("expected send_packet to fail after reader disconnect")
        finally:
            await client.close()
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_meshcore_tcp_client_aborts_stuck_close() -> None:
    class FakeTransport:
        def __init__(self) -> None:
            self.abort_calls = 0

        def abort(self) -> None:
            self.abort_calls += 1

    class FakeWriter:
        def __init__(self) -> None:
            self.close_calls = 0
            self.transport = FakeTransport()

        def close(self) -> None:
            self.close_calls += 1

        async def wait_closed(self) -> None:
            await asyncio.sleep(60)

    async def scenario() -> None:
        client = MeshcoreTCPClient("127.0.0.1", 5002)
        writer = FakeWriter()
        client._writer = cast(Any, writer)
        await client.close(timeout=0.01)
        assert writer.close_calls == 1
        assert writer.transport.abort_calls == 1

    asyncio.run(scenario())


def test_ingest_ignores_idle_receive_timeout_without_reconnect(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    received_packet = build_received_packet()

    class FakeTransport:
        def __init__(self) -> None:
            self.connect_calls = 0
            self.close_calls = 0
            self.receive_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

        async def receive_packet(self, *, timeout: float) -> ReceivedPacket:
            self.receive_calls += 1
            if self.receive_calls == 1:
                raise asyncio.TimeoutError()
            return received_packet

    fake_transport = FakeTransport()
    service = AdvertIngestService(config, database, transport_factory=lambda endpoint: cast(Any, fake_transport))
    endpoint = config.endpoints[0]

    async def stop_after_packet(endpoint_config: EndpointConfig, packet: ReceivedPacket) -> None:
        assert endpoint_config.name == "test-endpoint"
        assert packet is received_packet
        service._stop_event.set()

    service._handle_packet = AsyncMock(side_effect=stop_after_packet)
    asyncio.run(service._run_endpoint(endpoint))

    assert fake_transport.connect_calls == 1
    assert fake_transport.receive_calls == 2
    assert fake_transport.close_calls == 1
    assert service._handle_packet.await_count == 1


def test_ingest_skips_probe_for_stable_advert_after_recent_completed_probe(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = AdvertIngestService(config, database)
    endpoint = config.endpoints[0]
    repeater_identity = LocalIdentity.generate()
    observed_at = datetime.now(tz=UTC).isoformat()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name=endpoint.name,
        observed_at=observed_at,
        public_key=repeater_identity.public_key,
        advert_name="stable-rpt",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name=endpoint.name)
    database.save_repeater_path(repeater_id=repeater_id, encoded_path_len=1, path_hex="35", source="test")
    database.complete_probe_run(
        probe_run_id,
        repeater_id=repeater_id,
        result="completed",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=1,
        login_server_time=1,
        error_message=None,
    )

    advert_packet = build_advert_packet(
        identity=repeater_identity,
        name="stable-rpt",
        advert_type=int(AdvertType.REPEATER),
    )
    advert_packet.summary.path_len = 1
    advert_packet.summary.path_bytes = bytes.fromhex("35")
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=advert_packet.packet.hex().upper(),
        packet_hex=advert_packet.packet.hex().upper(),
        summary=advert_packet.summary,
    )

    asyncio.run(service._handle_packet(endpoint, received))

    assert database.claim_probe_job() is None
    assert service.stats.advert_jobs_skipped_stable == 1


def test_ingest_enqueues_probe_for_meaningful_path_change_after_cooldown(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = AdvertIngestService(config, database)
    endpoint = config.endpoints[0]
    repeater_identity = LocalIdentity.generate()
    observed_at = datetime.now(tz=UTC).isoformat()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name=endpoint.name,
        observed_at=observed_at,
        public_key=repeater_identity.public_key,
        advert_name="path-rpt",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name=endpoint.name)
    database.save_repeater_path(repeater_id=repeater_id, encoded_path_len=1, path_hex="35", source="test")
    database.complete_probe_run(
        probe_run_id,
        repeater_id=repeater_id,
        result="completed",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=1,
        login_server_time=1,
        error_message=None,
    )
    with database.connect() as connection:
        connection.execute(
            "UPDATE repeaters SET last_probe_at = ? WHERE id = ?",
            ((datetime.now(tz=UTC) - timedelta(seconds=301)).isoformat(), repeater_id),
        )

    advert_packet = build_advert_packet(
        identity=repeater_identity,
        name="path-rpt",
        advert_type=int(AdvertType.REPEATER),
    )
    advert_packet.summary.path_len = 1
    advert_packet.summary.path_bytes = bytes.fromhex("99")
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=advert_packet.packet.hex().upper(),
        packet_hex=advert_packet.packet.hex().upper(),
        summary=advert_packet.summary,
    )

    asyncio.run(service._handle_packet(endpoint, received))

    claimed = database.claim_probe_job()
    assert claimed is not None
    assert claimed["repeater_id"] == repeater_id
    assert claimed["reason"] == "repeater advert observed"


def test_ingest_spaces_advert_probe_jobs_per_endpoint(tmp_path) -> None:
    base_config = build_test_app_config(tmp_path)
    config = replace(base_config, probe=replace(base_config.probe, advert_probe_min_interval_secs=30.0))
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = AdvertIngestService(config, database)
    endpoint = config.endpoints[0]

    first_identity = LocalIdentity.generate()
    second_identity = LocalIdentity.generate()
    first_packet = build_advert_packet(
        identity=first_identity,
        name="first-rpt",
        advert_type=int(AdvertType.REPEATER),
    )
    second_packet = build_advert_packet(
        identity=second_identity,
        name="second-rpt",
        advert_type=int(AdvertType.REPEATER),
    )
    first_received = ReceivedPacket(
        observed_at=datetime(2026, 3, 18, 8, 0, tzinfo=UTC).isoformat(),
        frame_hex=first_packet.packet.hex().upper(),
        packet_hex=first_packet.packet.hex().upper(),
        summary=first_packet.summary,
    )
    second_received = ReceivedPacket(
        observed_at=datetime(2026, 3, 18, 8, 0, 1, tzinfo=UTC).isoformat(),
        frame_hex=second_packet.packet.hex().upper(),
        packet_hex=second_packet.packet.hex().upper(),
        summary=second_packet.summary,
    )

    asyncio.run(service._handle_packet(endpoint, first_received))
    asyncio.run(service._handle_packet(endpoint, second_received))

    with database.connect() as connection:
        rows = connection.execute(
            "SELECT scheduled_at FROM probe_jobs ORDER BY scheduled_at ASC, id ASC"
        ).fetchall()
    assert len(rows) == 2
    first_scheduled = datetime.fromisoformat(str(rows[0]["scheduled_at"]))
    second_scheduled = datetime.fromisoformat(str(rows[1]["scheduled_at"]))
    assert (second_scheduled - first_scheduled).total_seconds() >= 30.0
    assert service.stats.advert_jobs_deferred == 1


def test_ingest_enqueues_local_console_probe_for_tcp_accessible_node(tmp_path) -> None:
    config = build_local_console_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    service = AdvertIngestService(config, database)

    advert_packet = build_advert_packet(
        identity=LocalIdentity.generate(),
        name="SZN_STO_OMNI_RPT",
        advert_type=int(AdvertType.REPEATER),
    )
    received = ReceivedPacket(
        observed_at=datetime.now(tz=UTC).isoformat(),
        frame_hex=advert_packet.packet.hex().upper(),
        packet_hex=advert_packet.packet.hex().upper(),
        summary=advert_packet.summary,
    )

    asyncio.run(service._handle_packet(config.endpoints[1], received))

    claimed = database.claim_probe_job()
    assert claimed is not None
    assert claimed["endpoint_name"] == "RPT_Okolna"
    assert claimed["reason"] == "repeater advert observed"


def test_enqueue_probe_job_skips_recent_completed_advert_reprobe_but_allows_manual_reason(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=LocalIdentity.generate().public_key,
        advert_name="cooldown-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    advert_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="repeater advert observed",
        success_cooldown_secs=60.0,
        failure_cooldown_secs=300.0,
    )
    assert advert_job_id is not None

    database.finish_probe_job(advert_job_id, status="completed")

    skipped_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="repeater advert observed",
        success_cooldown_secs=60.0,
        failure_cooldown_secs=300.0,
    )
    manual_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="manual live verification",
        success_cooldown_secs=60.0,
        failure_cooldown_secs=300.0,
    )

    assert skipped_job_id is None
    assert manual_job_id is not None


def test_claim_probe_job_skips_future_scheduled_rows(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=LocalIdentity.generate().public_key,
        advert_name="future-job-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    future_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="repeater advert observed",
        scheduled_at=(datetime.now(tz=UTC) + timedelta(minutes=10)).isoformat(),
    )
    assert future_job_id is not None
    assert database.claim_probe_job() is None


def test_enqueue_probe_job_uses_longer_failure_cooldown_for_recent_failed_advert(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=LocalIdentity.generate().public_key,
        advert_name="failure-cooldown-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    advert_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="repeater advert observed",
        success_cooldown_secs=60.0,
        failure_cooldown_secs=300.0,
    )
    assert advert_job_id is not None

    database.finish_probe_job(advert_job_id, status="failed", last_error="timeout")

    skipped_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="repeater advert observed",
        success_cooldown_secs=60.0,
        failure_cooldown_secs=300.0,
    )

    assert skipped_job_id is None


def test_database_remembers_and_resets_learned_login_only_after_stable_successes(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=LocalIdentity.generate().public_key,
        advert_name="login-memory-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    database.remember_repeater_login(repeater_id=repeater_id, login_role="guest", login_password="hello")
    learned = database.preferred_repeater_login(repeater_id=repeater_id)
    assert learned is not None
    assert learned["learned_login_password"] == "hello"
    assert learned["learned_login_success_count"] == 1
    assert database.reset_repeater_login_if_stable(repeater_id=repeater_id, min_success_count=3) is False

    database.remember_repeater_login(repeater_id=repeater_id, login_role="guest", login_password="hello")
    database.remember_repeater_login(repeater_id=repeater_id, login_role="guest", login_password="hello")
    learned = database.preferred_repeater_login(repeater_id=repeater_id)
    assert learned is not None
    assert learned["learned_login_success_count"] == 3
    assert database.reset_repeater_login_if_stable(repeater_id=repeater_id, min_success_count=3) is True
    assert database.preferred_repeater_login(repeater_id=repeater_id) is None


def test_enqueue_probe_job_respects_daily_cap_for_automatic_collection(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime(2026, 3, 18, 8, 0, tzinfo=UTC).isoformat(),
        public_key=LocalIdentity.generate().public_key,
        advert_name="daily-cap-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    for hour in (0, 8, 16):
        job_id = database.enqueue_probe_job(
            repeater_id=repeater_id,
            endpoint_name="test-endpoint",
            reason="scheduled stale refresh",
            scheduled_at=datetime(2026, 3, 18, hour, 0, tzinfo=UTC).isoformat(),
            max_recent_jobs=3,
        )
        assert job_id is not None
        database.finish_probe_job(job_id, status="completed")

    blocked_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="repeater advert observed",
        scheduled_at=datetime(2026, 3, 18, 20, 0, tzinfo=UTC).isoformat(),
        max_recent_jobs=3,
    )

    assert blocked_job_id is None


def test_delete_failed_probe_jobs_older_than_keeps_fresh_rows(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC).isoformat(),
        public_key=LocalIdentity.generate().public_key,
        advert_name="cleanup-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    old_failed_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="repeater advert observed",
    )
    assert old_failed_job_id is not None
    database.finish_probe_job(old_failed_job_id, status="failed", last_error="old timeout")

    fresh_failed_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="manual live verification",
    )
    assert fresh_failed_job_id is not None
    database.finish_probe_job(fresh_failed_job_id, status="failed", last_error="fresh timeout")

    with database.connect() as connection:
        connection.execute(
            "UPDATE probe_jobs SET finished_at = ?, started_at = ?, scheduled_at = ? WHERE id = ?",
            (
                datetime(2026, 3, 13, 10, 0, tzinfo=UTC).isoformat(),
                datetime(2026, 3, 13, 9, 59, tzinfo=UTC).isoformat(),
                datetime(2026, 3, 13, 9, 58, tzinfo=UTC).isoformat(),
                old_failed_job_id,
            ),
        )
        connection.execute(
            "UPDATE probe_jobs SET finished_at = ?, started_at = ?, scheduled_at = ? WHERE id = ?",
            (
                datetime(2026, 3, 14, 11, 45, tzinfo=UTC).isoformat(),
                datetime(2026, 3, 14, 11, 44, tzinfo=UTC).isoformat(),
                datetime(2026, 3, 14, 11, 43, tzinfo=UTC).isoformat(),
                fresh_failed_job_id,
            ),
        )

    deleted_count = database.delete_failed_probe_jobs_older_than(
        older_than_secs=12 * 3600,
        now=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )

    assert deleted_count == 1
    with database.connect() as connection:
        rows = connection.execute("SELECT id, status FROM probe_jobs ORDER BY id ASC").fetchall()
    assert [tuple(row) for row in rows] == [(fresh_failed_job_id, "failed")]


def test_recover_interrupted_probe_work_marks_running_jobs_interrupted_without_requeue(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=LocalIdentity.generate().public_key,
        advert_name="restart-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    running_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="scheduled stale refresh",
    )
    assert running_job_id is not None
    claimed = database.claim_probe_job()
    assert claimed is not None
    run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name="test-endpoint")

    recovered = database.recover_interrupted_probe_work()

    assert recovered == {"jobs_interrupted": 1, "runs_interrupted": 1}
    with database.connect() as connection:
        job_row = connection.execute(
            "SELECT status, started_at, finished_at, last_error FROM probe_jobs WHERE id = ?",
            (running_job_id,),
        ).fetchone()
        run_row = connection.execute(
            "SELECT result, finished_at, error_message FROM repeater_probe_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
    assert job_row["status"] == "interrupted"
    assert job_row["started_at"] is not None
    assert job_row["finished_at"] is not None
    assert job_row["last_error"] == "worker restart recovery"
    assert run_row["result"] == "interrupted"
    assert run_row["finished_at"] is not None
    assert run_row["error_message"] == "worker restart recovery"
    assert database.claim_probe_job() is None


def test_schedule_stale_repeater_probe_jobs_only_enqueues_recent_repeaters_with_stale_data(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    now = datetime(2026, 3, 14, 12, 0, tzinfo=UTC)

    stale_identity = LocalIdentity.generate()
    fresh_identity = LocalIdentity.generate()
    unseen_identity = LocalIdentity.generate()
    old_identity = LocalIdentity.generate()

    stale_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(minutes=30)).isoformat(),
        public_key=stale_identity.public_key,
        advert_name="stale-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    fresh_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(minutes=25)).isoformat(),
        public_key=fresh_identity.public_key,
        advert_name="fresh-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    unseen_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(minutes=20)).isoformat(),
        public_key=unseen_identity.public_key,
        advert_name="unseen-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    old_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(hours=8)).isoformat(),
        public_key=old_identity.public_key,
        advert_name="old-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    stale_run_id = database.create_probe_run(repeater_id=stale_repeater_id, endpoint_name="test-endpoint")
    database.save_neighbour_snapshot_page(
        probe_run_id=stale_run_id,
        page_offset=0,
        total_neighbours_count=1,
        results_count=1,
        entries=[{"neighbour_pubkey_prefix_hex": "A1B2C3D4", "heard_seconds_ago": 90, "snr": 4.0}],
    )
    database.complete_probe_run(
        stale_run_id,
        repeater_id=stale_repeater_id,
        result="success",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=None,
        login_server_time=None,
        error_message=None,
    )

    fresh_run_id = database.create_probe_run(repeater_id=fresh_repeater_id, endpoint_name="test-endpoint")
    database.save_neighbour_snapshot_page(
        probe_run_id=fresh_run_id,
        page_offset=0,
        total_neighbours_count=1,
        results_count=1,
        entries=[{"neighbour_pubkey_prefix_hex": "01020304", "heard_seconds_ago": 30, "snr": 8.0}],
    )
    database.complete_probe_run(
        fresh_run_id,
        repeater_id=fresh_repeater_id,
        result="success",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=None,
        login_server_time=None,
        error_message=None,
    )

    with database.connect() as connection:
        connection.execute(
            "UPDATE repeater_neighbour_snapshots SET observed_at = ? WHERE probe_run_id = ?",
            ((now - timedelta(hours=3)).isoformat(), stale_run_id),
        )
        connection.execute(
            "UPDATE repeater_neighbour_snapshots SET observed_at = ? WHERE probe_run_id = ?",
            ((now - timedelta(minutes=40)).isoformat(), fresh_run_id),
        )

    enqueued = database.schedule_stale_repeater_probe_jobs(
        endpoint_names=["test-endpoint"],
        stale_after_secs=7200.0,
        seen_within_secs=6 * 3600.0,
        reason="scheduled stale refresh",
        success_cooldown_secs=7200.0,
        failure_cooldown_secs=3600.0,
        now=now,
    )

    assert enqueued == 2
    with database.connect() as connection:
        rows = connection.execute(
            "SELECT repeater_id, reason, status FROM probe_jobs ORDER BY repeater_id ASC"
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        (stale_repeater_id, "scheduled stale refresh", "pending"),
        (unseen_repeater_id, "scheduled stale refresh", "pending"),
    ]
    assert old_repeater_id not in {row[0] for row in rows}


def test_schedule_stale_repeater_probe_jobs_respects_daily_cap(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    now = datetime(2026, 3, 18, 20, 0, tzinfo=UTC)

    identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(minutes=30)).isoformat(),
        public_key=identity.public_key,
        advert_name="daily-capped-stale-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    stale_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name="test-endpoint")
    database.save_neighbour_snapshot_page(
        probe_run_id=stale_run_id,
        page_offset=0,
        total_neighbours_count=1,
        results_count=1,
        entries=[{"neighbour_pubkey_prefix_hex": "A1B2C3D4", "heard_seconds_ago": 90, "snr": 4.0}],
    )
    database.complete_probe_run(
        stale_run_id,
        repeater_id=repeater_id,
        result="success",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=None,
        login_server_time=None,
        error_message=None,
    )
    with database.connect() as connection:
        connection.execute(
            "UPDATE repeater_neighbour_snapshots SET observed_at = ? WHERE probe_run_id = ?",
            ((now - timedelta(hours=10)).isoformat(), stale_run_id),
        )

    for hour in (0, 8, 16):
        job_id = database.enqueue_probe_job(
            repeater_id=repeater_id,
            endpoint_name="test-endpoint",
            reason="scheduled stale refresh",
            scheduled_at=datetime(2026, 3, 18, hour, 0, tzinfo=UTC).isoformat(),
            max_recent_jobs=3,
        )
        assert job_id is not None
        database.finish_probe_job(job_id, status="completed")

    enqueued = database.schedule_stale_repeater_probe_jobs(
        endpoint_names=["test-endpoint"],
        stale_after_secs=8 * 3600.0,
        seen_within_secs=24 * 3600.0,
        reason="scheduled stale refresh",
        success_cooldown_secs=8 * 3600.0,
        failure_cooldown_secs=4 * 3600.0,
        max_recent_jobs=3,
        now=now,
    )

    assert enqueued == 0


def test_schedule_recent_failed_repeater_probe_jobs_only_enqueues_recent_failed_repeaters_with_adverts(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)

    failed_identity = LocalIdentity.generate()
    success_identity = LocalIdentity.generate()
    old_failed_identity = LocalIdentity.generate()

    failed_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(minutes=20)).isoformat(),
        public_key=failed_identity.public_key,
        advert_name="failed-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    success_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(minutes=15)).isoformat(),
        public_key=success_identity.public_key,
        advert_name="success-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    old_failed_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=(now - timedelta(hours=9)).isoformat(),
        public_key=old_failed_identity.public_key,
        advert_name="old-failed-target",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    failed_run_id = database.create_probe_run(repeater_id=failed_repeater_id, endpoint_name="test-endpoint")
    database.complete_probe_run(
        failed_run_id,
        repeater_id=failed_repeater_id,
        result="failed",
        guest_login_ok=False,
        guest_permissions=None,
        firmware_capability_level=None,
        login_server_time=None,
        error_message="login timeout",
    )

    success_run_id = database.create_probe_run(repeater_id=success_repeater_id, endpoint_name="test-endpoint")
    database.complete_probe_run(
        success_run_id,
        repeater_id=success_repeater_id,
        result="success",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=None,
        login_server_time=None,
        error_message=None,
    )

    old_failed_run_id = database.create_probe_run(repeater_id=old_failed_repeater_id, endpoint_name="test-endpoint")
    database.complete_probe_run(
        old_failed_run_id,
        repeater_id=old_failed_repeater_id,
        result="failed",
        guest_login_ok=False,
        guest_permissions=None,
        firmware_capability_level=None,
        login_server_time=None,
        error_message="login timeout",
    )

    enqueued = database.schedule_recent_failed_repeater_probe_jobs(
        endpoint_names=["test-endpoint"],
        seen_within_secs=2 * 3600.0,
        reason=GuestProbeWorker.NIGHT_FAILED_RETRY_REASON,
        success_cooldown_secs=3600.0,
        failure_cooldown_secs=3600.0,
        now=now,
    )

    assert enqueued == 1
    with database.connect() as connection:
        rows = connection.execute(
            "SELECT repeater_id, reason, status FROM probe_jobs WHERE reason = ? ORDER BY repeater_id ASC",
            (GuestProbeWorker.NIGHT_FAILED_RETRY_REASON,),
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        (failed_repeater_id, GuestProbeWorker.NIGHT_FAILED_RETRY_REASON, "pending"),
    ]
    assert success_repeater_id not in {row[0] for row in rows}
    assert old_failed_repeater_id not in {row[0] for row in rows}


def test_is_within_hour_window_supports_night_range() -> None:
    assert is_within_hour_window(hour=1, start_hour=1, end_hour=7) is True
    assert is_within_hour_window(hour=6, start_hour=1, end_hour=7) is True
    assert is_within_hour_window(hour=7, start_hour=1, end_hour=7) is False
    assert is_within_hour_window(hour=23, start_hour=22, end_hour=5) is True
    assert is_within_hour_window(hour=3, start_hour=22, end_hour=5) is True
    assert is_within_hour_window(hour=12, start_hour=22, end_hour=5) is False


def test_web_history_queries_keep_latest_neighbor_snapshot_and_signal_history(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()

    source_identity = LocalIdentity.generate()
    target_identity = LocalIdentity.generate()
    source_repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime(2026, 3, 14, 10, 0, tzinfo=UTC).isoformat(),
        public_key=source_identity.public_key,
        advert_name="Source RPT",
        advert_lat=53.43,
        advert_lon=14.55,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    database.upsert_repeater_from_advert(
        endpoint_name="test-endpoint",
        observed_at=datetime(2026, 3, 14, 10, 1, tzinfo=UTC).isoformat(),
        public_key=target_identity.public_key,
        advert_name="Target RPT",
        advert_lat=53.45,
        advert_lon=14.57,
        advert_timestamp_remote=2,
        path_len=1,
        path_hex="4E",
        raw_packet_hex="00",
    )

    first_run_id = database.create_probe_run(repeater_id=source_repeater_id, endpoint_name="test-endpoint")
    database.save_neighbour_snapshot_page(
        probe_run_id=first_run_id,
        page_offset=0,
        total_neighbours_count=1,
        results_count=1,
        entries=[
            {
                "neighbour_pubkey_prefix_hex": target_identity.public_key.hex().upper()[:8],
                "heard_seconds_ago": 30,
                "snr": 4.0,
            }
        ],
    )
    database.complete_probe_run(
        first_run_id,
        repeater_id=source_repeater_id,
        result="success",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=None,
        login_server_time=None,
        error_message=None,
    )

    second_run_id = database.create_probe_run(repeater_id=source_repeater_id, endpoint_name="test-endpoint")
    database.save_neighbour_snapshot_page(
        probe_run_id=second_run_id,
        page_offset=0,
        total_neighbours_count=1,
        results_count=1,
        entries=[
            {
                "neighbour_pubkey_prefix_hex": target_identity.public_key.hex().upper()[:8],
                "heard_seconds_ago": 12,
                "snr": 9.5,
            }
        ],
    )
    database.complete_probe_run(
        second_run_id,
        repeater_id=source_repeater_id,
        result="failed",
        guest_login_ok=False,
        guest_permissions=None,
        firmware_capability_level=None,
        login_server_time=None,
        error_message="owner-info timeout",
    )

    nodes = database.list_repeaters_for_web()
    source_node = next(item for item in nodes if item["identity_hex"] == source_identity.public_key.hex().upper())
    target_node = next(item for item in nodes if item["identity_hex"] == target_identity.public_key.hex().upper())
    assert source_node["data_fetch_ok"] == 1
    assert source_node["last_probe_status"] == "failed"
    assert target_node["data_fetch_ok"] == 0

    links = database.latest_repeater_neighbor_links(limit_repeaters=16)
    link = next(item for item in links if item["source_identity_hex"] == source_identity.public_key.hex().upper())
    assert link["probe_run_id"] == second_run_id
    assert link["target_identity_hex"] == target_identity.public_key.hex().upper()
    assert link["target_name"] == "Target RPT"
    assert link["snr"] == 9.5
    assert link["last_heard_seconds"] == 12

    history = database.repeater_neighbor_signal_history(limit_samples_per_source=16)
    source_history = history[source_identity.public_key.hex().upper()]
    assert len(source_history) == 2
    assert source_history[0]["target_identity_hex"] == target_identity.public_key.hex().upper()
    assert source_history[0]["snr"] == 9.5
    assert source_history[1]["snr"] == 4.0


def test_repeater_admin_database_helpers_support_manual_lifecycle(tmp_path) -> None:
    config = build_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()

    identity = LocalIdentity.generate()
    repeater_id = database.create_manual_repeater(
        pubkey_hex=identity.public_key.hex().upper(),
        name="Manual RPT",
        endpoint_name="test-endpoint",
        latitude=53.12,
        longitude=14.55,
    )
    assert repeater_id > 0

    database.remember_repeater_login(repeater_id=repeater_id, login_role="guest", login_password="hello")
    probe_job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="test-endpoint",
        reason="manual test probe",
    )
    assert probe_job_id is not None

    probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name="test-endpoint")
    database.save_neighbour_snapshot_page(
        probe_run_id=probe_run_id,
        page_offset=0,
        total_neighbours_count=1,
        results_count=1,
        entries=[
            {
                "neighbour_pubkey_prefix_hex": "A1B2C3D4",
                "heard_seconds_ago": 11,
                "snr": 7.5,
            }
        ],
    )
    database.complete_probe_run(
        probe_run_id,
        repeater_id=repeater_id,
        result="success",
        guest_login_ok=True,
        guest_permissions=1,
        firmware_capability_level=2,
        login_server_time=123,
        error_message=None,
    )

    full_state = database.repeater_full_state(repeater_id=repeater_id)
    assert full_state is not None
    assert full_state["last_name_from_advert"] == "Manual RPT"
    assert full_state["learned_login_role"] == "guest"
    assert full_state["next_probe_reason"] == "manual test probe"

    recent_runs = database.repeater_recent_probe_runs(repeater_id=repeater_id, limit=4)
    assert recent_runs[0]["result"] == "success"

    neighbours = database.latest_repeater_neighbours(repeater_id=repeater_id, limit=4)
    assert neighbours[0]["neighbour_pubkey_prefix_hex"] == "A1B2C3D4"

    jobs = database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=4)
    assert jobs[0]["reason"] == "manual test probe"

    updated = database.update_repeater_metadata(repeater_id=repeater_id, name="Manual RPT 2", latitude=50.0, longitude=16.0)
    assert updated is not None
    assert updated["last_name_from_advert"] == "Manual RPT 2"

    assert database.delete_repeater(repeater_id=repeater_id) is True
    assert database.repeater_full_state(repeater_id=repeater_id) is None


def test_cli_repeater_commands_add_probe_and_show(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "test-endpoint"
raw_host = "127.0.0.1"
raw_port = 5002
enabled = true
""".strip()
    )

    pubkey_hex = LocalIdentity.generate().public_key.hex().upper()

    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-add", "--config", str(config_path), "--pubkey", pubkey_hex, "--name", "CLI RPT"])
    cli_main.main()
    add_payload = json.loads(capsys.readouterr().out)
    repeater_id = int(add_payload["repeater_id"])

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "meshcore-bot",
            "rpt-probe",
            "--config",
            str(config_path),
            str(repeater_id),
            "--reason",
            "cli scheduled probe",
            "--role",
            "guest",
            "--password",
            "hello",
        ],
    )
    cli_main.main()
    probe_payload = json.loads(capsys.readouterr().out)
    assert probe_payload["job_id"] is not None
    assert probe_payload["learned_login"]["learned_login_role"] == "guest"

    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-show", "--config", str(config_path), str(repeater_id)])
    cli_main.main()
    show_payload = json.loads(capsys.readouterr().out)
    assert show_payload["repeater"]["id"] == repeater_id
    assert show_payload["repeater"]["next_probe_reason"] == "cli scheduled probe"
    assert show_payload["probe_jobs"][0]["reason"] == "cli scheduled probe"


def test_cli_endpoint_show_lists_recent_repeaters_for_endpoint(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "main"
raw_host = "172.30.105.24"
raw_port = 5002
enabled = true

[[endpoints]]
name = "backup"
raw_host = "172.30.252.58"
raw_port = 5002
enabled = true
console_mirror_port = 5003
""".strip()
    )

    config = load_config(config_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()

    old_pubkey = LocalIdentity.generate().public_key
    new_pubkey = LocalIdentity.generate().public_key
    database.upsert_repeater_from_advert(
        endpoint_name="backup",
        observed_at=(datetime.now(tz=UTC) - timedelta(hours=30)).isoformat(),
        public_key=old_pubkey,
        advert_name="Old backup RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    database.upsert_repeater_from_advert(
        endpoint_name="backup",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=new_pubkey,
        advert_name="Fresh backup RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=2,
        path_len=2,
        path_hex="3548",
        raw_packet_hex="00",
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["meshcore-bot", "endpoint-show", "--config", str(config_path), "backup", "--seen-within-hours", "24"],
    )
    cli_main.main()
    payload = json.loads(capsys.readouterr().out)

    assert payload["endpoint"]["name"] == "backup"
    assert payload["endpoint"]["raw_host"] == "172.30.252.58"
    assert payload["endpoint"]["console_mirror_host"] == "172.30.252.58"
    assert payload["endpoint"]["console_mirror_port"] == 5003
    assert payload["count"] == 1
    assert payload["repeaters"][0]["name"] == "Fresh backup RPT"
    assert payload["repeaters"][0]["advert_path_hex"] == "3548"


def test_cli_endpoint_config_commands_manage_named_endpoints(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "RPT_Okolna"
raw_host = "172.30.105.24"
raw_port = 5002
console_mirror_port = 5003
enabled = true
""".strip()
    )

    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "endpoint-list", "--config", str(config_path)])
    cli_main.main()
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["endpoints"][0]["name"] == "RPT_Okolna"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "meshcore-bot",
            "endpoint-add",
            "--config",
            str(config_path),
            "--name",
            "RPT_Przesocin",
            "--raw-host",
            "172.30.252.58",
            "--raw-port",
            "5002",
            "--console-mirror-port",
            "5003",
        ],
    )
    cli_main.main()
    added_payload = json.loads(capsys.readouterr().out)
    assert added_payload["endpoint"]["name"] == "RPT_Przesocin"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "meshcore-bot",
            "endpoint-update",
            "--config",
            str(config_path),
            "RPT_Okolna",
            "--raw-host",
            "172.30.105.99",
            "--disabled",
        ],
    )
    cli_main.main()
    updated_payload = json.loads(capsys.readouterr().out)
    assert updated_payload["endpoint"]["name"] == "RPT_Okolna"
    assert updated_payload["endpoint"]["raw_host"] == "172.30.105.99"
    assert updated_payload["endpoint"]["enabled"] is False

    reloaded = load_config(config_path)
    assert [endpoint.name for endpoint in reloaded.endpoints] == ["RPT_Okolna", "RPT_Przesocin"]
    assert reloaded.endpoints[0].raw_host == "172.30.105.99"
    assert reloaded.endpoints[0].enabled is False

    monkeypatch.setattr(
        sys,
        "argv",
        ["meshcore-bot", "endpoint-delete", "--config", str(config_path), "RPT_Przesocin", "--yes"],
    )
    cli_main.main()
    deleted_payload = json.loads(capsys.readouterr().out)
    assert deleted_payload["deleted"]["name"] == "RPT_Przesocin"

    final_config = load_config(config_path)
    assert [endpoint.name for endpoint in final_config.endpoints] == ["RPT_Okolna"]


def test_cli_without_subcommand_prints_help(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "test-endpoint"
raw_host = "127.0.0.1"
raw_port = 5002
enabled = true
""".strip()
    )

    monkeypatch.setattr(sys, "argv", ["meshcore-bot"])
    cli_main.main()
    output = capsys.readouterr().out
    assert "usage:" in output
    assert "init-db" in output
    assert "rpt-probe-now" in output


def test_cli_repeater_probe_now_runs_direct_probe(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "test-endpoint"
raw_host = "127.0.0.1"
raw_port = 5002
enabled = true
""".strip()
    )

    pubkey_hex = LocalIdentity.generate().public_key.hex().upper()
    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-add", "--config", str(config_path), "--pubkey", pubkey_hex, "--name", "CLI RPT"])
    cli_main.main()
    add_payload = json.loads(capsys.readouterr().out)
    repeater_id = int(add_payload["repeater_id"])

    async def fake_probe_repeater_as_guest(self, *, probe_run_id: int, repeater_id: int, **kwargs) -> None:
        self.database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=1,
            firmware_capability_level=2,
            login_server_time=123,
            error_message=None,
        )

    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_as_guest", fake_probe_repeater_as_guest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "meshcore-bot",
            "rpt-probe-now",
            "--config",
            str(config_path),
            str(repeater_id),
            "--role",
            "guest",
            "--password",
            "hello",
        ],
    )
    cli_main.main()
    output = capsys.readouterr().out
    assert "Starting probe for RPT" in output
    assert "Login succeeded:" not in output or "Probe completed successfully" in output
    assert "Probe completed successfully" in output
    assert "Last probe status: success" in output


def test_cli_repeater_probe_now_passes_force_path_discovery(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "test-endpoint"
raw_host = "127.0.0.1"
raw_port = 5002
enabled = true
""".strip()
    )

    pubkey_hex = LocalIdentity.generate().public_key.hex().upper()
    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-add", "--config", str(config_path), "--pubkey", pubkey_hex, "--name", "CLI RPT"])
    cli_main.main()
    add_payload = json.loads(capsys.readouterr().out)
    repeater_id = int(add_payload["repeater_id"])

    observed_kwargs: dict[str, object] = {}

    async def fake_probe_repeater_as_guest(self, *, probe_run_id: int, repeater_id: int, **kwargs) -> None:
        observed_kwargs.update(kwargs)
        self.database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=1,
            firmware_capability_level=2,
            login_server_time=123,
            error_message=None,
        )

    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_as_guest", fake_probe_repeater_as_guest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "meshcore-bot",
            "rpt-probe-now",
            "--config",
            str(config_path),
            str(repeater_id),
            "--force-path-discovery",
        ],
    )
    cli_main.main()
    output = capsys.readouterr().out
    assert "Route mode: force fresh discovery" in output
    assert observed_kwargs["force_path_discovery"] is True


def test_cli_repeater_probe_now_prefers_endpoint_from_advert_path(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "alpha"
raw_host = "127.0.0.1"
raw_port = 5002
enabled = true

[[endpoints]]
name = "beta"
raw_host = "127.0.0.1"
raw_port = 5003
enabled = true
""".strip()
    )

    pubkey = LocalIdentity.generate().public_key
    pubkey_hex = pubkey.hex().upper()
    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-add", "--config", str(config_path), "--pubkey", pubkey_hex, "--name", "CLI RPT"])
    cli_main.main()
    add_payload = json.loads(capsys.readouterr().out)
    repeater_id = int(add_payload["repeater_id"])

    config = load_config(config_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    database.upsert_repeater_from_advert(
        endpoint_name="beta",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=pubkey,
        advert_name="CLI RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    observed_kwargs: dict[str, object] = {}

    async def fake_probe_repeater_as_guest(self, *, probe_run_id: int, repeater_id: int, **kwargs) -> None:
        observed_kwargs.update(kwargs)
        self.database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=1,
            firmware_capability_level=2,
            login_server_time=123,
            error_message=None,
        )

    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_as_guest", fake_probe_repeater_as_guest)
    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-probe-now", "--config", str(config_path), str(repeater_id)])
    cli_main.main()
    output = capsys.readouterr().out

    assert "Endpoint: beta" in output
    assert observed_kwargs["endpoint"].name == "beta"


def test_cli_repeater_probe_prefers_endpoint_from_advert_path(tmp_path, monkeypatch, capsys) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.toml"
    config_path.write_text(
        """
[service]
name = "meshcore-bot"
log_level = "INFO"

[storage]
database_path = "./data/test-cli.db"

[identity]
key_file_path = "./data/identity.bin"

[gateway]
control_socket_path = "./data/gateway/control.sock"
event_socket_path = "./data/gateway/events.sock"

[[endpoints]]
name = "alpha"
raw_host = "127.0.0.1"
raw_port = 5002
enabled = true

[[endpoints]]
name = "beta"
raw_host = "127.0.0.1"
raw_port = 5003
enabled = true
""".strip()
    )

    pubkey = LocalIdentity.generate().public_key
    pubkey_hex = pubkey.hex().upper()
    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-add", "--config", str(config_path), "--pubkey", pubkey_hex, "--name", "CLI RPT"])
    cli_main.main()
    add_payload = json.loads(capsys.readouterr().out)
    repeater_id = int(add_payload["repeater_id"])

    config = load_config(config_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    database.upsert_repeater_from_advert(
        endpoint_name="beta",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=pubkey,
        advert_name="CLI RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-probe", "--config", str(config_path), str(repeater_id)])
    cli_main.main()
    payload = json.loads(capsys.readouterr().out)

    assert payload["endpoint_name"] == "beta"


def test_cli_repeater_probe_now_prefers_stored_preferred_endpoint(tmp_path, monkeypatch, capsys) -> None:
    config = build_multi_endpoint_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Okolna",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="CLI RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    database.set_repeater_preferred_endpoint(repeater_id=repeater_id, endpoint_name="RPT_Przesocin")

    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / "config.toml"
    save_raw_config(
        config_path,
        {
            "service": {"name": config.service.name, "log_level": config.service.log_level},
            "storage": {"database_path": "./meshcore-bot.db"},
            "identity": {"key_file_path": "./identity.bin"},
            "gateway": {
                "control_socket_path": "./gateway-control.sock",
                "event_socket_path": "./gateway-events.sock",
            },
            "endpoints": [
                {"name": endpoint.name, "raw_host": endpoint.raw_host, "raw_port": endpoint.raw_port, "enabled": endpoint.enabled}
                for endpoint in config.endpoints
            ],
        },
    )

    observed_kwargs: dict[str, object] = {}

    async def fake_probe_repeater_as_guest(self, *, probe_run_id: int, repeater_id: int, **kwargs) -> None:
        observed_kwargs.update(kwargs)
        self.database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=1,
            firmware_capability_level=2,
            login_server_time=123,
            error_message=None,
        )

    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_as_guest", fake_probe_repeater_as_guest)
    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-probe-now", "--config", str(config_path), str(repeater_id)])
    cli_main.main()
    output = capsys.readouterr().out

    assert "Endpoint: RPT_Przesocin" in output
    assert observed_kwargs["endpoint"].name == "RPT_Przesocin"


def test_cli_repeater_probe_now_uses_console_for_tcp_accessible_node(tmp_path, monkeypatch, capsys) -> None:
    config = build_local_console_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Przesocin",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="SZN_STO_OMNI_RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / "config.toml"
    save_raw_config(
        config_path,
        {
            "service": {"name": config.service.name, "log_level": config.service.log_level},
            "storage": {"database_path": "./meshcore-bot.db"},
            "identity": {"key_file_path": "./identity.bin"},
            "gateway": {
                "control_socket_path": "./gateway-control.sock",
                "event_socket_path": "./gateway-events.sock",
            },
            "endpoints": [
                {
                    **{
                        "name": endpoint.name,
                        "raw_host": endpoint.raw_host,
                        "raw_port": endpoint.raw_port,
                        "enabled": endpoint.enabled,
                    },
                    **({"local_node_name": endpoint.local_node_name} if endpoint.local_node_name else {}),
                }
                for endpoint in config.endpoints
            ],
        },
    )

    async def fake_console_probe(self, *, probe_run_id: int, repeater_id: int, endpoint, repeater_name: str | None) -> None:
        self.database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=3,
            firmware_capability_level=None,
            login_server_time=None,
            error_message=None,
        )

    radio_probe = AsyncMock(side_effect=AssertionError("radio probe should not run for TCP-accessible node"))
    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_via_console", fake_console_probe)
    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_as_guest", radio_probe)
    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-probe-now", "--config", str(config_path), str(repeater_id)])

    cli_main.main()
    output = capsys.readouterr().out

    assert "Endpoint: RPT_Okolna" in output
    assert "Probe completed successfully" in output
    radio_probe.assert_not_awaited()


def test_probe_repeater_via_console_retries_empty_neighbors_reply(tmp_path) -> None:
    config = build_local_console_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Okolna",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="SZN_STO_OMNI_RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name="RPT_Okolna")
    worker = GuestProbeWorker(config, database)
    endpoint = next(item for item in config.endpoints if item.name == "RPT_Okolna")

    responses = AsyncMock(
        side_effect=[
            "SZN_STO_OMNI_RPT",
            "1.2.3",
            "Owner|Info",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "01C97DDB:238:12\n35D4F997:275:-10",
        ]
    )

    async def scenario() -> None:
        with patch("meshcore_bot.probe_service.run_console_command", responses):
            await worker.probe_repeater_via_console(
                probe_run_id=probe_run_id,
                repeater_id=repeater_id,
                endpoint=endpoint,
                repeater_name="SZN_STO_OMNI_RPT",
            )

    asyncio.run(scenario())

    neighbours = database.latest_repeater_neighbours(repeater_id=repeater_id, limit=16)
    recent_runs = database.repeater_recent_probe_runs(repeater_id=repeater_id, limit=1)

    assert len(neighbours) == 2
    assert recent_runs[0]["result"] == "success"
    assert responses.await_count == 16


def test_local_console_endpoint_resolver_retries_after_transient_get_name_failure(tmp_path) -> None:
    base_config = build_test_app_config(tmp_path)
    config = replace(
        base_config,
        endpoints=(
            EndpointConfig(
                name="RPT_Okolna",
                raw_host="127.0.0.1",
                raw_port=5002,
                enabled=True,
                console_mirror_host="127.0.0.2",
                console_mirror_port=5003,
            ),
        ),
    )
    resolver = LocalConsoleEndpointResolver(config)
    endpoint = next(item for item in config.endpoints if item.name == "RPT_Okolna")
    responses = AsyncMock(side_effect=[RuntimeError("temporary console failure"), "SZN_STO_OMNI_RPT"])

    async def scenario() -> None:
        with patch("meshcore_bot.probe_service.run_console_command", responses):
            assert await resolver.resolve_endpoint_local_node_name(endpoint) is None
            assert await resolver.resolve_endpoint_local_node_name(endpoint) == "SZN_STO_OMNI_RPT"

    asyncio.run(scenario())

    assert responses.await_count == 2


def test_probe_repeater_via_console_fails_after_exhausted_empty_neighbors_reply(tmp_path) -> None:
    config = build_local_console_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Okolna",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="SZN_STO_OMNI_RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    probe_run_id = database.create_probe_run(repeater_id=repeater_id, endpoint_name="RPT_Okolna")
    worker = GuestProbeWorker(config, database)
    endpoint = next(item for item in config.endpoints if item.name == "RPT_Okolna")
    responses = AsyncMock(side_effect=["SZN_STO_OMNI_RPT", "1.2.3", "Owner|Info", *([""] * worker.CONSOLE_NEIGHBORS_RETRY_ATTEMPTS)])

    async def scenario() -> None:
        with patch("meshcore_bot.probe_service.run_console_command", responses):
            await worker.probe_repeater_via_console(
                probe_run_id=probe_run_id,
                repeater_id=repeater_id,
                endpoint=endpoint,
                repeater_name="SZN_STO_OMNI_RPT",
            )

    try:
        asyncio.run(scenario())
    except RuntimeError as exc:
        assert str(exc) == "console neighbors command returned empty response on endpoint RPT_Okolna"
    else:
        raise AssertionError("console probe should fail when neighbors reply stays empty")

    recent_runs = database.repeater_recent_probe_runs(repeater_id=repeater_id, limit=1)
    assert recent_runs[0]["result"] == "running"


def test_cli_repeater_probe_enqueues_local_console_endpoint_for_tcp_accessible_node(tmp_path, monkeypatch, capsys) -> None:
    config = build_local_console_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Przesocin",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="SZN_STO_OMNI_RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )

    config_dir = tmp_path / "config"
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / "config.toml"
    save_raw_config(
        config_path,
        {
            "service": {"name": config.service.name, "log_level": config.service.log_level},
            "storage": {"database_path": "./meshcore-bot.db"},
            "identity": {"key_file_path": "./identity.bin"},
            "gateway": {
                "control_socket_path": "./gateway-control.sock",
                "event_socket_path": "./gateway-events.sock",
            },
            "endpoints": [
                {
                    **{
                        "name": endpoint.name,
                        "raw_host": endpoint.raw_host,
                        "raw_port": endpoint.raw_port,
                        "enabled": endpoint.enabled,
                    },
                    **({"local_node_name": endpoint.local_node_name} if endpoint.local_node_name else {}),
                }
                for endpoint in config.endpoints
            ],
        },
    )

    monkeypatch.setattr(sys, "argv", ["meshcore-bot", "rpt-probe", "--config", str(config_path), str(repeater_id)])
    cli_main.main()
    payload = json.loads(capsys.readouterr().out)

    assert payload["endpoint_name"] == "RPT_Okolna"


def test_probe_repeater_as_guest_uses_global_advert_path_when_endpoint_was_renamed(tmp_path) -> None:
    base_config = build_test_app_config(tmp_path)
    config = replace(
        base_config,
        probe=replace(base_config.probe, pre_login_advert_name=""),
        endpoints=(EndpointConfig(name="RPT_Okolna", raw_host="127.0.0.1", raw_port=5002, enabled=True),),
    )
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="rpt-primary",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="SZN_STO_OMNI_RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=3,
        path_hex="4805EF",
        raw_packet_hex="00",
    )
    worker = GuestProbeWorker(config, database, transport_factory=lambda endpoint: FakeTCPClient([]))
    fake_client = cast(FakeTCPClient, worker._transport_factory(config.endpoints[0]))
    worker._transport_factory = lambda endpoint: fake_client

    async def fail_discovery(**kwargs) -> tuple[int, bytes]:
        raise AssertionError("path discovery should not run when a fresh advert path exists")

    async def fake_login_response(**kwargs) -> tuple[bytes, int, bytes]:
        payload = struct.pack("<IBBBB4sB", 1234, 0, 0, 1, 3, b"ABCD", 2)
        return payload, 3, bytes.fromhex("4805EF")

    async def fake_settle_post_login_frames(**kwargs) -> tuple[int, bytes]:
        return int(kwargs["current_path_len"]), bytes(kwargs["current_path_bytes"])

    async def fake_send_with_tagged_response_retries(**kwargs) -> tuple[bytes, int, bytes]:
        return b"dummy", int(kwargs["current_path_len"]), bytes(kwargs["current_path_bytes"])

    worker._discover_repeater_path = AsyncMock(side_effect=fail_discovery)
    worker._await_login_response = AsyncMock(side_effect=fake_login_response)
    worker._settle_post_login_frames = AsyncMock(side_effect=fake_settle_post_login_frames)
    worker._send_with_tagged_response_retries = AsyncMock(side_effect=fake_send_with_tagged_response_retries)

    with (
        patch("meshcore_bot.probe_service.parse_neighbours_response") as parse_neighbours,
        patch("meshcore_bot.probe_service.parse_status_response") as parse_status,
        patch("meshcore_bot.probe_service.parse_owner_info_response") as parse_owner,
    ):
        parse_neighbours.return_value = type(
            "NeighboursResponse",
            (),
            {"neighbours_count": 1, "results_count": 1, "entries": []},
        )()
        parse_status.return_value = type("StatusResponse", (), {})()
        parse_owner.return_value = type(
            "OwnerInfoResponse",
            (),
            {"firmware_version": "v1", "node_name": "SZN_STO_OMNI_RPT", "owner_info": "owner"},
        )()

        asyncio.run(
            worker.probe_repeater_as_guest(
                probe_run_id=database.create_probe_run(repeater_id=repeater_id, endpoint_name="RPT_Okolna"),
                repeater_id=repeater_id,
                endpoint=config.endpoints[0],
                remote_pubkey=remote_identity.public_key,
                repeater_name="SZN_STO_OMNI_RPT",
            )
        )

    sent_summary = parse_packet(fake_client.sent_packets[0])
    assert sent_summary.path_len == 3
    assert sent_summary.path_bytes.hex().upper() == "4805EF"
    worker._discover_repeater_path.assert_not_awaited()


def test_run_job_uses_direct_console_for_local_endpoint_node(tmp_path, monkeypatch) -> None:
    config = build_local_console_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Przesocin",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="SZN_STO_OMNI_RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="RPT_Okolna",
        reason="manual cli probe",
        success_cooldown_secs=0.0,
        failure_cooldown_secs=0.0,
    )
    assert job_id is not None
    job = database.claim_probe_job()
    assert job is not None
    worker = GuestProbeWorker(config, database, transport_factory=lambda endpoint: FakeTCPClient([]))

    async def fake_console_probe(self, *, probe_run_id: int, repeater_id: int, endpoint, repeater_name: str | None) -> None:
        self.database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=3,
            firmware_capability_level=None,
            login_server_time=None,
            error_message=None,
        )

    radio_probe = AsyncMock(side_effect=AssertionError("radio probe should not run for local console node"))
    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_via_console", fake_console_probe)
    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_as_guest", radio_probe)

    asyncio.run(worker._run_job(job))

    state = database.repeater_full_state(repeater_id=repeater_id)
    assert state is not None
    assert state["last_probe_status"] == "success"
    assert state["preferred_endpoint_name"] == "RPT_Okolna"
    radio_probe.assert_not_awaited()


def test_run_job_redirects_local_node_to_matching_console_endpoint(tmp_path, monkeypatch) -> None:
    config = build_local_console_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Przesocin",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="SZN_STO_OMNI_RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    job_id = database.enqueue_probe_job(
        repeater_id=repeater_id,
        endpoint_name="RPT_Przesocin",
        reason="scheduled stale refresh",
        success_cooldown_secs=0.0,
        failure_cooldown_secs=0.0,
    )
    assert job_id is not None
    job = database.claim_probe_job()
    assert job is not None
    worker = GuestProbeWorker(config, database, transport_factory=lambda endpoint: FakeTCPClient([]))
    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_via_console", AsyncMock(side_effect=AssertionError("should redirect before probing")))
    monkeypatch.setattr(GuestProbeWorker, "probe_repeater_as_guest", AsyncMock(side_effect=AssertionError("should redirect before probing")))

    asyncio.run(worker._run_job(job))

    state = database.repeater_full_state(repeater_id=repeater_id)
    assert state is not None
    probe_jobs = database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=10)
    redirected_jobs = [
        item for item in probe_jobs
        if item["endpoint_name"] == "RPT_Okolna" and item["reason"] == GuestProbeWorker.LOCAL_CONSOLE_REDIRECT_REASON
    ]
    original_jobs = [item for item in probe_jobs if item["endpoint_name"] == "RPT_Przesocin"]
    assert redirected_jobs
    assert original_jobs[0]["status"] == "completed"


def test_schedule_stale_repeater_probe_jobs_prefers_stored_endpoint(tmp_path) -> None:
    config = build_multi_endpoint_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Okolna",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="Sched RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    database.set_repeater_preferred_endpoint(repeater_id=repeater_id, endpoint_name="RPT_Przesocin")

    enqueued = database.schedule_stale_repeater_probe_jobs(
        endpoint_names=[endpoint.name for endpoint in config.endpoints],
        stale_after_secs=3600.0,
        seen_within_secs=3600.0,
        reason="scheduled stale refresh",
        success_cooldown_secs=0.0,
        failure_cooldown_secs=0.0,
    )

    assert enqueued == 1
    jobs = database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=5)
    assert jobs[0]["endpoint_name"] == "RPT_Przesocin"


def test_run_job_success_sets_preferred_endpoint(tmp_path) -> None:
    config = build_multi_endpoint_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Okolna",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="Probe RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    job_id = database.enqueue_probe_job(repeater_id=repeater_id, endpoint_name="RPT_Przesocin", reason="manual cli probe")
    assert job_id is not None
    job = database.claim_probe_job()
    assert job is not None

    worker = GuestProbeWorker(config, database)

    async def fake_probe_repeater_as_guest(*, probe_run_id: int, repeater_id: int, endpoint: EndpointConfig, **kwargs) -> None:
        database.complete_probe_run(
            probe_run_id,
            repeater_id=repeater_id,
            result="success",
            guest_login_ok=True,
            guest_permissions=1,
            firmware_capability_level=2,
            login_server_time=123,
            error_message=None,
        )

    worker.probe_repeater_as_guest = fake_probe_repeater_as_guest  # type: ignore[method-assign]
    asyncio.run(worker._run_job(job))

    preferred = database.preferred_repeater_endpoint(repeater_id=repeater_id)
    assert preferred is not None
    assert preferred["preferred_endpoint_name"] == "RPT_Przesocin"


def test_run_job_failure_enqueues_fallback_jobs_once(tmp_path) -> None:
    config = build_multi_endpoint_test_app_config(tmp_path)
    database = BotDatabase(config.storage.database_path)
    database.initialize()
    remote_identity = LocalIdentity.generate()
    repeater_id = database.upsert_repeater_from_advert(
        endpoint_name="RPT_Okolna",
        observed_at=datetime.now(tz=UTC).isoformat(),
        public_key=remote_identity.public_key,
        advert_name="Fail RPT",
        advert_lat=None,
        advert_lon=None,
        advert_timestamp_remote=1,
        path_len=1,
        path_hex="35",
        raw_packet_hex="00",
    )
    job_id = database.enqueue_probe_job(repeater_id=repeater_id, endpoint_name="RPT_Okolna", reason="manual cli probe")
    assert job_id is not None
    job = database.claim_probe_job()
    assert job is not None

    worker = GuestProbeWorker(config, database)

    async def failing_probe(*, probe_run_id: int, repeater_id: int, endpoint: EndpointConfig, **kwargs) -> None:
        raise RuntimeError(f"failed via {endpoint.name}")

    worker.probe_repeater_as_guest = failing_probe  # type: ignore[method-assign]
    asyncio.run(worker._run_job(job))

    jobs = database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=10)
    endpoint_names = {(item["endpoint_name"], item["reason"], item["status"]) for item in jobs}
    assert ("RPT_Okolna", "manual cli probe", "failed") in endpoint_names
    assert ("RPT_Przesocin", GuestProbeWorker.ENDPOINT_FALLBACK_REASON, "pending") in endpoint_names
    assert ("RPT_Zapas", GuestProbeWorker.ENDPOINT_FALLBACK_REASON, "pending") in endpoint_names

    fallback_job = database.claim_probe_job()
    assert fallback_job is not None
    asyncio.run(worker._run_job(fallback_job))
    jobs_after = database.probe_jobs_for_repeater(repeater_id=repeater_id, limit=10)
    fallback_pending = [item for item in jobs_after if item["reason"] == GuestProbeWorker.ENDPOINT_FALLBACK_REASON and item["status"] == "pending"]
    assert len(fallback_pending) == 1


def test_select_login_candidates_forced_login_disables_empty_fallback() -> None:
    config = ProbeConfig(
        key_file_path=None,
        admin_password="admin-secret",
        admin_password_name_prefixes=("RAKU",),
        admin_password_pubkey_prefixes=(),
        guest_password="hello",
        default_guest_password="",
        guest_password_name_prefixes=("RAKU",),
        guest_password_pubkey_prefixes=(),
        pre_login_advert_name="",
        pre_login_advert_delay_secs=0.0,
        advert_reprobe_success_cooldown_secs=60.0,
        advert_reprobe_failure_cooldown_secs=60.0,
        advert_probe_min_interval_secs=10.0,
        advert_path_change_cooldown_secs=300.0,
        automatic_probe_max_per_day=3,
        scheduled_reprobe_interval_secs=28800.0,
        night_failed_retry_start_hour=1,
        night_failed_retry_end_hour=7,
        night_failed_retry_interval_secs=3600.0,
        poll_interval_secs=2.0,
        request_timeout_secs=8.0,
        route_freshness_secs=1800.0,
        neighbours_page_size=15,
        neighbours_prefix_len=4,
    )

    candidates = select_login_candidates(
        config=config,
        remote_pubkey=bytes.fromhex("C11A7386A9A47C7BF08F0E20B5F90A75D10E918BEE2FB49060198EE6E0D7DB07"),
        repeater_name="Drzetowo Dw. SZN RAKU",
        forced_login=("guest", "hello"),
        allow_default_guest_fallback=False,
    )

    assert candidates == [("guest", "hello")]
