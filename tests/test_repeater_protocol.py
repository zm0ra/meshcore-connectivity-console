import struct

from datetime import UTC, datetime, timedelta

from meshcore_bot.config import ProbeConfig
from meshcore_bot.identity import LocalIdentity
from meshcore_bot.mesh_builders import build_advert_packet, build_login_packet, next_request_tag, next_wire_timestamp, parse_anon_request
from meshcore_bot.mesh_packets import AdvertType, PayloadType, RouteType, parse_advert
from meshcore_bot.channels import channel_hash, derive_hashtag_secret, hashtag_psk_base64
from meshcore_bot.probe_service import is_recent_observation, select_login_candidates, select_login_route_attempts
from meshcore_bot.repeater_protocol import (
    parse_login_response,
    parse_neighbours_response,
    parse_owner_info_response,
    parse_status_response,
)


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
    assert parsed.n_recv_errors == 7


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


def test_build_advert_packet_roundtrip() -> None:
    identity = LocalIdentity.generate()
    packet = build_advert_packet(identity=identity, name="441CFEA26666", advert_type=int(AdvertType.CHAT))
    assert packet.summary.payload_type is PayloadType.ADVERT
    assert packet.summary.route_type is RouteType.DIRECT
    advert = parse_advert(packet.summary)
    assert advert.public_key == identity.public_key
    assert advert.advert_type is AdvertType.CHAT
    assert advert.name == "441CFEA26666"


def test_next_wire_timestamp_is_monotonic() -> None:
    first = next_wire_timestamp(100)
    second = next_wire_timestamp(99)
    third = next_wire_timestamp(101)
    assert first == 100
    assert second == 99
    assert third == 101


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


def test_select_login_route_attempts_prefers_local_zero_hop_visibility() -> None:
    attempts = select_login_route_attempts(fresh_path=(2, bytes.fromhex("3548")), local_zero_hop_visible=True)
    assert attempts == [(0, b"")]


def test_select_login_route_attempts_uses_fresh_direct_path_when_not_local() -> None:
    attempts = select_login_route_attempts(fresh_path=(2, bytes.fromhex("3548")), local_zero_hop_visible=False)
    assert attempts == [(2, bytes.fromhex("3548"))]


def test_select_login_route_attempts_returns_empty_without_route_or_local_visibility() -> None:
    assert select_login_route_attempts(fresh_path=None, local_zero_hop_visible=False) == []
