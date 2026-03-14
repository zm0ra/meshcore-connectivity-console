import struct

from meshcore_bot.mesh_packets import AdvertType, PayloadType, RouteType, parse_advert, parse_packet


def test_parse_minimal_repeater_advert() -> None:
    pubkey = bytes(range(32))
    timestamp = 1_710_000_000
    signature = bytes([0xAA]) * 64
    flags = AdvertType.REPEATER | 0x80
    name = b"rpt-main"
    payload = pubkey + struct.pack("<I", timestamp) + signature + bytes([flags]) + name
    packet = bytes([(PayloadType.ADVERT << 2) | RouteType.FLOOD, 0x00]) + payload

    summary = parse_packet(packet)
    advert = parse_advert(summary)

    assert summary.payload_type is PayloadType.ADVERT
    assert summary.route_type is RouteType.FLOOD
    assert advert.public_key == pubkey
    assert advert.timestamp == timestamp
    assert advert.advert_type is AdvertType.REPEATER
    assert advert.name == "rpt-main"
    assert advert.latitude is None
    assert advert.longitude is None
