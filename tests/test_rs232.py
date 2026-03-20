from meshcore_bot.rs232 import RS232BridgeDecoder, decode_frame, encode_frame, fletcher16


def test_encode_decode_roundtrip() -> None:
    payload = bytes.fromhex("01020304AA55")
    frame = encode_frame(payload)
    decoded = decode_frame(frame)
    assert decoded.payload == payload
    assert decoded.payload_len == len(payload)
    assert decoded.checksum == fletcher16(payload)


def test_stream_decoder_handles_partial_and_multiple_frames() -> None:
    payload_1 = bytes.fromhex("010203")
    payload_2 = bytes.fromhex("AABBCCDDEE")
    stream = encode_frame(payload_1) + encode_frame(payload_2)

    decoder = RS232BridgeDecoder()
    first = decoder.feed(stream[:5])
    assert first == []

    second = decoder.feed(stream[5:])
    assert [item.payload for item in second] == [payload_1, payload_2]
