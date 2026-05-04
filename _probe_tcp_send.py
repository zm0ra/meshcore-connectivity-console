import select
import socket
import time

from meshcore_tcp_bot.config import load_config
from meshcore_tcp_bot.packets import build_group_text_packet
from meshcore_tcp_bot.protocol import encode_frame

cfg = load_config('config/config.toml')
endpoint = next(ep for ep in cfg.endpoints if ep.enabled)
channel = next(ch for ch in cfg.channels if ch.name == 'bot-test')
payload = build_group_text_packet('MeshBot', '[manual4] probe via tcp', channel_psk=channel.psk, channel_name=channel.name)
frame = encode_frame(payload)

mirror = socket.create_connection((endpoint.raw_host, endpoint.console_mirror_port), timeout=5)
mirror.setblocking(False)
raw = socket.create_connection((endpoint.raw_host, endpoint.raw_port), timeout=5)
raw.settimeout(5)

try:
    time.sleep(0.5)
    raw.sendall(frame)
    print('sent_frame_len=', len(frame))
    print('packet_hex=', payload.hex())
    deadline = time.time() + 3.0
    chunks = []
    while time.time() < deadline:
        readable, _, _ = select.select([mirror], [], [], 0.25)
        if not readable:
            continue
        data = mirror.recv(4096)
        if not data:
            break
        chunks.append(data)
    output = b''.join(chunks).decode('utf-8', errors='replace')
    print('mirror_output_start')
    print(output[-4000:])
    print('mirror_output_end')
finally:
    raw.close()
    mirror.close()
