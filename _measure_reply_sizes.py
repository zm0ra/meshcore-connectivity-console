from datetime import UTC, datetime

from meshcore_tcp_bot.config import load_config
from meshcore_tcp_bot.models import MeshMessage
from meshcore_tcp_bot.packets import build_group_text_packet
from meshcore_tcp_bot.service import MeshcoreTCPBotService

cfg = load_config('config/config.toml')
service = MeshcoreTCPBotService(cfg)
msg = MeshMessage(
    endpoint_name='rpt-primary',
    channel_name='bot-test',
    channel_psk=None,
    sender='zmo FHGR+56Q Szczecin',
    sender_identity_hex=None,
    content='!test',
    packet_type='GRP_TXT',
    route_name='FLOOD',
    path_hashes=['35'],
    path_len=1,
    received_at=datetime.now(tz=UTC),
    channel_hash=0x35,
    snr=None,
    rssi=None,
    distance_km=None,
    raw_payload_hex='',
)
ctx = service._command_context(msg)
help_old = service._safe_format(service._default_command_settings()['help']['response_template'], ctx)
help_new = service._compact_channel_help_response()
test_text = service._safe_format(service._default_command_settings()['test']['response_template'], ctx)
channel = next(ch for ch in cfg.channels if ch.name == 'bot-test')
for label, text in [('help-old', help_old), ('help-new', help_new), ('test', test_text)]:
    pkt = build_group_text_packet(cfg.bot.name, text, channel_psk=channel.psk, channel_name=channel.name)
    print(label)
    print(text)
    print('text_len=', len(text), 'packet_len=', len(pkt))
    print('wire_plaintext_prefix_len=', len(f"{cfg.bot.name}: ".encode('utf-8')) + len(text.encode('utf-8')))
    print()
