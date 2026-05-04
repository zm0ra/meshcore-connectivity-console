# meshcore-tcp-bot

MeshCore TCP bot for repeaters running the XIAO WiFi RS232Bridge wrapper from `meshcore-xiao-wifi-serial2tcp`.

This project is built from scratch around the raw TCP packet bridge. It connects to one or more repeaters, listens on configured public channels, decodes real MeshCore packets, tracks known nodes from adverts, and sends replies by assembling raw packets directly.

For the next step toward full private-message and repeater-management support, see `COMPANION_MODE_ANALYSIS.md`.

## Scope

Current capabilities:

- Multi-endpoint TCP connections to MeshCore repeaters
- Continuous receive loop on raw RS232Bridge TCP streams
- Optional console-mirror ingestion for real per-packet SNR/RSSI enrichment
- Public-channel command handling for `!ping`, `!test`, `!trace`, and `!help`
- Node registry built from live ADVERT packets
- SQLite persistence for adverts and known nodes across restarts
- Configurable management target registry for future guest/admin repeater sessions
- Authenticated `/admin` panel for runtime configuration persisted in SQLite
- Distance calculation using repeater coordinates and node advert locations
- Built-in HTTP API and a simple browser viewer for known nodes and recent messages
- Private-message command handling and optional private auto-replies
- Docker and docker-compose deployment

## TCP framing

The repeater firmware exposes the raw packet bridge on port `5002`.
Each frame uses the RS232Bridge wrapper:

```text
[magic:2] [length:2] [payload:N] [fletcher16:2] [newline:1]
 C0 3E    00 15      ...         D9 B0          0A
```

Rules implemented here match the firmware wrapper:

- `magic` is always `C0 3E`
- `length` is big-endian
- Fletcher-16 is calculated over the MeshCore payload only
- input ignores CR and LF delimiters
- the validated payload is the exact `Packet::writeTo()` wire format

## Commands

The bot currently reacts on configured public channels. The default test setup listens on `#bot-test` so development traffic stays away from `#bot` and `#test`.

- `!ping` replies with `pong`
- `!test` reports only verified data collected for the triggering packet
- `!trace` shows the hop path derived from the incoming packet path
- `!help` returns a short command summary
- `!neighbors` reports nearby repeaters from persisted adverts and later management snapshots

All command replies are delayed by at least 1 second.

Example `!test` response style:

```text
[MeshBot] I saw: MT01 (hops=3, snr=1.5, rssi=-97, dist=15.96km)
```

Important:

- `hops` comes from the packet path length
- `dist` is calculated only when both the repeater and sender location are known
- `snr` and `rssi` are included only when the endpoint provides real telemetry, typically via console mirror
- no placeholder RF values are fabricated

## Configuration

Configuration is provided in TOML.
See `config/config.example.toml`.

Minimal endpoint example:

```toml
[[endpoints]]
name = "rpt-primary"
enabled = true
raw_host = "172.30.105.24"
raw_port = 5002
console_mirror_host = "172.30.105.24"
console_mirror_port = 5003
latitude = 52.22977
longitude = 21.01178
```

For hashtag channels, the bot can derive the channel key automatically from the channel name using the same firmware rule as MeshCore: first 16 bytes of `sha256("#channel-name")`.
If `psk` is omitted in the config, the bot uses that derived hashtag key. For the default repository setup, the test channel is `bot-test`.

Persistent state is stored in SQLite. By default the database path is `./data/meshcore-bot.db`.

The bot now also keeps its own persistent MeshCore identity in `./data/bot-identity.json` by default. This identity is used for encrypted private datagrams, repeater login, and management requests.

Management targets are configured separately from TCP endpoints so individual repeaters can carry their own guest/admin passwords and identity hints:

```toml
[[management_nodes]]
name = "rpt-primary"
endpoint_name = "rpt-primary"
target_hash_prefix = "4E"
guest_password = "hello"
admin_password = "password"
prefer_role = "guest"
enabled = true
```

Current status of management support:

- the bot now has a persistent MeshCore identity and active ANON_REQ/REQ/RESPONSE support for repeater sessions
- when repeater adverts are seen, the bot can auto-register those repeaters as management targets and try a guest login using the shared `management.auto_guest_password`
- blank-password login is not a generic guest login; upstream firmware only accepts that when the bot is already in the repeater ACL
- live `owner info` and `neighbors` polling works after successful login; `ACL` polling still requires admin credentials

Auto-discovery notes:

- auto-discovery is controlled by `[management].auto_discover_from_adverts`
- guest-first login for newly discovered repeaters uses `[management].auto_guest_password`
- optional escalation to admin for ACL polling can use `[management].auto_admin_password`
- discovered repeaters are stored in the management target registry and appear in the viewer even before a successful login

## Admin panel

The bot now exposes an authenticated admin panel at `/admin`.

Login is configured through Docker environment variables:

```env
MESHCORE_ADMIN_PASSWORD=changeme
MESHCORE_ADMIN_SESSION_SECRET=replace-with-long-random-secret
```

If `MESHCORE_ADMIN_PASSWORD` is omitted, the bot falls back to `changeme`.

The admin secret is a single password field. It can be a normal password or a numeric-only value, but the system treats it as one secret everywhere.

Key regeneration requires typing `REGENERATE` and then entering the same admin password again.

The admin panel persists runtime settings in SQLite and applies them to the running bot. It covers:

- endpoint add/update/delete for raw TCP repeater connections
- known repeater / room-server target add/update/delete
- guest/admin passwords per management target, used directly by live management logic
- enable/disable command handlers
- per-command response template editing
- bot name, reply prefix, private-message replies, and history limits
- channel list and listen toggles
- bot identity inspection and guarded key regeneration

Important operational note:

- once the admin panel has seeded runtime settings, SQLite becomes the active source for editable bot/runtime settings; TOML remains the bootstrap source for first run and non-editable settings

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config/config.example.toml config/config.toml
python -m meshcore_tcp_bot --config config/config.toml
```

HTTP viewer:

- `http://127.0.0.1:8080/`
- `http://127.0.0.1:8080/admin`
- health endpoint: `http://127.0.0.1:8080/healthz`

## Docker

```bash
cp config/config.example.toml config/config.toml
cp .env.example .env
docker compose up --build -d
```

## Project layout

```text
meshcore-tcp-bot/
  config/
    config.example.toml
  meshcore_tcp_bot/
    __main__.py
    app.py
    config.py
    console.py
    models.py
    packets.py
    protocol.py
    service.py
    web.py
```

## Notes for future work

The container already exposes a web surface and in-memory node model so it can be extended into:

- a map of known repeaters and companions
- periodic admin-console polling for neighbor lists
- topology and signal-quality views across multiple repeaters

## Protocol source

This implementation follows the XIAO repeater wrapper and MeshCore packet behavior documented in `meshcore-xiao-wifi-serial2tcp`.