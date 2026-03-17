# MeshCore Connectivity Console

MeshCore Connectivity Console is a Docker-first MeshCore operations stack for collecting repeater neighbor data, probing reachable nodes, serving a connectivity dashboard, and running a small channel bot.

The project is built around one hard requirement: it must talk to MeshCore over serial-over-TCP exposed by [meshcore-xiao-wifi-serial2tcp](https://github.com/zm0ra/meshcore-xiao-wifi-serial2tcp). That bridge is not optional in this setup.

## What it does

- connects to an RS232Bridge-compatible TCP endpoint, usually on port `5002`
- ingests MeshCore adverts and stores repeater metadata in SQLite
- logs into reachable repeaters and fetches neighbor snapshots
- builds a directed connectivity graph from the latest known neighbor data
- calculates directional path results for `A->B` and `B->A` separately
- serves a desktop and mobile web UI for map, connectivity, and route analysis
- runs a minimal hashtag-channel bot over the same gateway layer

## Why it is complicated

This is not a thin wrapper around an existing serial client.

The runtime builds MeshCore packets itself, encrypts and MACs payloads itself, wraps them into the RS232 bridge framing itself, and sends those frames over TCP to the serial bridge. It also parses the replies itself.

In practice this means the stack owns the full path below:

1. build MeshCore request or text payloads
2. encrypt and authenticate them using the expected MeshCore wire format
3. wrap them in the RS232 bridge frame format
4. send the frame to the serial-over-TCP bridge
5. decode responses, store snapshots, and derive graph views for the UI

That is why this repository is more involved than a typical dashboard or bot project.

## Must-have dependency

This project requires a repeater or Companion-side setup that exposes the serial interface you want to consume, plus a separate serial-to-TCP bridge in front of it.

The expected bridge implementation is:

- [meshcore-xiao-wifi-serial2tcp](https://github.com/zm0ra/meshcore-xiao-wifi-serial2tcp)

Without that bridge layer, this repository is not enough on its own.

## Architecture

The stack is split into four long-running services plus two one-shot initialization steps:

- `init-db`: initializes the SQLite schema
- `ensure-identity`: creates or loads the local MeshCore identity
- `bridge-gateway`: the only process allowed to hold the TCP connection to the serial bridge
- `neighbours-worker`: ingests adverts, schedules probes, fetches neighbor snapshots, and stores results
- `bot-worker`: listens on configured hashtag channels and replies to a limited command set
- `web`: serves the FastAPI dashboard and `/api/state`

SQLite remains file-based in the shared volume. There is no separate database container.

## Connectivity model

The dashboard works on directed edges.

- `A -> B` means repeater `A` reported that it sees repeater `B`
- missing reverse edge does not imply symmetry
- route search is directional and always computed independently for `A->B` and `B->A`
- the graph represents the latest known neighbor snapshots, not a guaranteed live routing table

The current implementation uses the latest known directed links and shortest-hop BFS for route exploration.

## Desktop and mobile UI

The web UI is designed for both desktop and mobile.

- desktop uses a map-first operator workflow for connectivity and route inspection
- mobile uses a split `Map` / `Analysis` flow instead of a compressed desktop clone
- route results are shown per direction, not as a single symmetric answer
- connectivity views explicitly distinguish outbound, inbound, and mutual visibility

## Documentation

Public documentation assets live in `docs/`.

- `docs/README.md` contains the documentation asset layout
- `docs/screenshots/README.md` contains the screenshot naming scheme and suggested captions

## Screenshots

Documentation screenshots should live under `docs/screenshots/`.

Recommended filenames:

- `docs/screenshots/dashboard-overview-desktop.png`
- `docs/screenshots/connectivity-list-desktop.png`
- `docs/screenshots/signal-history-desktop.png`
- `docs/screenshots/mobile-map-outbound.png`
- `docs/screenshots/mobile-map-mutual.png`
- `docs/screenshots/route-analysis-desktop.png`

Once those files are present in the repository, add them to this section with standard Markdown image references.

## Bot scope

The bot is intentionally small and operationally narrow.

- it listens only on hashtag channels from `[bot].channels`
- it supports only commands from `[bot].enabled_commands`
- current supported commands are `!ping`, `!test`, and `!help`
- it does not send self adverts
- it does not handle private messages

## Public configuration

The public repository is expected to keep only example configuration.

- `config/config.example.toml` contains placeholder values only
- `docker-compose.example.yml` is the public compose baseline
- local copies such as `config/config.toml`, `docker-compose.yml`, and `docker-compose.override.yml` should not be published with private endpoints or host-specific network details

Most important configuration sections:

- `[endpoints]`: serial-over-TCP targets
- `[gateway]`: Unix sockets shared between containers
- `[probe]`: repeater probe behavior, credentials, retry windows
- `[bot]`: enabled channels, commands, response behavior
- `[web]`: dashboard bind address

Use `python -m meshcore_bot show-config --config config/config.toml` to print the resolved local configuration.

## Local run

```bash
cp config/config.example.toml config/config.toml
cp docker-compose.example.yml docker-compose.yml
docker compose up -d --build
```

Check logs:

```bash
docker compose logs --tail 100 bridge-gateway neighbours-worker bot-worker web
```

Stop everything:

```bash
docker compose down
```

## Technical notes

Relevant implementation areas:

- `meshcore_bot/mesh_builders.py`: builds MeshCore packets and parses encrypted replies
- `meshcore_bot/rs232.py`: encodes and decodes RS232 bridge frames
- `meshcore_bot/bridge_gateway.py`: owns the TCP session and exposes local gateway IPC
- `meshcore_bot/neighbours_worker.py` and `meshcore_bot/probe_service.py`: probing, retries, and snapshot collection
- `meshcore_bot/database.py`: SQLite persistence and web-facing graph queries
- `meshcore_bot/web_service.py`: FastAPI app and the desktop/mobile dashboard

## Security and publishing notes

This repository should not publish:

- private endpoint addresses
- live passwords
- local identity files
- local databases
- host-specific Docker network settings

Before pushing to a public remote, review tracked files and keep only sanitized examples.
