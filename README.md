# meshcore-bot

Docker-first MeshCore runtime for two concrete jobs:

- harvesting repeater data over RS232Bridge TCP on port `5002`
- running a small channel bot on configured MeshCore hashtag channels

Reference and reverse-engineering notes live in `../trunk/`. Runtime code lives only in this directory.

## Runtime layout

- `bridge-gateway`: the only process allowed to hold the TCP connection to RS232Bridge
- `neighbours-worker`: advert ingestion plus repeater probing over gateway IPC
- `bot-worker`: channel-only command bot over gateway IPC
- `web`: SQLite-backed status UI

SQLite stays file-based in the shared volume. There is no separate database container because this deployment is intentionally single-host and that extra moving part would not improve correctness.

## RS232 over TCP requirement

This runtime does not talk to a repeater over USB directly. It expects an RS232Bridge-compatible TCP endpoint, typically on port `5002`.

That part is not plug-and-play in stock repeater / Companion setups. To use this project you need both:

- a MeshCore repeater / Companion build that exposes the RS232 serial interface you want to consume
- a separate serial-to-TCP bridge that publishes that RS232 stream on the network in the framing expected by this runtime

In this deployment we use [meshcore-xiao-wifi-serial2tcp](https://github.com/zm0ra/meshcore-xiao-wifi-serial2tcp) for that bridge layer.

Practical meaning:

- first get your repeater / Companion side working with RS232 available on the serial pins you intend to use
- then put the TCP bridge in front of it
- only after that point this repo can connect via `[endpoints].raw_host` and `[endpoints].raw_port`

If you do not already have RS232-over-TCP exposed, this repo alone is not enough to create it.

## Bot scope

The bot is intentionally narrow:

- it listens only on hashtag channels from `[bot].channels`
- it supports only commands from `[bot].enabled_commands`
- current supported commands are `!ping`, `!test`, and `!help`
- it does not send self adverts
- it does not handle private messages

All bot behavior is configured in one place: `[bot]` in your local `config/config.toml`, created from `config/config.example.toml`.

## Probe scheduling

- advert-triggered probe jobs are rate-limited per repeater and endpoint
- successful advert probes can be retriggered sooner than failed ones
- failed repeaters that are still advertising can be retried automatically during the night window, default `01:00-07:00`, once per hour
- manual jobs bypass advert cooldowns because they use distinct reasons
- old failed jobs can be pruned with `python -m meshcore_bot cleanup-probe-jobs --failed-older-than-hours 12`

## Configuration

Public example configuration lives in `config/config.example.toml`.

For local/runtime use:

- copy `config/config.example.toml` to `config/config.toml`
- copy `docker-compose.example.yml` to `docker-compose.yml`
- fill in your own endpoints, credentials, channels, and host-specific Docker settings

Most important sections:

- `[endpoints]`: RS232Bridge TCP targets
- `[gateway]`: Unix sockets shared between containers
- `[probe]`: repeater probe behavior, credentials, retry windows
- `[bot]`: enabled channels, supported commands, reply timing, quiet window
- `[web]`: dashboard bind address

Use `python -m meshcore_bot show-config --config config/config.toml` to print the resolved local configuration.

## Local Docker run

Build and start the full stack:

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

## Production copy checklist

Before copying this to the target machine:

1. Create and review `config/config.toml` from `config/config.example.toml`, especially `[endpoints]`, `[probe]`, and `[bot]`.
2. Ensure `data/` is persistent on the target host.
3. Create and review `docker-compose.yml` from `docker-compose.example.yml` and adapt host-specific network and publishing settings for your target environment.
4. Bring the stack up with `docker compose up -d --build`.
5. Verify `bridge-gateway` logs show `gateway connected` for the production endpoint.
6. Verify `bot-worker` is listening on the channels you configured.

The public repo keeps only example configuration. Local runtime copies such as `config/config.toml`, `docker-compose.yml`, and `docker-compose.override.yml` are intentionally not tracked.

## Current status

This repo is intended for deployment once configuration is reviewed. It is not trying to be full Companion parity. The bot layer is now deliberately minimal so operational behavior is easier to reason about and support.
