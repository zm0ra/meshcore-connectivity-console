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

## Bot scope

The bot is intentionally narrow:

- it listens only on hashtag channels from `[bot].channels`
- it supports only commands from `[bot].enabled_commands`
- current supported commands are `!ping`, `!test`, and `!help`
- it does not send self adverts
- it does not handle private messages

All bot behavior is configured in one place: `[bot]` in `config/config.toml`.

## Probe scheduling

- advert-triggered probe jobs are rate-limited per repeater and endpoint
- successful advert probes can be retriggered sooner than failed ones
- failed repeaters that are still advertising can be retried automatically during the night window, default `01:00-07:00`, once per hour
- manual jobs bypass advert cooldowns because they use distinct reasons
- old failed jobs can be pruned with `python -m meshcore_bot cleanup-probe-jobs --failed-older-than-hours 12`

## Configuration

Main runtime configuration lives in `config/config.toml`.

Most important sections:

- `[endpoints]`: RS232Bridge TCP targets
- `[gateway]`: Unix sockets shared between containers
- `[probe]`: repeater probe behavior, credentials, retry windows
- `[bot]`: enabled channels, supported commands, reply timing, quiet window
- `[web]`: dashboard bind address

Use `python -m meshcore_bot show-config --config config/config.toml` to print the resolved configuration.

## Local Docker run

Build and start the full stack:

```bash
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

1. Review `config/config.toml`, especially `[endpoints]`, `[probe]`, and `[bot]`.
2. Ensure `data/` is persistent on the target host.
3. Review `docker-compose.yml` and adapt any host-specific network and publishing settings for your target environment.
4. Bring the stack up with `docker compose up -d --build`.
5. Verify `bridge-gateway` logs show `gateway connected` for the production endpoint.
6. Verify `bot-worker` is listening on the channels you configured.

## Current status

This repo is intended for deployment once configuration is reviewed. It is not trying to be full Companion parity. The bot layer is now deliberately minimal so operational behavior is easier to reason about and support.
