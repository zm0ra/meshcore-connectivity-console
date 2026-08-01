# meshcore-tcp-bot (MeshCore Connectivity Console)

MeshCore operations stack built around serial-over-TCP: collects adverts, probes
repeaters, stores neighbour history in SQLite, serves a web dashboard, CLI for
inspection/maintenance. Expects MeshCore exposed via `meshcore-xiao-wifi-serial2tcp`
or a compatible RS232 bridge.

## Stack
- Python (`pyproject.toml`, package `meshcore_bot`/`meshcore_tcp_bot`), Docker/`docker-compose`
- Tests under `tests/`
- `update-stack.sh` — deploy/update entrypoint

## Conventions
- `docs/` — architecture/ops notes, read before infra changes
- `config/`, `data/`, `logs/` — runtime state, don't commit contents
