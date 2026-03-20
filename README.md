# MeshCore Connectivity Console

MeshCore Connectivity Console is a Docker-first operations stack for collecting MeshCore repeater data over serial-over-TCP, storing probe history in SQLite, serving a web dashboard, and exposing a practical CLI for day-to-day inspection and maintenance.

It is meant for operators who need one place for:

- advert ingestion
- repeater discovery
- neighbour harvesting
- route and connectivity inspection
- lightweight bot-driven operational responses

## Scope

This repository is not a generic MeshCore SDK.

It implements a full runtime around a serial bridge exposed over TCP and assumes that a compatible serial-to-TCP layer already exists in front of the radio or companion device.

The expected bridge in this setup is:

- [meshcore-xiao-wifi-serial2tcp](https://github.com/zm0ra/meshcore-xiao-wifi-serial2tcp)

Without that bridge layer, this project cannot talk to MeshCore on its own.

## What the stack does

- opens and maintains TCP sessions to configured RS232 bridge endpoints
- ingests MeshCore adverts and stores discovered repeater metadata
- probes reachable repeaters and saves neighbour snapshots
- builds a directed connectivity model from the latest saved neighbour data
- serves a web UI for map, connectivity, and route inspection
- runs a small hashtag-channel bot on top of the same transport layer

## Transport model

The runtime distinguishes three transport paths:

- `5002`: primary RS232 bridge transport used for MeshCore packet exchange
- `5001`: direct console port used for local text-console access when available
- `5003`: optional console mirror used as a verifier channel, not as the primary mesh transport

Important detail:

- `5002` remains the authoritative packet path
- `5003` may help verify console reachability or fetch local-node console data
- `5003` is not treated as the main source of mesh packet truth

## Services

The Compose setup starts six processes:

- `init-db`: creates or upgrades the SQLite schema
- `ensure-identity`: creates or loads the local MeshCore identity
- `bridge-gateway`: owns the raw TCP connection to each enabled endpoint
- `neighbours-worker`: ingests adverts, schedules probes, and stores snapshots
- `bot-worker`: listens on configured hashtag channels and replies to selected commands
- `web`: serves the dashboard and the JSON state API

SQLite is file-based and stored in the shared data volume. There is no separate database service.

## Repository layout

- [meshcore_bot/__main__.py](meshcore_bot/__main__.py): CLI entrypoint
- [meshcore_bot/bridge_gateway.py](meshcore_bot/bridge_gateway.py): endpoint connection ownership and gateway IPC
- [meshcore_bot/neighbours_worker.py](meshcore_bot/neighbours_worker.py): advert-driven and scheduled probe orchestration
- [meshcore_bot/probe_service.py](meshcore_bot/probe_service.py): probe execution, local-console probe flow, retries
- [meshcore_bot/database.py](meshcore_bot/database.py): SQLite schema and queries
- [meshcore_bot/web_service.py](meshcore_bot/web_service.py): FastAPI app and dashboard
- [config/config.example.toml](config/config.example.toml): example configuration
- [docker-compose.example.yml](docker-compose.example.yml): example container topology

## Quick start

### 1. Prepare configuration

```bash
cp config/config.example.toml config/config.toml
cp docker-compose.example.yml docker-compose.yml
```

### 2. Adjust the example config

Replace the example hosts, passwords, and endpoint names with your own values.

The example file intentionally uses documentation-only addresses and fake labels.

### 3. Start the stack

```bash
docker compose up -d --build
```

### 4. Check logs

```bash
docker compose logs --tail 100 bridge-gateway neighbours-worker bot-worker web
```

### 5. Open the dashboard

By default the web service listens on port `8080`.

## Configuration guide

The example configuration lives in [config/config.example.toml](config/config.example.toml).

The most important sections are:

- `[service]`: service name and log level
- `[storage]`: SQLite database path
- `[identity]`: local identity key path
- `[probe]`: probe timing, login strategy, retry windows, neighbour paging
- `[bot]`: bot enablement, channels, allowed commands, retry behaviour
- `[web]`: host and port for the dashboard
- `[gateway]`: Unix socket paths and transport watchdog settings
- `[[endpoints]]`: RS232 bridge targets and optional console settings

### Example endpoint block

```toml
[[endpoints]]
name = "RPT_WEST"
raw_host = "192.0.2.10"
raw_port = 5002
console_port = 5001
console_mirror_port = 5003
enabled = true
local_node_name = "RPT_WEST_LOCAL"
```

Notes:

- use RFC 5737 documentation-only addresses such as `192.0.2.0/24`, `198.51.100.0/24`, and `203.0.113.0/24` in examples and documentation
- `local_node_name` is optional; it lets the runtime identify a repeater that is directly exposed on that endpoint
- `console_mirror_port` is optional; when present it can be used as a verifier channel

## CLI overview

The image installs two equivalent commands:

- `meshcore_bot`
- `meshcore-bot`

Unless you pass `--config`, the CLI uses `config/config.toml`.

Most data-oriented commands print JSON so they can be piped into tools such as `jq`.

This CLI is built around subcommands. The command name must come before the command-specific options.

Valid form:

```bash
meshcore_bot rpt-show --config config/config.toml 42
```

Invalid form:

```bash
meshcore_bot --config config/config.toml rpt-show 42
```

Start here when in doubt:

```bash
meshcore_bot --help
meshcore_bot rpt-probe-now --help
meshcore_bot endpoint-update --help
```

### How repeater selection works

Commands that take a repeater selector accept one of these forms:

- numeric repeater ID
- full public key hex
- unique public key prefix
- exact repeater name
- unique name substring

If a selector matches more than one repeater, the command stops and prints the candidates instead of guessing.

### Command groups

The CLI is easier to use if you treat it as four groups.

#### Runtime and service commands

- `init-db`
- `show-config`
- `ensure-identity`
- `run-ingest`
- `run-probe`
- `run-bridge-gateway`
- `run-neighbours-worker`
- `run-bot-worker`
- `run-web`
- `cleanup-probe-jobs`

#### Repeater inspection commands

- `rpt-list`
- `rpt-show`
- `rpt-probe`
- `rpt-probe-now`
- `rpt-login-set`
- `rpt-login-clear`

#### Repeater data maintenance commands

- `rpt-add`
- `rpt-update`
- `rpt-delete`

#### Endpoint configuration commands

- `endpoint-list`
- `endpoint-show`
- `endpoint-add`
- `endpoint-update`
- `endpoint-delete`

## CLI reference

### `show-config`

Prints the resolved runtime configuration with secrets represented as booleans where appropriate.

Example:

```bash
meshcore_bot show-config --config config/config.toml
```

Use this first when the runtime seems to ignore a setting.

### `endpoint-list`

Prints configured endpoints directly from the TOML file.

Example:

```bash
meshcore_bot endpoint-list
```

Useful when you need to verify whether an endpoint is enabled, which raw port it uses, or whether a console mirror is configured.

### `endpoint-show`

Shows repeaters recently seen on one endpoint.

Examples:

```bash
meshcore_bot endpoint-show RPT_WEST
meshcore_bot endpoint-show RPT_WEST --seen-within-hours 6
meshcore_bot endpoint-show RPT_WEST --limit 20
```

Use this when you want to answer: which repeaters were recently visible on this ingress point?

### `endpoint-add`

Adds a new endpoint entry to the config file.

Example:

```bash
meshcore_bot endpoint-add \
  --name RPT_NORTH \
  --raw-host 198.51.100.20 \
  --raw-port 5002 \
  --console-port 5001 \
  --console-mirror-port 5003 \
  --local-node-name RPT_NORTH_LOCAL
```

This command edits the TOML file in place.

### `endpoint-update`

Updates a single endpoint entry.

Examples:

```bash
meshcore_bot endpoint-update RPT_NORTH --raw-host 198.51.100.21
meshcore_bot endpoint-update RPT_NORTH --disabled
meshcore_bot endpoint-update RPT_NORTH --enabled
meshcore_bot endpoint-update RPT_NORTH --clear-console-mirror-port
```

This is the safest way to change endpoint settings without editing the TOML file by hand.

### `endpoint-delete`

Deletes an endpoint from the config file.

Example:

```bash
meshcore_bot endpoint-delete RPT_NORTH --yes
```

The `--yes` flag is required because the command is destructive.

### `rpt-list`

Lists known repeaters stored in the database.

Examples:

```bash
meshcore_bot rpt-list
meshcore_bot rpt-list --query west
meshcore_bot rpt-list --limit 25
```

Use this to discover IDs before using more specific commands.

### `rpt-show`

Shows one repeater with recent adverts, probe jobs, probe runs, and latest neighbour data.

Examples:

```bash
meshcore_bot rpt-show 42
meshcore_bot rpt-show RPT_WEST_LOCAL
meshcore_bot rpt-show ABCDEF12
```

If you are trying to understand why a node looks stale, this is the first inspection command to run.

### `rpt-probe`

Queues a manual probe job and returns job metadata immediately.

Examples:

```bash
meshcore_bot rpt-probe 42
meshcore_bot rpt-probe RPT_WEST_LOCAL --endpoint RPT_WEST
meshcore_bot rpt-probe 42 --reason "manual verification after bridge restart"
meshcore_bot rpt-probe 42 --schedule-after-secs 300
```

Use this when you want the worker to pick the job up asynchronously.

### `rpt-probe-now`

Runs a probe immediately in the current shell and streams progress.

Examples:

```bash
meshcore_bot rpt-probe-now 42
meshcore_bot rpt-probe-now RPT_WEST_LOCAL --endpoint RPT_WEST
meshcore_bot rpt-probe-now 42 --force-path-discovery
meshcore_bot rpt-probe-now 42 --role guest --password "guest-demo"
meshcore_bot rpt-probe-now 42 --verbose
```

This is the most useful diagnostic command in the whole CLI.

Typical progress output looks like this:

```text
Starting probe for RPT 42: RPT_WEST_LOCAL
Endpoint: RPT_WEST
- Login attempt: role=guest route=direct password=empty
- Login succeeded: role=guest permissions=0 capability=2
- Fetching neighbours
  neighbours page: offset=0 results=8 total=8
Probe completed successfully
```

When you are debugging routing or login behaviour, prefer `rpt-probe-now` over `rpt-probe`.

### `rpt-login-set`

Stores a preferred login for one repeater.

Example:

```bash
meshcore_bot rpt-login-set 42 --role guest --password "guest-demo"
```

This is useful when a repeater needs a known override and you do not want to wait for the worker to relearn it.

### `rpt-login-clear`

Clears a stored login override.

Example:

```bash
meshcore_bot rpt-login-clear 42
```

Use this when a remembered credential is wrong or outdated.

### `rpt-add`

Creates a manual repeater row.

Example:

```bash
meshcore_bot rpt-add \
  --pubkey ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789 \
  --name RPT_TEST_PORTABLE \
  --endpoint manual \
  --lat 53.4301 \
  --lon 14.5500
```

This is useful for seeding metadata before the repeater is seen in normal traffic.

### `rpt-update`

Updates manual repeater metadata.

Examples:

```bash
meshcore_bot rpt-update 42 --name RPT_TEST_PORTABLE
meshcore_bot rpt-update 42 --lat 53.4301 --lon 14.5500
```

### `rpt-delete`

Deletes a repeater and all related history.

Example:

```bash
meshcore_bot rpt-delete 42 --yes
```

This command is destructive and removes related historical data.

### `cleanup-probe-jobs`

Deletes old failed probe jobs.

Examples:

```bash
meshcore_bot cleanup-probe-jobs --dry-run
meshcore_bot cleanup-probe-jobs --failed-older-than-hours 24
```

This helps keep the job table readable after repeated operational failures.

## CLI usage patterns

### Inspect first, mutate second

Recommended operator flow:

1. run `rpt-list` or `endpoint-show`
2. inspect details with `rpt-show`
3. run `rpt-probe-now` if you need immediate confirmation
4. use `rpt-login-set`, `endpoint-update`, or `rpt-update` only after you know what is wrong

### Prefer JSON-aware tooling for inspection

Most output is JSON. That makes filtering easier.

Examples:

```bash
meshcore_bot rpt-list --limit 200 | jq '.repeaters[] | {id, name, last_seen_at}'
meshcore_bot rpt-show 42 | jq '.recent_probe_runs'
meshcore_bot endpoint-list | jq '.endpoints[] | {name, raw_host, enabled}'
```

### Running the CLI inside Docker

If the stack is already running in Compose, use `docker compose exec` instead of installing anything locally.

Examples:

```bash
docker compose exec bot-worker meshcore_bot rpt-list --limit 20
docker compose exec bot-worker meshcore_bot rpt-show 42
docker compose exec bot-worker meshcore_bot rpt-probe-now 42
docker compose exec web meshcore_bot show-config
```

## Probe behaviour

Advert-driven probing is intentionally selective.

- stable adverts do not automatically trigger fresh probes
- first-seen repeaters and recent failures are more likely to be reprobed
- path-change noise is rate-limited
- successful logins are remembered and retried first
- learned logins are cleared when they stop working reliably
- automatic collection is capped per repeater in a rolling 24-hour window

This keeps the runtime useful without turning every advert into an aggressive probe cycle.

## Bot behaviour

The bot is deliberately small.

- it listens only on configured hashtag channels
- it responds only to commands listed in `[bot].enabled_commands`
- it treats packet echo as the success signal for replies
- it is intended for operational shortcuts, not as a general chat bot

## Web UI

The dashboard exposes a map, connectivity views, and directional route analysis.

The data model is directional:

- `A -> B` means repeater `A` reported `B` as a neighbour
- the reverse direction is evaluated separately
- route analysis for `A -> B` and `B -> A` is not assumed to be identical

## Safety notes for publishing

If you are adapting this repository for a public deployment or an open-source release, keep these rules in place:

- do not publish real endpoint names
- do not publish private IP addresses or DNS names
- do not publish real repeater labels unless they are already intentionally public
- do not commit live credentials, identity files, or database snapshots
- keep examples on fake data only

The files shipped in this repository should stay safe to publish without leaking operational specifics.

## Development notes

Install development dependencies:

```bash
python -m pip install -e .[dev]
```

Run tests:

```bash
python -m pytest -q
```

Run one targeted test file:

```bash
python -m pytest -q tests/test_repeater_protocol.py
```

## License

Add a repository-specific `LICENSE` file before publishing this project independently.

