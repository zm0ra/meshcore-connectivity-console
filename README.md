# meshcore-bot

Versioned rebuild of a MeshCore bot that targets repeaters exposing the XIAO WiFi RS232Bridge TCP wrapper.

The previous implementation lived in a separate repository without usable version history. This repository starts from a clean baseline and every change is tracked from the first commit.

## Purpose

The target system is a bot that connects to one or more MeshCore repeaters over the raw TCP bridge, decodes real MeshCore packets, reacts on selected public channels, and later extends into private datagrams, repeater management, persistence, and a web control surface.

This repository does not claim that all of that already exists. The current codebase is the first runtime foundation for that rebuild.

## Current implementation

Current repository state:

- containerized Python runtime for the new bot process,
- TOML bootstrap configuration loaded at startup,
- single-process async application skeleton,
- SQLite database initialized on startup,
- bootstrap persistence for channels, endpoints, management targets, and runtime settings,
- database tables prepared for radio packets, messages, commands, node adverts, and management snapshots,
- built-in HTTP server with `GET /healthz` and `GET /` status endpoints,
- bind-mount friendly Docker Compose setup for `config`, `data`, and `logs`.

What is not implemented yet:

- RS232Bridge transport client,
- MeshCore packet codec,
- command handling,
- admin UI,
- repeater management sessions.

## Process model

The rebuild starts with one process, not multiple workers.

That is the safer default for this project because transport, packet decoding, runtime state, and HTTP status reporting all depend on the same in-memory state and tight ordering. Splitting them too early would add queues, delivery semantics, and failure modes before the protocol layer is stable.

Recommended approach:

- keep one main process until raw transport, packet parsing, and runtime state are stable,
- add separate workers only when there is a proven boundary with clear contracts,
- the first realistic split, if needed later, is an optional web/API process and separate background jobs for slow management polling.

At this stage, separate workers are not recommended.

## Configuration

Tracked bootstrap configuration lives in `config/config.toml`.

The current config defines:

- bot identity at the application level,
- HTTP bind host and port,
- service name,
- log level,
- storage paths,
- initial channel list,
- future endpoint and management target sections.

The file is safe to version because it contains no credentials and no production endpoint details.

## Docker

Build and run the current environment:

```bash
docker compose up --build
```

The compose setup exposes port `8080` and mounts:

- `./config` to `/app/config`,
- `./data` to `/app/data`,
- `./logs` to `/app/logs`.

Once the stack is up, the container serves:

- `http://127.0.0.1:8080/healthz`
- `http://127.0.0.1:8080/`

The startup path creates the SQLite database automatically and seeds configuration snapshots into it.

## Historical references

The old implementation is kept outside this repository as reference material only. It is used to recover protocol behavior and scope, not as an active runtime base.

Primary references:

- `../meshcore-tcp-bot/README.md`
- `../meshcore-tcp-bot/MESHCORE_TCP_BOT_SPECIFICATION.md`
- `../meshcore-tcp-bot/AI_ISSUES.md`
- `../_clones/MeshCore-upstream`
- `../_clones/meshcore-xiao-wifi-serial2tcp-upstream`
- `../_clones/meshcore-bot-agessaman`

## Repository layout

```text
meshcore-bot/
	config/
		config.toml
	meshcore_bot/
		__init__.py
		__main__.py
		app.py
		config.py
		database.py
	.gitignore
	Dockerfile
	docker-compose.yml
	pyproject.toml
	README.md
```