# meshcore-bot

Focused rebuild of the MeshCore TCP bot with the first goal of harvesting repeater data over RS232Bridge TCP on port 5002.

Current scope:
- RS232Bridge framing helpers
- MeshCore packet parsing primitives
- Bridge gateway process owning the single TCP connection per endpoint
- Neighbours worker process handling advert ingestion and repeater harvesting over gateway IPC
- SQLite schema for repeater adverts and guest probe runs
- CLI bootstrap for initializing storage

Reference notes live in ../trunk/ and drive the protocol implementation.

Runtime split in Docker:
- `bridge-gateway`: only process allowed to connect to RS232Bridge
- `neighbours-worker`: advert ingestion plus repeater neighbour harvesting
- `web`: status UI over SQLite

SQLite stays file-based in the shared volume. It is not extracted into a separate process because that would add coordination complexity without improving correctness for the current single-host deployment.
