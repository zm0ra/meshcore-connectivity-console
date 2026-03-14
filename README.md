# meshcore-bot

Focused rebuild of the MeshCore TCP bot with the first goal of harvesting repeater data over RS232Bridge TCP on port 5002.

Current scope:
- RS232Bridge framing helpers
- MeshCore packet parsing primitives
- SQLite schema for repeater adverts and guest probe runs
- CLI bootstrap for initializing storage

Reference notes live in ../trunk/ and drive the protocol implementation.
