# meshcore-bot

Greenfield rebuild of the MeshCore TCP bot.

This repository starts from zero after the previous implementation in `meshcore-tcp-bot` became unsafe to evolve due to missing version history.

## Working mode

- `meshcore-tcp-bot` is treated as a read-only historical reference.
- New implementation work happens only in this repository.
- Every change must be versioned in git.
- High-risk runtime experiments are not done here without a clear test and rollback plan.

## Reference sources

Primary historical references live outside this repo:

- `../meshcore-tcp-bot/MESHCORE_TCP_BOT_SPECIFICATION.md`
- `../meshcore-tcp-bot/AI_ISSUES.md`
- `../_clones/MeshCore-upstream`
- `../_clones/meshcore-xiao-wifi-serial2tcp-upstream`
- `../_clones/meshcore-bot-agessaman`

## Current goal

Build a new implementation from scratch with:

- explicit protocol boundaries,
- strict separation of transport, packet logic, runtime, persistence, and UI,
- reproducible commits from the first step onward.