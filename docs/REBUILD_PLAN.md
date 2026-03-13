# Rebuild Plan

## Phase 0: Foundation

- establish a clean repository baseline,
- define working rules,
- lock the legacy repo into reference-only mode,
- document trusted sources.

## Phase 1: Transport and protocol baseline

- reimplement RS232Bridge framing,
- add incremental frame decoding,
- add packet summary decoding for inspection,
- verify behavior against upstream and the XIAO bridge client.

## Phase 2: Public channel messaging

- implement public channel key derivation,
- implement `GRP_TXT` decode and encode,
- verify send and receive behavior against known captures.

## Phase 3: Runtime skeleton

- define endpoint session model,
- define message model,
- define service orchestration boundaries,
- keep persistence and web layers out until runtime boundaries are stable.

## Phase 4: Persistence and management

- add SQLite with explicit source provenance fields,
- add management state only after protocol paths are testable,
- keep console-derived and management-derived data separate.

## Phase 5: UI and operator surfaces

- add HTTP API,
- add UI only on top of already-defined runtime models,
- mark uncertainty and data origin explicitly.