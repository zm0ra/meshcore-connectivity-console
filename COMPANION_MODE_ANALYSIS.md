# Companion Mode Analysis

This document captures the verified upstream MeshCore behavior needed to turn this project from a public-channel bot into a full companion-style node that can:

- own a real MeshCore identity
- receive and send private messages
- log into repeaters as guest or admin
- send encrypted management requests such as status, telemetry, owner info, ACL, and neighbors

The analysis below is based on upstream MeshCore source cloned outside this repo and on the reference `agessaman/meshcore-bot` project.

## Bottom line

The current bot already has the raw TCP transport layer.

What is still missing for full companion mode is the MeshCore peer layer:

- persistent local Ed25519 identity
- ECDH shared-secret derivation against repeater and peer public keys
- encrypted private datagrams: `TXT_MSG`, `REQ`, `RESPONSE`, `ANON_REQ`, `PATH`
- contact database keyed by public key
- pending request tracking and replay-safe timestamp handling
- repeater login state and path management

The reference `meshcore-bot` is not a protocol implementation. It uses `meshcore-cli` and `meshcore.py` as its transport/control plane, so it is useful mainly for product behavior, command design, persistence ideas, and web-viewer ideas.

## Identity and keys

Verified upstream behavior:

- MeshCore identity is an Ed25519 keypair.
- Routing hashes are not a separate hash function output. The node hash is just the prefix of the public key.
- Shared secrets are derived directly from Ed25519 keys using `ed25519_key_exchange(...)`.

Relevant upstream files:

- `src/Identity.h`
- `src/Identity.cpp`
- `src/helpers/IdentityStore.cpp`

Important details:

- `LocalIdentity(RNG* rng)` generates a random 32-byte seed and calls `ed25519_create_keypair(pub_key, prv_key, seed)`.
- `calcSharedSecret(secret, other_pub_key)` calls `ed25519_key_exchange(secret, other_pub_key, prv_key)`.
- `Identity::copyHashTo(...)` copies the first `PATH_HASH_SIZE` bytes of `pub_key`.
- File persistence through `IdentityStore::save()` writes `pub_key` followed by `prv_key` when saving to `.id` files.

Implication for this project:

- We need a long-lived bot identity file, not an ephemeral session key.
- Our bot must treat the repeater and any DM peer as full identities, not just channel names or hash prefixes.
- Our contact store should cache `shared_secret` per contact exactly like upstream does.

## Private messages

Verified upstream behavior:

- Private messages use `PAYLOAD_TYPE_TXT_MSG`.
- Packet envelope is:
  - destination hash prefix
  - source hash prefix
  - MAC + encrypted payload
- The encrypted payload plaintext is:
  - `timestamp` as 4 bytes
  - `txt_type + attempt` as 1 byte
  - message bytes

Relevant upstream files:

- `src/Mesh.cpp`
- `src/Mesh.cpp:createDatagram(...)`
- `src/helpers/BaseChatMesh.cpp:sendMessage(...)`

Receive path:

- On incoming `REQ`, `RESPONSE`, `TXT_MSG`, or `PATH`, MeshCore checks whether `dest_hash` matches `self_id`.
- It then searches contacts by `src_hash` prefix.
- For every matching contact, it derives or reuses the ECDH shared secret and tries `MACThenDecrypt(...)`.
- First successful decrypt wins.

Implication for this project:

- Receiving DMs is impossible without a real local identity and a contact table containing the sender public key.
- We need exact handling for `TXT_TYPE_PLAIN`, `TXT_TYPE_CLI_DATA`, and retries.
- Flood duplicates and multi-path copies still apply, so DM dedupe must also be logical, not raw-packet-only.

## Repeater login

Verified upstream behavior:

- Repeater login starts with `PAYLOAD_TYPE_ANON_REQ`.
- This packet is not anonymous in the human sense. It includes the sender public key in cleartext.
- Its structure is:
  - destination hash prefix
  - sender public key, 32 bytes
  - MAC + encrypted payload

For repeater login, encrypted payload is:

- `timestamp` as 4 bytes
- `password` as remaining bytes

Relevant upstream files:

- `src/Mesh.cpp:onRecvPacket()`
- `src/Mesh.cpp:createAnonDatagram(...)`
- `src/helpers/BaseChatMesh.cpp:sendLogin(...)`
- `examples/simple_repeater/MyMesh.cpp:handleLoginReq(...)`

Repeater-side behavior:

- Repeater computes shared secret from sender public key and its own private key.
- Password is checked against admin or guest password.
- On success, sender is inserted into repeater ACL/contact table.
- Repeater stores the shared secret for that client.
- If login arrived via flood, repeater may force path rediscovery.

Login success response payload in current repeater example:

- bytes `0..3`: repeater current timestamp
- byte `4`: `RESP_SERVER_LOGIN_OK`, currently `0`
- byte `5`: legacy keepalive interval field
- byte `6`: legacy admin flag
- byte `7`: permissions byte
- bytes `8..11`: random uniqueness blob
- byte `12`: firmware version level

Important nuance:

- Login response is not matched by a reflected request tag the same way normal `REQ` responses are.
- Upstream companion code matches login responses via pending target contact, not via a generic response tag.

Implication for this project:

- We need a login state machine separate from generic request/response tracking.
- Each configured management repeater needs full public key knowledge, password selection, permission tracking, and login freshness.

## Normal management requests

After login, repeater management moves to `PAYLOAD_TYPE_REQ` and `PAYLOAD_TYPE_RESPONSE`.

Verified upstream behavior:

- `REQ` and `RESPONSE` both use `createDatagram(...)`.
- Plaintext starts with a 4-byte tag or timestamp.
- For generic requests, upstream uses current unique RTC timestamp as the tag.
- For most normal responses, repeater reflects the sender timestamp back in the first 4 bytes.

Relevant upstream files:

- `src/helpers/BaseChatMesh.cpp:sendRequest(...)`
- `examples/simple_repeater/MyMesh.cpp:handleRequest(...)`
- `examples/companion_radio/MyMesh.cpp:onContactResponse(...)`

Verified request types in the repeater example:

- `0x01` `REQ_TYPE_GET_STATUS`
- `0x02` `REQ_TYPE_KEEP_ALIVE`
- `0x03` `REQ_TYPE_GET_TELEMETRY_DATA`
- `0x05` `REQ_TYPE_GET_ACCESS_LIST`
- `0x06` `REQ_TYPE_GET_NEIGHBOURS`
- `0x07` `REQ_TYPE_GET_OWNER_INFO`

Access rules in the repeater example:

- status is allowed for guest and admin
- telemetry is allowed, with permission masking for guest
- access list requires admin
- neighbors is available
- owner info is available

Implication for this project:

- Once login works, repeater management is a standard encrypted direct-message protocol.
- We need pending tag tables and response dispatch by tag.
- We also need periodic keepalive for active sessions if we want stable long-lived admin/guest connections.

## Neighbors request and response

Verified from `examples/simple_repeater/MyMesh.cpp`.

Neighbors request payload layout:

- byte `0`: request type `0x06`
- byte `1`: request version, current example expects `0`
- byte `2`: requested result count
- bytes `3..4`: offset as little-endian `uint16`
- byte `5`: sort order
- byte `6`: requested pubkey prefix length
- bytes `7..10`: random uniqueness blob

Sort order values:

- `0`: newest to oldest
- `1`: oldest to newest
- `2`: strongest to weakest
- `3`: weakest to strongest

Neighbors response payload layout:

- bytes `0..3`: reflected request tag
- bytes `4..5`: total neighbor count
- bytes `6..7`: returned result count
- then repeated entries:
  - pubkey prefix of requested length
  - `heard_seconds_ago` as little-endian `uint32`
  - `snr` as signed `int8`, actually `SNR * 4`

Implication for this project:

- The current `!neighbors` command should eventually switch from advert-only heuristics to live `REQ_TYPE_GET_NEIGHBOURS` polling.
- Our schema already has the right direction; it just needs the active RF management layer.

## Anonymous direct helper requests

Repeaters also support anonymous direct requests through `ANON_REQ` subtypes for bootstrap discovery.

Verified subtype values in repeater example:

- `0x01` regions
- `0x02` owner info
- `0x03` basic clock/status

These only work on direct routes in the example implementation.

Each request carries:

- 4-byte sender timestamp
- 1-byte subtype
- 1-byte encoded reply path len/hash size
- variable reply path bytes

Implication for this project:

- These are useful for low-cost bootstrap before full session establishment.
- They are not a substitute for the normal guest/admin login flow.

## Path handling

Verified upstream behavior:

- Private packets can be sent either flood or direct.
- If a flood request reaches the repeater, repeater can answer with `PATH` data so the sender learns a direct return route.
- Upstream companion logic upgrades to direct send once `out_path` is known.

Implication for this project:

- A real companion implementation should maintain per-contact `out_path`, `out_path_len`, and last path update time.
- Raw TCP gives us the same packet bytes as radio, so this logic can live in our Python service without BLE or serial.

## What the reference bot actually gives us

Verified from `meshcore-bot-reference`:

- It explicitly describes itself as using `meshcore-cli` and `meshcore.py`.
- Direct-message sending in `modules/command_manager.py` delegates to that external stack.
- Repeater operations are product logic and database logic on top of an already working companion connection.

Useful takeaways from the reference bot:

- DM-only management UX is a good pattern
- repeater inventory and web viewer ideas are useful
- admin ACL handling and persistence ideas are useful
- graph/map presentation ideas are useful

Not useful as a raw-TCP protocol basis:

- it does not show how to build MeshCore encrypted packets from scratch over TCP
- it does not replace upstream firmware code as the protocol source of truth

## Recommended implementation order

### Phase 1: local identity

- add persistent bot identity file
- support loading existing private key and deriving public key
- expose bot public key in API/viewer

### Phase 2: private contact crypto

- add `LocalIdentity` equivalent in Python
- add Ed25519 key derivation and ECDH shared-secret derivation compatible with upstream
- add direct packet build/decode for `TXT_MSG`, `REQ`, `RESPONSE`, `ANON_REQ`, `PATH`

### Phase 3: private messages

- ingest full adverts into contact store
- support DM receive and DM send to known contacts
- add replay protection and pending ack tracking

### Phase 4: repeater sessions

- add management login per configured repeater as guest or admin
- track session permissions and connection freshness
- support `GET_STATUS`, `GET_OWNER_INFO`, `GET_TELEMETRY_DATA`, `GET_NEIGHBOURS`, and optionally `GET_ACCESS_LIST`

### Phase 5: topology and map

- replace advert-only neighbor view with live neighbor snapshots
- correlate paths, neighbor edges, SNR, and recency
- render colored links and richer repeater state in the viewer

## Practical conclusion for this repo

The raw TCP bridge layer is already sufficient.

To reach "aka companion" mode, the next code changes should focus on:

- Python identity storage and key derivation compatible with upstream MeshCore
- packet codec support for encrypted peer-to-peer datagrams
- repeater login and request state machines
- live neighbor polling over `REQ_TYPE_GET_NEIGHBOURS`

Once those are in place, the rest of the product work becomes incremental: better commands, admin tooling, richer viewer, and map overlays.