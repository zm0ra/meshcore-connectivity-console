# Working Rules

These rules are mandatory for this rebuild.

## 1. Versioning

- Every meaningful change is committed to git.
- No direct editing in the legacy runtime repository.
- No untracked architectural drift.

## 2. Source hierarchy

When sources disagree, use this order:

1. Upstream MeshCore code and verified transport code.
2. Verified behavior from the XIAO TCP bridge implementation.
3. Historical behavior documented in the old bot specification.
4. Old runtime observations and debugging notes.

## 3. Safety constraints

- Treat `meshcore-tcp-bot` as read-only reference unless explicitly told otherwise.
- Separate analysis from implementation.
- Change only one layer at a time: transport, packet codec, runtime, persistence, or UI.
- Do not treat TCP write success as RF delivery success.

## 4. Before risky changes

Record all of the following before making a high-risk change:

- the exact file and function to change,
- the symptom being fixed,
- the minimal success test,
- the likely side effects,
- the rollback method.

## 5. Error discipline

- If a mistake happens, document it.
- New mistakes must be appended to the historical issue log in the legacy repo and reflected in the process used here.