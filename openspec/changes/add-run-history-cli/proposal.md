## Why

`kazeflow run --store` can explicitly retain portable local records, but users
must currently write Python to inspect that history. M10 completes the local
review loop by exposing stored run summaries, records, and meaningful comparisons
through the CLI without adding a daemon or remote service.

## What Changes

- Add `kazeflow runs list`, `kazeflow runs show RUN_ID`, and `kazeflow runs
  compare RUN_A RUN_B` for a project-local SQLite store, with an optional
  caller-selected override.
- Provide deterministic text and one-document JSON output based solely on the
  existing portable store boundary.
- Define missing-store, unknown-run, malformed-record, and comparison error
  diagnostics and exit-status behavior.
- Document local-only storage ownership, lossy records, and the absence of raw
  outputs, exceptions, and partition-key values.

## Capabilities

### New Capabilities

- `run-history-cli`: Provides local CLI inspection and comparison of stored
  SQLite run records.

### Modified Capabilities

- None. The existing SQLite store remains the persistence adapter; this change
  adds an explicit CLI consumer without changing its record schema.

## Impact

- Advances ROADMAP M10 after explicit M9 run persistence.
- Extends the stdlib CLI adapter, tests, documentation, and installed-wheel
  smoke coverage; it may add read-only convenience projections but no database
  migration or mandatory runtime dependency.
- Does not make ordinary `run` commands persist automatically, add remote
  history, reconstruct raw task outputs/exceptions/partition keys, or change
  core execution behavior.
