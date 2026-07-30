## Why

`RunResult` makes one execution inspectable in memory, but users who explicitly
want to compare or revisit local runs need a small durable record without turning
the core into an always-on workflow platform.  M6 adds that capability as an
opt-in SQLite adapter after the result schema has stabilized.

## What Changes

- Add a standard-library `sqlite3` adapter that a caller explicitly constructs with
  a database path and calls to save, load, and list persistent run records.
- Define a versioned SQLite schema, compatibility checks, and a forward migration
  policy for stored records.
- Persist the stable, lossy `RunResult` record projection for successful, failed,
  cancelled, and partitioned runs, preserving task/attempt order and status/failure
  information.
- Specify that raw outputs, raw exception objects, and raw partition keys are never
  serialized; partition presence and falsey-key identity remain distinguishable in
  the stored portable record.
- Add adapter-only tests and documentation while preserving core-only imports and
  execution behavior when no store is constructed.

## Capabilities

### New Capabilities

- `sqlite-run-store`: Explicit local SQLite storage and retrieval of versioned,
  RunResult-derived portable run records.

### Modified Capabilities

- None.

## Impact

This M6 change advances the roadmap's optional SQLite persistence milestone.  It
adds an adapter module and dedicated tests/docs, using only Python's `sqlite3`; it
does not add a mandatory dependency, modify `run()` or `Flow.run_async()`, or cause
the core to create a database.  The existing core API and behavior remain compatible
on Python 3.10 through 3.13 when the adapter is absent, and this change adds no
daemon, scheduler, remote database, worker, or control plane.
