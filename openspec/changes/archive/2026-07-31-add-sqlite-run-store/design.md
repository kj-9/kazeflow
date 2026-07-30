## Context

The core already returns immutable `RunResult` values and defines deliberately
lossy JSON-compatible `to_record()` projections.  M6 uses that stable projection
to offer optional local history through Python's standard-library `sqlite3`, without
changing execution, making persistence automatic, or requiring a third-party extra.

## Goals / Non-Goals

**Goals:**

- Provide an explicit local adapter for saving, loading, and listing versioned
  RunResult-derived records.
- Preserve the portable record's flow/task/attempt ordering, terminal statuses,
  timestamps, durations, skip/blocking information, and serializable failure
  metadata for success, failure, cancellation, and partitioned runs.
- Define SQLite schema initialization, schema-version compatibility, and a safe
  forward migration policy.
- Keep the adapter absent from the core import graph and make database creation a
  caller-selected side effect.

**Non-Goals:**

- Persisting raw Python outputs, raw exception objects, or raw partition-key values.
- Reconstructing a full `RunResult` (which intentionally may contain in-memory-only
  values), providing a scheduler, daemon, remote database, multi-user service,
  automatic run capture, or an execution API that writes to a database.
- Adding a package dependency or changing the core `run()`/`Flow.run_async()` API.

## Decisions

### A separate SQLite adapter owns all database effects

`kazeflow.sqlite_store` will expose `SQLiteRunStore`, constructed with an explicit
database path.  It will use only `sqlite3`, `json`, and other standard-library
modules.  Constructing/opening the store is the only API that may initialize a
database file; core imports, planning, and execution never import the adapter or
create files.  The adapter is not a root `kazeflow` export, so `import kazeflow`
remains free of SQLite storage behavior.

The public methods are `save(result: RunResult)`, `load(run_id: str)`, and
`list_runs(...)`.  `save` accepts a completed RunResult and stores its derived
portable record; `load` returns a persistent record value rather than pretending to
reconstruct a RunResult; `list_runs` returns stable summaries suitable for local
history browsing.  A missing run id raises `KeyError`; attempting to save a run id
already present raises `ValueError` rather than silently overwriting history.

An executor-integrated run store was rejected because it would make persistence
implicit and couple core execution to a database lifecycle.

### Persist one versioned canonical JSON record per run

Schema version 1 stores one row per `run_id`, with a schema version, saved timestamp,
terminal run status, and canonical JSON encoding of `RunResult.to_record()`.  A
loaded record exposes `schema_version`, saved metadata, and a newly decoded portable
record.  Task and attempt arrays are stored and returned in their existing order.

`PRAGMA user_version` identifies the database schema.  Opening a new empty database
initializes version 1 transactionally.  Opening the current version succeeds;
opening a future version fails before reads or writes.  Future code must provide
ordered, transactional forward migrations from each supported older version and
must never silently downgrade or discard records.  A failed migration rolls back
and leaves the prior version intact.

Keeping normalized task/attempt tables was considered, but a canonical record blob
better matches the versioned portable projection and avoids inventing a second,
partially divergent result schema in this milestone.

### The persistence boundary is deliberately lossy

The adapter serializes exactly the portable record projection.  It excludes arbitrary
`output`, raw `BaseException` objects, and raw partition keys because none has a
stable or safe JSON serialization contract.  Serializable `FailureInfo` metadata is
retained.  For every attempt, `partition.present` is retained: `0`, `""`, and
`False` are all represented as present partitions even though their raw identities
are intentionally not recoverable; absent partitions remain distinct with
`present: false`.  Any non-serializable raw output, exception, or key therefore
cannot prevent save or load.

### Parallel ownership avoids core and schema conflicts

The persistence owner exclusively owns `src/kazeflow/sqlite_store.py` and dedicated
store tests.  The migration/test owner owns compatibility fixtures and migration
tests only.  The documentation owner owns adapter documentation/examples only.
No M6 worker edits `flow.py`, `assets.py`, `results.py`, `__init__.py`,
`pyproject.toml`, `uv.lock`, or CI.  The persistence owner must stabilize the public
adapter interface before migration fixtures or documentation are finalized; root
integration, OpenSpec sync, and archive are serial.

## Risks / Trade-offs

- [A database created by import surprises core users] → isolate all `sqlite3` use in
  the explicitly imported and constructed adapter.
- [Raw values are assumed durable] → return a named persistent record, not a
  RunResult, and document the exact lossy exclusions.
- [A newer database is corrupted by older code] → reject future schema versions
  before database mutation.
- [Migration fails halfway] → wrap each migration and version bump in one SQLite
  transaction and test rollback.
- [Concurrent processes contend for a local file] → rely on SQLite's normal locking;
  M6 makes no distributed writer or service guarantee.

## Migration Plan

1. Create a new version-1 database only when a caller constructs an adapter for a
   chosen path.
2. Store each completed result through explicit `save` calls; existing flows need no
   code change and produce no database until a caller opts in.
3. For future schema versions, ship and test an ordered transactional migration from
   every supported older schema before allowing that version to read/write the file.
4. If migration is not supported or the database is newer than this library, fail
   clearly and leave the database unchanged; users can retain the file and use a
   compatible library version.

## Open Questions

None.  M6 intentionally uses record-level load APIs; reconstructing typed RunResult
objects from lossy persisted data is deferred unless a future requirement defines a
separate durable-result type.
