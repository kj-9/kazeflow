# SQLite run store

## Purpose

Define the explicit, local SQLite adapter that persists Kazeflow's portable
`RunResult` records without adding persistence to the core execution path.

## Requirements

### Requirement: SQLite persistence is explicit and adapter-scoped

The project SHALL expose `SQLiteRunStore` from `kazeflow.sqlite_store` using only
Python standard-library modules. A caller SHALL explicitly construct it with a
local database path before it creates, opens, migrates, or writes a SQLite database.
Core `import kazeflow`, flow planning, and `run()`/`Flow.run_async()` SHALL NOT
import the adapter, create a database, or persist a result automatically. The
adapter SHALL NOT be a root `kazeflow` export.

#### Scenario: A core-only caller never opts into storage

- **WHEN** a caller imports kazeflow, plans a flow, and runs it without importing or
  constructing `SQLiteRunStore`
- **THEN** no SQLite database is created and core public behavior is unchanged

#### Scenario: A caller opts into a local database

- **WHEN** a caller imports `SQLiteRunStore` and constructs it for a new local path
- **THEN** the adapter initializes that database and can accept an explicit save
  without changing the result returned by core execution

### Requirement: Store saves, loads, and lists portable run records

`SQLiteRunStore.save(result)` SHALL accept a completed `RunResult` and create one
immutable persistent record identified by its non-empty `run_id`. `load(run_id)`
SHALL return the saved persistent record, including its schema version and a newly
decoded portable record, and `list_runs()` SHALL return deterministic summaries of
stored records without executing flows. Saving an existing run id SHALL raise
`ValueError`; loading an unknown run id SHALL raise `KeyError`.

#### Scenario: A successful run is saved and loaded

- **WHEN** a caller explicitly saves a successful RunResult and loads its run id
- **THEN** the loaded record preserves its run id, terminal status, timestamps,
  duration, task/attempt order, and successful attempt status

#### Scenario: A caller lists local history

- **WHEN** a store contains multiple explicitly saved runs
- **THEN** `list_runs()` returns deterministic summaries containing each run id,
  terminal status, saved metadata, and schema version without loading raw outputs

#### Scenario: A duplicate or missing record is requested

- **WHEN** a caller saves a run id already present or loads an unknown run id
- **THEN** save raises `ValueError` for the duplicate and load raises `KeyError` for
  the missing id without overwriting or creating another record

### Requirement: Stored records preserve portable failure and partition semantics

The adapter SHALL persist the canonical portable projection of a RunResult and SHALL
preserve success, failure, cancellation, skipped, and partitioned task/attempt
statuses; UTC timestamps; durations; skip reasons; blockers; and serializable
failure metadata. It SHALL preserve task and attempt array order. Raw `output`, raw
exception objects, and raw partition-key values SHALL NOT be stored or returned.
Each attempt's partition-presence flag SHALL round-trip: `0`, `""`, and `False` are
all present partitions, while an unpartitioned attempt is absent; raw present-key
identity is intentionally not recoverable. Non-serializable raw values SHALL NOT
prevent persistence.

#### Scenario: A failed partitioned run round-trips

- **WHEN** a partitioned run includes successful, failed, and dependency-blocked
  attempts and is explicitly saved then loaded
- **THEN** the loaded portable record retains selected attempt order, present versus
  absent partition markers, statuses, blockers, and failure type/message/traceback
  while omitting raw output, exception, and partition-key values

#### Scenario: Falsey and non-serializable values cross the boundary

- **WHEN** a run contains partition keys `0`, `""`, or `False` and raw outputs,
  exception objects, or keys that cannot be JSON encoded
- **THEN** save and load succeed, each present partition remains marked present, and
  none of those raw values appears in the loaded portable record

### Requirement: SQLite schema versions are checked and migrated safely

Every store database SHALL expose a positive integer schema version. A newly created
database SHALL initialize schema version 1 transactionally. The adapter SHALL open
the current supported version, reject a newer unsupported version before mutation,
and apply only explicitly supported ordered forward migrations from older versions.
Each migration and version update SHALL be transactional; a migration failure SHALL
roll back and preserve the prior schema version and records. The adapter SHALL NOT
silently downgrade, discard, or reinterpret an unsupported schema.

#### Scenario: A new database is initialized

- **WHEN** a caller constructs a store at a new path
- **THEN** it has schema version 1 and can save/load a portable run record

#### Scenario: A future database is opened by older code

- **WHEN** a database reports a schema version newer than the adapter supports
- **THEN** construction fails before any schema or record mutation

#### Scenario: A migration fails

- **WHEN** an explicitly supported forward migration encounters an error
- **THEN** the transaction rolls back, the database retains its prior schema version
  and records, and the error is reported to the caller
