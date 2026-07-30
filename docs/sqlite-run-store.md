# SQLite run store

`SQLiteRunStore` is an explicit, local persistence adapter for portable execution
records. It is not part of the core import path: importing `kazeflow`, planning a
flow, or running one never creates a database. A database is created or initialized
only when you import and construct the store for a path you choose.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from kazeflow import Flow, asset, run
from kazeflow.sqlite_store import SQLiteRunStore


@asset
def report() -> str:
    return "complete"


result = run(["report"])

with TemporaryDirectory() as directory:
    path = Path(directory) / "runs.sqlite3"

    # Construction explicitly creates/initializes this new database at schema v1.
    with SQLiteRunStore(path) as store:
        saved = store.save(result)
        loaded = store.load(result.run_id)
        history = store.list_runs()

        assert saved.run_id == result.run_id
        assert loaded.record == result.to_record()
        assert history[0].status == "success"
```

`save()` accepts a completed `RunResult` and returns a frozen `StoredRunRecord`.
`load()` returns the same persistent-record type; it does not reconstruct a
`RunResult`, because the persisted format intentionally omits in-memory-only
values. `StoredRunRecord.record` returns a newly decoded portable dictionary each
time. `list_runs()` returns frozen `StoredRunSummary` values in saved-time then
run-id order; pass `limit=` to restrict that list.

Saving a run id already in the database raises `ValueError`. Loading an unknown run
id raises `KeyError`. Use the context manager, or call `close()`, when you are done
with the caller-owned connection.

## Stored boundary

The store writes canonical JSON from `RunResult.to_record()`. It retains run/task/
attempt order, terminal statuses, UTC timing fields, durations, skip reasons,
dependency blockers, and serializable failure metadata. This includes successful,
failed, cancelled, skipped, and partitioned outcomes.

The following values are deliberately never stored or returned from the portable
record:

- raw task outputs;
- raw exception objects; and
- raw partition-key values.

For partitioned attempts, the record keeps only `partition.present`. Keys such as
`0`, `""`, and `False` are therefore correctly retained as present partitions, but
their original values are not recoverable. An unpartitioned attempt remains distinct
with `partition.present: false`. Non-serializable outputs, exceptions, and keys do
not prevent the record from being saved.

## Schema compatibility

New or empty databases initialize transactionally at schema version 1. A current
database opens normally. A database from a newer adapter version is rejected before
the store reads or writes records. Future releases can add only ordered, transactional
forward migrations; a failed migration rolls back its schema and record changes, and
the store never silently downgrades or discards data.

An existing non-empty SQLite file with `PRAGMA user_version = 0` is rejected as an
unversioned database rather than being guessed or overwritten. A malformed or
non-SQLite file raises the underlying `sqlite3.DatabaseError`. Keep such files and
open them with a compatible tool or library version.

## Local-only limitations

This adapter does not automatically persist runs, provide a remote database, start a
daemon, or create a security sandbox. It uses SQLite's normal local-file locking;
simultaneous writers can contend and may receive SQLite locking errors. A store owns
one default SQLite connection and is not a thread-sharing abstraction—construct and
use a separate store in each thread when needed. The caller remains responsible for
choosing the database path, protecting its filesystem permissions, and deciding when
to save a run.
