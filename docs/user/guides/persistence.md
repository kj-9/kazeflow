# Persist run history explicitly

Core execution never creates a database. Add `--store` when one completed run should
be retained in a caller-owned SQLite file:

```console
mkdir -p .kazeflow
kazeflow run daily.py --yes --store .kazeflow/runs.sqlite3
```

The store is initialized after a terminal result exists and before the final CLI
result is emitted. Without `--store`, no database is opened.

## Inspect the store

```console
kazeflow runs list
kazeflow runs show RUN_ID
kazeflow runs compare LEFT_RUN_ID RIGHT_RUN_ID
```

History commands read `./.kazeflow/runs.sqlite3` by default. They never create a
missing history database. Use `--store PATH` to read another existing file.

## Know the portable boundary

The SQLite adapter stores run/task/attempt order, terminal statuses, UTC timestamps,
durations, skip reasons, blockers, portable failure metadata, and record v4
successful resolution and materialization IDs. It deliberately omits raw task
outputs, exception objects, and the dedicated raw partition-key field. Database
schema v1 is unchanged; each row records its portable record version, and new saves
use record v4.

This is not a confidentiality or redaction guarantee. Exception messages and
tracebacks are stored as portable failure metadata and can contain partition keys,
paths, queries, or other application-controlled values. Protect the database as
diagnostic data and review the
[portable-record trust boundary](../concepts/trust-boundary.md#portable-record-boundary).
Materialization IDs are opaque adapter-defined strings, may contain application
information, and are not automatically redacted. Reading history displays the saved
ID without contacting the adapter or loading the artifact.

See [Results and history](../results.md) for the record model and
[SQLiteRunStore](../api/sqlite.md) for the direct Python API.

SQLite run history and file materialization storage are separate. Saving or deleting
a run record does not commit, invalidate, or remove an asset output. If an application
uses `FileMaterialization`, its output generations and revocation ledger live under
the explicitly configured materialization root; see
[partition materialization and invalidation](../partitions.md#invalidate-one-materialization-explicitly).
Treat both the SQLite database and the materialization root as application data.
