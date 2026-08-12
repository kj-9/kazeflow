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
durations, skip reasons, blockers, and portable failure metadata. It deliberately
omits raw task outputs, exception objects, and the dedicated raw partition-key field.

This is not a confidentiality or redaction guarantee. Exception messages and
tracebacks are stored as portable failure metadata and can contain partition keys,
paths, queries, or other application-controlled values. Protect the database as
diagnostic data and review the
[portable-record trust boundary](../concepts/trust-boundary.md#portable-record-boundary).

See [Results and history](../results.md) for the record model and
[SQLiteRunStore](../api/sqlite.md) for the direct Python API.
