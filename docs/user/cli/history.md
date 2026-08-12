# `kazeflow runs`

Read portable records from an existing local SQLite store. History commands never
create, initialize, migrate, or write the database.

## List

```console
kazeflow runs list [--store PATH] [--limit N] [--format text|json]
```

Records are ordered by saved time and run ID. The default read path is
`./.kazeflow/runs.sqlite3`.

## Show

```console
kazeflow runs show RUN_ID [--store PATH] [--format text|json]
```

`show` returns the stored portable envelope. Unknown IDs are selection errors.

## Compare

```console
kazeflow runs compare LEFT_RUN_ID RIGHT_RUN_ID \
  [--store PATH] [--format text|json]
```

Comparison preserves left/right argument order and compares run and task aggregates.
It does not match dedicated raw partition-key fields because those fields are not
stored. Failure messages and tracebacks remain application-controlled and can repeat
the same values; stored records are not automatically redacted. See the
[portable-record trust boundary](../concepts/trust-boundary.md#portable-record-boundary).

```console
$ kazeflow runs list
Stored runs:
- <run-id> (success; saved_at: <timestamp>; schema: 1)

$ kazeflow runs show <run-id>
Stored run:
- run_id: <run-id>
- status: success
- saved_at: <timestamp>
Portable record:
{ ... }
```
