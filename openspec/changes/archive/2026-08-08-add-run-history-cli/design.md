## Context

M9 can save a terminal portable `RunResult` to a caller-selected SQLite file.
M10 makes those records inspectable from the existing stdlib CLI, defaulting to
the project-local `./.kazeflow/runs.sqlite3` file without adding history
behavior to core execution or changing the SQLite schema.

## Goals / Non-Goals

**Goals:**

- Add `runs list`, `runs show`, and `runs compare` commands with a predictable
  project-local store default.
- Preserve the SQLite adapter's ordering, schema checks, and deliberately lossy
  portable-record boundary.
- Keep JSON one-document output and use existing CLI error classes.

**Non-Goals:**

- Create a missing database from a history command, migrate schemas, add a
  remote store, or make ordinary `run` commands save automatically.
- Rebuild a `RunResult`, reveal raw output/exception objects/partition keys, or
  identify individual partitions whose keys were intentionally omitted.

## Decisions

### Store path has a project-local default and must already exist

Every history command resolves its store path to `--store PATH` when supplied,
otherwise to `./.kazeflow/runs.sqlite3` relative to the invocation's current
working directory. The CLI checks that the resolved path is a regular existing
file before constructing `SQLiteRunStore`, since construction would otherwise
create a database. Store opening, schema, malformed data, and read failures are
infrastructure failures (exit 4). This default affects history reads only;
ordinary `run` remains non-persistent unless its existing explicit store option
is supplied.

### Existing adapter queries are the source of truth

`list` uses `list_runs(limit=...)` in its saved-time/run-id order. `show` uses
`load(run_id)` and displays its stored portable record envelope. `compare` loads
both records and compares only portable fields, retaining the caller's left/right
order. Unknown IDs and invalid limits are selection/configuration failures (2).

### Comparison is aggregate where identity is not stored

Task comparison uses names, terminal status, reason, partitioned state, and
attempt-status counts. It does not claim to match individual partition attempts
because portable records intentionally preserve only partition presence, not keys.
Failure comparison reports presence and exception type; `show` remains the place
to inspect portable detailed metadata.

## Risks / Trade-offs

- [N+1 loads for a rich list] → acceptable for local history; add a read-only
  adapter convenience method only if measured usage needs it.
- [Missing database path could create state] → validate before adapter import or
  construction.
- [Lossy comparison could imply false precision] → aggregate unavailable identity
  and state the omitted boundary in text and JSON documentation.

## Migration Plan

No migration: these commands read existing schema-v1 records. Removal only drops
CLI access, not stored records or the Python adapter API.
