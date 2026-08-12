# SQLite adapter

The adapter is imported explicitly and remains outside the core import path:

```python
from kazeflow.sqlite_store import SQLiteRunStore

with SQLiteRunStore("runs.sqlite3") as store:
    saved = store.save(result)
    loaded = store.load(result.run_id)
    recent = store.list_runs(limit=10)
```

Constructing a store creates or initializes schema version 1. It stores portable
records, not reconstructed in-memory `RunResult` objects.

## `SQLiteRunStore`

::: kazeflow.sqlite_store.SQLiteRunStore

## `StoredRunRecord`

::: kazeflow.sqlite_store.StoredRunRecord

## `StoredRunSummary`

::: kazeflow.sqlite_store.StoredRunSummary
