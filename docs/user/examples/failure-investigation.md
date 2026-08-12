# Investigate a failed run

Start with the terminal result, then retain it only when useful.

```console
kazeflow run pipeline.py --yes --verbose --store .kazeflow/runs.sqlite3
```

A failed run exits `1` but still prints a structured terminal result. Text identifies
failed tasks, portable exception type/message, skipped tasks, and dependency
blockers. `--verbose` adds structurally limited attempt-level detail.

Inspect the exact saved record later:

```console
kazeflow runs list
kazeflow runs show RUN_ID
```

Compare a later rerun without assuming dedicated raw-output or partition-key fields
were stored:

```console
kazeflow runs compare FAILED_RUN_ID RETRY_RUN_ID
```

For programmatic in-process diagnosis, inspect each `TaskResult` and `AttemptResult`
on the returned `RunResult`; the Python value retains details intentionally omitted
from portable storage.

Portable failure messages and tracebacks remain application-controlled and can
repeat a partition key or other sensitive value. A stored record is not automatically
redacted; review the
[portable-record trust boundary](../concepts/trust-boundary.md#portable-record-boundary).
