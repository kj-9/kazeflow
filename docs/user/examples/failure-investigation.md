# Investigate a failed run

Start with the terminal result, then retain it only when useful.

```console
kazeflow run pipeline.py --yes --verbose --store .kazeflow/runs.sqlite3
```

A failed run exits `1` but still prints a structured terminal result. Text identifies
failed tasks, portable exception type/message, skipped tasks, and dependency
blockers. `--verbose` adds safe attempt-level detail.

Inspect the exact saved record later:

```console
kazeflow runs list
kazeflow runs show RUN_ID
```

Compare a later rerun without assuming raw outputs or partition values were stored:

```console
kazeflow runs compare FAILED_RUN_ID RETRY_RUN_ID
```

For programmatic in-process diagnosis, inspect each `TaskResult` and `AttemptResult`
on the returned `RunResult`; the Python value retains details intentionally omitted
from portable storage.
