# Results and history

A completed run explains every task's terminal outcome. Persistence is explicit,
local, and intentionally lossy.

## Terminal review

```console
kazeflow run daily.py --yes
kazeflow run daily.py --yes --verbose
kazeflow run daily.py --yes --format json
```

Text includes run ID, overall status and duration, then tasks in plan order. Failed
tasks include portable exception type/message; skipped tasks include a reason.
`--verbose` adds ordered attempt detail without raw keys, outputs, exception objects,
or tracebacks.

## In-memory Python result

```python
from kazeflow import run

result = run(["publish"])
print(result.run_id, result.status, result.duration)

for task in result.tasks:
    print(task.task.task_name, task.status)
    for attempt in task.attempts:
        print(attempt.status, attempt.output)
```

`result.to_record()` returns a new JSON-friendly projection. It does not persist the
run.

## Stored record boundary

| Stored | Not stored |
| --- | --- |
| Run/task/attempt order and statuses | Raw task outputs |
| UTC timestamps and durations | Raw exception objects |
| Skip reasons and dependency blockers | Raw partition-key values |
| Portable failure metadata | Reconstructed `RunResult` objects |

Use [SQLite persistence](guides/persistence.md) to opt in and the
[`runs` reference](cli/history.md) to list, show, or compare records.
