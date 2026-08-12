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
`--verbose` adds ordered attempt detail without dedicated raw-key fields, outputs,
exception objects, or tracebacks. Failure messages shown in normal text remain
application-controlled and can repeat those values.

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
| Skip reasons and dependency blockers | Dedicated raw partition-key fields |
| Portable failure metadata | Reconstructed `RunResult` objects |

This table describes structural fields, not redaction. Portable failure metadata
contains exception messages and tracebacks, which can include partition keys or
other sensitive application values. Treat JSON and SQLite records as potentially
sensitive. See
the [portable-record trust boundary](concepts/trust-boundary.md#portable-record-boundary).

Use [SQLite persistence](guides/persistence.md) to opt in and the
[`runs` reference](cli/history.md) to list, show, or compare records.
