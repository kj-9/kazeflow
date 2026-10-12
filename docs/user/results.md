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
run. The default projection is record v1 for Python compatibility. Pass
`schema_version=4` to include successful `resolution` and `materialization_id`
metadata; versions 1–3 retain their previous shapes. A committed execution reports
`executed` and its committed ID, while a reused output reports `materialized` and its
ID. Failed, skipped, and cancelled attempts have null provenance fields.

Materialization IDs are opaque, adapter-defined diagnostic strings. They may contain
application information and are not automatically redacted. Result inspection never
uses an ID to load an artifact; it does not reconstruct raw keys or outputs.

## Live resource statistics

The live `RunResult` also exposes a `resources` tuple and an exact-name
`resource_statistics(name)` lookup. Each immutable `ResourceStatistics` snapshot
reports actual body-plus-commit operation starts and outcomes, retries that really
started, and observed resource-admission wait episodes. Selected resources remain
present with zero counters when no body starts, and each run starts with fresh
counters. See the [API reference](api/plans-results.md#runresult) for the field
definitions, lookup errors, and timing boundaries.

Statistics are collected directly during execution, independently of optional
invocation history. A body start counts as one invocation; a completed commit is
included in that operation's success or failure and duration. Preparation, reused
materializations, skips, and retries that never start a body are not counted.
Admission waits start only when ready body work is actually observed being denied
by a resource concurrency, rate, retry-delay, or cooldown rule. The observed interval
ends at body start or terminal skip. It can include later scheduler delay, and
intervals may overlap, so their sum is neither exclusive cause time nor flow wall
clock time.

The counters are available only from a live result. Portable record versions 1–4,
events, CLI output, and SQLite records keep their existing shapes; old records cannot
restore these summaries. See the
[resource statistics example](https://github.com/kj-9/kazeflow/blob/main/examples/resource_statistics.py).

## Stored record boundary

| Stored | Not stored |
| --- | --- |
| Run/task/attempt order and statuses | Raw task outputs |
| UTC timestamps and durations | Raw exception objects |
| Skip reasons and dependency blockers | Dedicated raw partition-key fields |
| Portable failure metadata | Reconstructed `RunResult` objects |
| Successful resolution and materialization ID in record v4 | Raw outputs or adapter code |

This table describes structural fields, not redaction. Portable failure metadata
contains exception messages and tracebacks, which can include partition keys or
other sensitive application values. Treat JSON and SQLite records as potentially
sensitive. See
the [portable-record trust boundary](concepts/trust-boundary.md#portable-record-boundary).

Use [SQLite persistence](guides/persistence.md) to opt in and the
[`runs` reference](cli/history.md) to list, show, or compare records.
