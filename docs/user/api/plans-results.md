# Plans and results

Use `FlowPlan` before execution and `RunResult` after a normally completed run. See
[Plan, result, and events](../concepts/planning-results-events.md) for their roles and
portable projection boundary.

## `FlowPlan`

::: kazeflow.FlowPlan

## `RunResult`

::: kazeflow.RunResult

## `InvocationResult`

::: kazeflow.InvocationResult

`AttemptResult.invocations` records body/commit operations only when retry behavior
is enabled for the resource. The logical partition attempt remains one result with
one terminal status. `RunResult.to_record()` defaults to record schema v1 for Python
compatibility; pass `schema_version=2` to include invocation history, or `3` to
include resource circuit skip reasons. Circuit skips retain the originating failed
attempt in `blocked_by`. Portable references intentionally retain only the task name
and whether a partition key was present; they do not serialize the raw key, so
different partitions of one task may not be distinguishable in a portable record.

For a live `RunResult`, `failed_partition_keys(task_name)` returns the original keys
for terminal failed attempts, and `circuit_skipped_partition_keys(task_name)` returns
the original keys for attempts skipped because a resource circuit opened. Both return
tuples in attempt order. They include falsey keys such as `0`, `""`, and `False`, and
they do not treat a failed retry invocation as a failed logical attempt if that
attempt later succeeded. A skipped attempt is selected only when its terminal reason
is `resource_circuit_open`.

Each method raises `TypeError` when `task_name` is not a string, `KeyError` when the
task is absent, and `ValueError` when the task exists but is not partitioned. A
partitioned task with no matching outcomes returns `()`. The tuple is immutable, but
its keys are the original application objects; mutable keys remain owned by the
application. These helpers inspect only the live result. Portable records omit raw
keys, so a record loaded from storage cannot reconstruct or provide these queries.
To run selected outcomes again, pass the returned keys explicitly in a new run's
`partition_keys` configuration. kazeflow does not resume a prior run automatically.
See the [partition outcome example](https://github.com/kj-9/kazeflow/blob/main/examples/partition_outcomes.py).
