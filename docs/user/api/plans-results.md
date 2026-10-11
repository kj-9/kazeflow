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
