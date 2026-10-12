# Plans and results

Use `FlowPlan` before execution and `RunResult` after a normally completed run. See
[Plan, result, and events](../concepts/planning-results-events.md) for their roles and
portable projection boundary.

## `FlowPlan`

::: kazeflow.FlowPlan

`FlowPlan.partition_groups` lists the selected immutable `PartitionGroup`
declarations in deterministic plan order. Planning validates group membership and
linear dependencies before execution and does not invoke assets or adapters. The
group limit governs active selected keys; global concurrency and resource policies
continue to govern invocations separately. See [partition groups](../partitions.md#bound-progress-through-a-partition-chain).

## `RunResult`

::: kazeflow.RunResult

`RunResult.resources` contains immutable `ResourceStatistics` snapshots for the
named resources selected by this plan, in plan order. Use
`result.resource_statistics(name)` for exact lookup. A non-string name raises
`TypeError`; an unknown or empty name raises `KeyError`. Historical or manually
constructed results without live summaries do not invent statistics.

`invocations` counts actual starts of an asset body-plus-commit operation.
`succeeded` and `failed` count its completed outcome, and `invocation_duration`
adds monotonic elapsed time through commit. `retries` counts a subsequent body
invocation only when it actually starts. Preparation failures, materialization
reuse, skips, and retry decisions that never start another body do not add an
invocation or retry. Selected resources with no body starts still appear with
zero counters. Counters belong to one run and are not process-wide totals.

`wait_count` and `wait_duration` describe observed resource-admission episodes.
An episode starts when otherwise eligible body work is checked and denied by
resource concurrency, rolling rate limit, retry delay, or resource cooldown. It
ends at body start or terminal skip; repeated checks during one episode count
once. Dependency and preparation waits, and global-only queue time before
resource admission is examined, are not measured. Once an episode starts, its
duration may include later scheduler or global delay. Durations are summed across
operations, may overlap, and do not measure exclusive time for one cause or total
flow wall-clock time.

These summaries exist only on the live `RunResult`. They do not change record
versions 1–4, event projections, CLI or SQLite schemas. A historical record cannot
reconstruct statistics that were not recorded. See the
[resource statistics example](https://github.com/kj-9/kazeflow/blob/main/examples/resource_statistics.py).

## `InvocationResult`

::: kazeflow.InvocationResult

`AttemptResult.invocations` records body/commit operations only when retry behavior
is enabled for the resource. The logical partition attempt remains one result with
one terminal status. `RunResult.to_record()` defaults to record schema v1 for Python
compatibility; pass `schema_version=2` to include invocation history, `3` to include
resource circuit skip reasons, or `4` to include successful materialization
provenance. Circuit skips retain the originating failed attempt in `blocked_by`.
Portable references intentionally retain only the task name and whether a partition
key was present; they do not serialize the raw key, so different partitions of one
task may not be distinguishable in a portable record.

Record v4 adds `resolution` and `materialization_id` to each attempt. A successful
ordinary execution reports `executed` with a null ID; a successful committed
execution reports `executed` with its committed ID; a reuse reports `materialized`
with its ID. Failed, skipped, and cancelled attempts report null for both fields.
Hand-built successful results may leave both unknown. Versions 1–3 keep their
existing shape, and the zero-argument projection remains v1. Materialization IDs
are opaque, adapter-defined diagnostic strings. They are not interpreted as keys,
automatically redacted, or used to load artifacts during inspection.

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
