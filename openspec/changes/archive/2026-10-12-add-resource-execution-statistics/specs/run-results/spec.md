## MODIFIED Requirements

### Requirement: Core exposes exact immutable result value types
The core SHALL expose frozen, slot-based dataclasses named `TaskReference`,
`AttemptReference`, `FailureInfo`, `AttemptResult`, `TaskResult`, and `RunResult`,
plus `str`-backed enums `FlowStatus`, `AttemptStatus`, and `SkipReason`. Their
fields, types, and defaults SHALL be exactly those listed below:

```text
FlowStatus: PENDING="pending", RUNNING="running", SUCCESS="success",
            FAILED="failed", CANCELLED="cancelled"
AttemptStatus: PENDING="pending", RUNNING="running", SUCCESS="success",
               FAILED="failed", SKIPPED="skipped", CANCELLED="cancelled"
SkipReason: DEPENDENCY_BLOCKED="dependency_blocked",
            NO_PARTITION_KEYS="no_partition_keys",
            RESOURCE_CIRCUIT_OPEN="resource_circuit_open",
            MIXED_BLOCKED="mixed_blocked"
TaskReference(task_name: str)
AttemptReference(task: TaskReference, partition_key_present: bool = False,
                 partition_key: object | None = None)
FailureInfo(exception_type: str, message: str, traceback: str)
AttemptResult(attempt: AttemptReference, status: AttemptStatus,
              started_at: datetime, ended_at: datetime, duration: timedelta,
              output: object = None, exception: BaseException | None = None,
              failure: FailureInfo | None = None,
              reason: SkipReason | None = None,
              blocked_by: tuple[AttemptReference, ...] = (),
              resolution: str | None = None,
              materialization_id: str | None = None,
              invocations: tuple[InvocationResult, ...] = ())
TaskResult(task: TaskReference, is_partitioned: bool, status: AttemptStatus,
           started_at: datetime, ended_at: datetime, duration: timedelta,
           attempts: tuple[AttemptResult, ...] = (),
           reason: SkipReason | None = None,
           blocked_by: tuple[AttemptReference, ...] = ())
RunResult(run_id: str, status: FlowStatus, started_at: datetime,
          ended_at: datetime, duration: timedelta,
          tasks: tuple[TaskResult, ...] = (),
          resources: tuple[ResourceStatistics, ...] = ())
```

`resolution` SHALL be `executed` or `materialized` for executor-produced successful attempts and `None` for other statuses. Manually constructed successful attempts MAY retain unknown resolution as `None` without an ID. `materialization_id` SHALL be a non-empty string for reused materialized successes and MAY also be present for executed successes with a completed commit. It SHALL be absent for ordinary successful attempts and non-success outcomes. IDs require a known resolution. Existing zero-argument and explicit version 1–3 projections SHALL retain their prior shapes; version 4 SHALL export these fields.

`TaskReference.task_name` and `RunResult.run_id` SHALL be non-empty strings. An
absent partition SHALL be represented only by `partition_key_present=False` and
`partition_key=None`; a present partition SHALL have
`partition_key_present=True` and a non-`None` raw key. Result collection fields
SHALL remain tuples and result envelopes SHALL not be structurally mutable.

#### Scenario: A falsey partition key remains present
- **WHEN** an `AttemptReference` is constructed with `partition_key_present=True`
  and key `0`, `""`, or `False`
- **THEN** it represents a present partition and is not conflated with an absent one

#### Scenario: An invalid partition reference is rejected
- **WHEN** a reference has an absent partition with a non-`None` key or a present
  partition with `None` as its key
- **THEN** construction raises `ValueError`

#### Scenario: A reused attempt is identifiable in memory
- **WHEN** a committed materialization supplies a successful attempt's output
- **THEN** that attempt's in-memory resolution is `materialized` and it has a committed materialization identifier while its historical portable projections retain their existing shapes and version 4 reports this metadata


`RunResult.resources` SHALL be a tuple of immutable ResourceStatistics snapshots as defined by the resource-execution-statistics capability, defaulting empty without reconstructing absent historical metadata. Existing result projection versions1–4 SHALL omit this field.

#### Scenario: Live resource metadata preserves historical result shape
- **WHEN** a completed result includes resource summaries
- **THEN** the live tuple and exact-name lookup expose them while portable versions1–4 retain their prior shapes
