## MODIFIED Requirements

### Requirement: Result projections support explicit portable record versions
Existing zero-argument result projections SHALL preserve portable record version 1. Explicit version 2 projections SHALL add an ordered `invocations` array to each attempt, with number, status, start/end/duration, nullable failure, retry-delay and cooldown seconds. Explicit version 3 SHALL preserve version 2 fields and support circuit and mixed-blocked reasons. Versions 1 and 2 SHALL reject results containing these new reasons. Explicit version 4 SHALL preserve version 3 fields and add nullable resolution and materialization_id to every attempt using the successful provenance contract. Versions 1–3 SHALL retain their prior shapes and omit provenance. Unknown or boolean versions SHALL be rejected. All versions SHALL omit raw outputs, raw exceptions, raw partition keys and classifier code.

#### Scenario: Export retry history safely
- **WHEN** a recovered result is projected explicitly as portable record version 2
- **THEN** invocation history is included without raw output/exception/key values and version 1 retains its original shape

#### Scenario: Export committed provenance
- **WHEN** a successful committed or reused result is projected as version 4
- **THEN** resolution and ID are present and projecting versions 1–3 still omits both fields

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
          tasks: tuple[TaskResult, ...] = ())
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
