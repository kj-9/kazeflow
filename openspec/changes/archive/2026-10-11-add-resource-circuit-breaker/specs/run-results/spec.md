## MODIFIED Requirements

### Requirement: Result snapshots are completed, time-valid terminal outcomes
`AttemptResult` and `TaskResult` SHALL accept only terminal `AttemptStatus` values:
`success`, `failed`, `skipped`, or `cancelled`.  `RunResult` SHALL accept only
terminal `FlowStatus` values: `success`, `failed`, or `cancelled`.  Each result
timestamp SHALL be aware UTC, `ended_at` SHALL not precede `started_at`, and
`duration` SHALL be a non-negative `timedelta` supplied from monotonic elapsed time,
not calculated from wall-clock timestamps.

An attempt with `failed` status SHALL have `FailureInfo`; it MAY retain a raw
exception in `exception`.  A non-failed attempt SHALL have neither `failure` nor
`exception`.  A skipped attempt SHALL have a reason; `dependency_blocked` and `resource_circuit_open` are valid for attempts and SHALL have non-empty
`blocked_by`.  A non-skipped attempt SHALL have `reason=None` and empty
`blocked_by`.

#### Scenario: A failed attempt retains portable and in-memory failure views
- **WHEN** a failed attempt has failure metadata and a raw exception instance
- **THEN** the result accepts both while only the metadata is eligible for a record

#### Scenario: A malformed terminal snapshot is rejected
- **WHEN** a completed result has a naive or non-UTC timestamp, negative duration,
  reversed timestamps, or a non-terminal status
- **THEN** construction raises `ValueError`

### Requirement: Task aggregates preserve selected partition outcomes and order
For an unpartitioned `TaskResult`, `is_partitioned` SHALL be false and `attempts`
SHALL contain exactly one attempt for the same task with an absent partition.  For a
partitioned task, `is_partitioned` SHALL be true and every attempt SHALL reference
the same task with a present partition.  A partitioned task with no attempts SHALL
have status `skipped`, reason `no_partition_keys`, and no blockers; no other
zero-attempt task result is valid.

For non-empty partitioned tasks, aggregate status SHALL be `failed` if any attempt
failed; otherwise `cancelled` if any attempt was cancelled; otherwise `skipped` if
any attempt was skipped; otherwise `success`.  A uniform blocked aggregate SHALL use its shared `dependency_blocked` or `resource_circuit_open` reason only when every attempt is skipped with that reason; an all-skipped aggregate with both reasons SHALL use `mixed_blocked`. Its `blocked_by`
SHALL concatenate attempt blockers in attempt order.  A mixed success/skipped
aggregate SHALL use `reason=None` and `blocked_by=()`.

An unpartitioned task's aggregate status, reason, and blockers SHALL equal its sole
attempt's values.  Task references in `RunResult.tasks` SHALL be unique.  `RunResult`
status SHALL be `failed` if any task failed; otherwise `cancelled` if any task was
cancelled; otherwise `success`, including a run containing only success or skipped
tasks.  Construction SHALL reject a task or run whose aggregate does not match these
rules.

`RunResult.tasks` SHALL be in exactly the `FlowPlan.tasks` order for the run.
`TaskResult.attempts` for a partitioned task SHALL be in exactly the selected
partition-key order.  Result constructors and projections SHALL preserve supplied
tuple order and SHALL NOT sort, coerce, deduplicate, or use mappings to represent
tasks or attempts.

#### Scenario: Mixed partition output stays ordered and visible
- **WHEN** selected keys are ordered `(0, "", 1)` and their outcomes include a
  success and a dependency-blocked skip
- **THEN** the attempt tuple preserves that exact order and the aggregate is skipped
  with no aggregate reason or blockers

#### Scenario: Explicitly empty partitions represent no work
- **WHEN** a partitioned task has an explicit empty selection
- **THEN** it has zero attempts, aggregate status `skipped`, reason
  `no_partition_keys`, and no blockers

### Requirement: Result projections support explicit portable record versions
Existing zero-argument result projections SHALL preserve portable record version 1. Explicit version 2 projections SHALL add an ordered `invocations` array to each attempt, with number, status, start/end/duration, nullable failure, retry-delay and cooldown seconds. Other record fields SHALL remain identical. Explicit version 3 SHALL preserve version 2 fields and support circuit and mixed-blocked reasons. Versions 1 and 2 SHALL reject results containing these new reasons instead of mislabeling them. Unknown or boolean versions SHALL be rejected. Both projections SHALL omit raw outputs, raw exceptions, raw partition keys, classifier code and materialization provenance.

#### Scenario: Export retry history safely
- **WHEN** a recovered result is projected explicitly as portable record version 2
- **THEN** its invocation history is included without raw output/exception/key values and version 1 remains available with its original shape


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

`resolution` SHALL be `executed` or `materialized` for successful attempts and
`None` for other statuses. `materialization_id` SHALL be present only for a
successful materialized asset attempt with a committed record; it SHALL be absent
for ordinary successful attempts and failures. These fields are in-memory only
in this change; existing `to_record()` keys and schemas SHALL remain unchanged.

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
- **THEN** that attempt's in-memory resolution is `materialized` and it has a committed materialization identifier while its portable record retains the existing shape
