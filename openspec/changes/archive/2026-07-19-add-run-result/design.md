## Context

The archived `execution-contracts` capability fixes the execution semantics that M1
must represent.  M1 Wave 1 deliberately establishes result and event model modules
before the single M2 owner changes `flow.py` or `assets.py`.  The three owners need
a fully specified seam: previous prose left constructor shapes, records, task events,
and result ordering open to incompatible implementation.

This change advances ROADMAP M1 Workstream B.  It supports the M2 executor, M3
presentation/observer adapters, and M6 persistence adapters on Python 3.10--3.13
with the standard library alone.

## Goals / Non-Goals

**Goals:**

- Freeze the exact immutable public values, fields, defaults, and validation rules
  for completed results and neutral lifecycle events.
- Make raw in-memory output, exceptions, and partition keys distinct from a fixed,
  JSON-compatible record projection.
- Preserve `FlowPlan` task order and selected partition-key order in result and
  record collections.
- Define task identity for all task lifecycle events without treating an absent
  partition as a falsey partition key.

**Non-Goals:**

- Change execution, scheduler, cancellation delivery, `Flow`, exports, TUI/logging,
  dependencies, or persistence behavior.
- Add an event bus, observer registration or error policy, asynchronous consumers,
  durable delivery, retries, a daemon, or a database schema.
- Serialize, hash, copy, compare, or persist arbitrary output, exception, or
  partition-key values.

## Decisions

### Exact public result values

`results.py` exposes only frozen `@dataclass(slots=True)` values and `str, Enum`
enums (not `StrEnum`, which is unavailable on Python 3.10).  These are the exact
public names and constructor signatures; all tuple defaults are immutable:

```python
class FlowStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

class AttemptStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

class SkipReason(str, Enum):
    DEPENDENCY_BLOCKED = "dependency_blocked"
    NO_PARTITION_KEYS = "no_partition_keys"

@dataclass(frozen=True, slots=True)
class TaskReference:
    task_name: str

@dataclass(frozen=True, slots=True)
class AttemptReference:
    task: TaskReference
    partition_key_present: bool = False
    partition_key: object | None = None

@dataclass(frozen=True, slots=True)
class FailureInfo:
    exception_type: str
    message: str
    traceback: str

@dataclass(frozen=True, slots=True)
class AttemptResult:
    attempt: AttemptReference
    status: AttemptStatus
    started_at: datetime
    ended_at: datetime
    duration: timedelta
    output: object = None
    exception: BaseException | None = None
    failure: FailureInfo | None = None
    reason: SkipReason | None = None
    blocked_by: tuple[AttemptReference, ...] = ()

@dataclass(frozen=True, slots=True)
class TaskResult:
    task: TaskReference
    is_partitioned: bool
    status: AttemptStatus
    started_at: datetime
    ended_at: datetime
    duration: timedelta
    attempts: tuple[AttemptResult, ...] = ()
    reason: SkipReason | None = None
    blocked_by: tuple[AttemptReference, ...] = ()

@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: str
    status: FlowStatus
    started_at: datetime
    ended_at: datetime
    duration: timedelta
    tasks: tuple[TaskResult, ...] = ()
```

`TaskReference.task_name` and `run_id` are non-empty strings.  An absent partition
is precisely `partition_key_present=False, partition_key=None`; a present partition
has `partition_key_present=True` and a non-`None` raw key.  This is why task events
use `TaskReference` rather than an optional `AttemptReference`.

`AttemptResult`, `TaskResult`, and `RunResult` are completed snapshots: their status
is terminal, all timestamps are aware UTC, `ended_at >= started_at`, and duration is
non-negative.  The executor obtains duration from a monotonic elapsed measurement;
the value model stores it as `timedelta` and does not derive it from wall time.
`FailureInfo` is required exactly for failed attempts; raw `exception` is optional
for a failed attempt and prohibited otherwise.  `reason` and `blocked_by` are
allowed only on skipped values.  `dependency_blocked` requires one or more blockers;
`no_partition_keys` requires none and is permitted only for a partitioned task with
zero attempts.  A skipped attempt cannot use `no_partition_keys`.

An unpartitioned `TaskResult` has one attempt with the same task and an absent
partition; its aggregate status, reason, and blockers equal that sole attempt's.
A partitioned task has only attempts for that same task with present partitions. For
a non-empty partitioned task, aggregate status is `failed` if any attempt failed,
otherwise `cancelled` if any cancelled, otherwise `skipped` if any skipped, otherwise
`success`.  A `dependency_blocked` aggregate is valid only when every attempt is
dependency-blocked skipped; its blockers are the attempts' blockers concatenated in
attempt order.  A mixed success/skipped aggregate has `reason=None, blocked_by=()`.
`no_partition_keys` is the sole zero-attempt case.

Task references in `RunResult.tasks` are unique. `RunResult.status` is `failed` if
any task failed; otherwise `cancelled` if any task cancelled; otherwise `success`
(including a run containing only successful or skipped tasks). No other aggregate
combination is valid. `RunResult` does not expose a flow-level raw exception or
failure field because attempt-level `FailureInfo` values retain the causal failures.

An alternate mutable builder was rejected because it would re-create mutable
execution state.  Deep-freezing raw outputs was rejected because it would break
ordinary Python assets and cannot be done generically.

### Ordering is part of the result contract

`RunResult.tasks` is an ordered tuple and its order SHALL be exactly the order of
`FlowPlan.tasks` for the run.  `TaskResult.attempts` is an ordered tuple and, for a
partitioned task, SHALL be exactly the selected partition-key order.  Constructors
preserve supplied tuple order and do not sort, coerce, deduplicate, or use a mapping;
the M2 executor is responsible for supplying the validated `FlowPlan` and selected
key sequence.  `to_record()` arrays preserve those tuple orders unchanged.

This rejects unordered mappings because falsey or non-serializable partition keys
must not collapse and because human review needs deterministic output.

### Exact record projection

`TaskReference.to_record()`, `AttemptReference.to_record()`, `FailureInfo.to_record()`,
`AttemptResult.to_record()`, `TaskResult.to_record()`, and `RunResult.to_record()`
return newly created JSON-compatible `dict[str, object]` values.  There are no
schema-version, output, exception, or partition-key fields.  All timestamps use
`datetime.isoformat()` on already-UTC fields and all durations use
`timedelta.total_seconds()`.

```text
TaskReference:    {"task_name": str}
AttemptReference: {"task": TaskReference-record,
                    "partition": {"present": bool}}
FailureInfo:      {"exception_type": str, "message": str, "traceback": str}
AttemptResult:    {"attempt": AttemptReference-record, "status": str,
                    "started_at": str, "ended_at": str,
                    "duration_seconds": float, "reason": str | null,
                    "blocked_by": [AttemptReference-record, ...],
                    "failure": FailureInfo-record | null}
TaskResult:       {"task": TaskReference-record, "is_partitioned": bool,
                    "status": str, "started_at": str, "ended_at": str,
                    "duration_seconds": float, "reason": str | null,
                    "blocked_by": [AttemptReference-record, ...],
                    "attempts": [AttemptResult-record, ...]}
RunResult:        {"run_id": str, "status": str, "started_at": str,
                    "ended_at": str, "duration_seconds": float,
                    "tasks": [TaskResult-record, ...]}
```

The projection is intentionally lossy and is not a database schema or a reversible
codec.  Automatic JSON encoding was rejected because it would constrain asset
authors and blur the core/persistence boundary.

### Exact neutral event values and sequence validation

`events.py` imports only `AttemptReference`, `AttemptStatus`, `FailureInfo`,
`FlowStatus`, `SkipReason`, and `TaskReference` from `results.py`; `results.py`
imports nothing from `events.py`.  It exposes these exact public values:

```python
class EventKind(str, Enum):
    FLOW_STARTED = "flow_started"
    TASK_STARTED = "task_started"
    ATTEMPT_STARTED = "attempt_started"
    ATTEMPT_FINISHED = "attempt_finished"
    TASK_FINISHED = "task_finished"
    FLOW_FINISHED = "flow_finished"

@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    run_id: str
    sequence: int
    occurred_at: datetime
    kind: EventKind
    task: TaskReference | None = None
    attempt: AttemptReference | None = None
    status: FlowStatus | AttemptStatus | None = None
    reason: SkipReason | None = None
    blocked_by: tuple[AttemptReference, ...] = ()
    failure: FailureInfo | None = None

class ExecutionEventConsumer(Protocol):
    def on_event(self, event: ExecutionEvent) -> None: ...

def validate_event_sequence(events: Sequence[ExecutionEvent]) -> None: ...
```

An event's `run_id` is non-empty, `sequence` is positive, and `occurred_at` is aware
UTC.  `flow_started` and `flow_finished` carry no task or attempt; the former has
`FlowStatus.RUNNING`, the latter a terminal `FlowStatus`.  `task_started` and
`task_finished` require `task` and forbid `attempt`; start has
`AttemptStatus.RUNNING`, finish a terminal `AttemptStatus`.  `attempt_started` and
`attempt_finished` require `attempt` and forbid `task`; start has running status,
finish a terminal status.  Reasons and blockers obey the same skipped-value rules as
results.  `failure` is permitted only on `attempt_finished` with `failed` status;
it is required there and absent for all other kinds.  Events therefore never carry
a `RunResult`, `TaskResult`, `AttemptResult`, raw output, or raw exception.

`ExecutionEvent.to_record()` returns exactly:

```text
{"run_id": str, "sequence": int, "occurred_at": str, "kind": str,
 "task": TaskReference-record | null,
 "attempt": AttemptReference-record | null, "status": str,
 "reason": str | null, "blocked_by": [AttemptReference-record, ...],
 "failure": FailureInfo-record | null}
```

`validate_event_sequence` validates a complete run stream: it is non-empty; all
events have one run id; sequences begin at 1 and increment by one; first is
`flow_started`; last is `flow_finished`; a start precedes its matching finish; every
task finish follows all its attempt finishes; and every task/attempt has exactly one
finish and at most one start.  A dependency-blocked task or attempt, and a no-work
task, have a finish but zero starts.  Independent branches have no additional
ordering guarantee.  The function does not dispatch events or schedule work.

### Ownership

| Owner | Exclusive files | Boundary |
| --- | --- | --- |
| Result owner | `src/kazeflow/results.py`, `tests/test_results.py` | Implements all result values/records; no event import. |
| Event owner | `src/kazeflow/events.py`, `tests/test_events.py` | Implements events/protocol/sequence validator; imports only the six neutral result values above. |
| Future M2 owner | `flow.py`, `assets.py`, `tests/test_execution.py` | Sole integration producer after this seam is accepted. |

No Wave 1 owner edits hotspots, package metadata, or existing flow tests.  M2 decides
observer registration and consumer-failure policy, not the shapes above.

## Risks / Trade-offs

- [Raw payload referenced by a frozen envelope can mutate] → envelope structure is
  immutable; raw payloads are explicitly in-memory-only.
- [Lossy records cannot reconstruct a partition key] → the in-memory reference
  retains it; M6 must choose an explicit storage codec if needed.
- [A complete event sequence is stricter than a dropped observer stream] → sequence
  validation is for producer output, not durability or delivery guarantees.
- [Current executor cannot yet meet this model] → M2 is the sole integration owner.

## Migration Plan

1. Result and Event owners implement their exclusive modules and unit tests against
   the frozen signatures and records above.
2. Review their import graph and `validate_event_sequence` tests before integration.
3. M2 builds fresh ordered `RunResult` values from `FlowPlan` and emits complete
   event sequences while repairing scheduler behavior.
4. M3/M6 consume only event and record values after M2 integration.

No code or persisted data is changed by this proposal; no migration or archive is
performed here.

## Open Questions

- M2 alone chooses observer registration and how an observer exception affects the
  executor; this model does not prescribe delivery policy.
- M2 assigns non-empty `run_id` values; these models do not mandate generation.
- M6 chooses any storage-specific codec, record schema version, and replay policy.
