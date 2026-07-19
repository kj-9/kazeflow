## Purpose

Define passive, presentation-neutral execution event values, portable projections,
and complete-stream causal validation.

## Requirements

### Requirement: Core exposes exact neutral event values and consumer protocol
The core SHALL expose `EventKind` as a `str`-backed enum with exactly
`FLOW_STARTED="flow_started"`, `TASK_STARTED="task_started"`,
`ATTEMPT_STARTED="attempt_started"`, `ATTEMPT_FINISHED="attempt_finished"`,
`TASK_FINISHED="task_finished"`, and `FLOW_FINISHED="flow_finished"`.

The core SHALL expose this frozen, slot-based `ExecutionEvent` value and protocol:

```text
ExecutionEvent(run_id: str, sequence: int, occurred_at: datetime, kind: EventKind,
               task: TaskReference | None = None,
               attempt: AttemptReference | None = None,
               status: FlowStatus | AttemptStatus | None = None,
               reason: SkipReason | None = None,
               blocked_by: tuple[AttemptReference, ...] = (),
               failure: FailureInfo | None = None)
ExecutionEventConsumer.on_event(event: ExecutionEvent) -> None
validate_event_sequence(events: Sequence[ExecutionEvent]) -> None
```

`run_id` SHALL be non-empty, `sequence` SHALL be a positive integer, and
`occurred_at` SHALL be aware UTC.  `TaskReference` SHALL identify task lifecycle
events even when a task has no attempts; `AttemptReference` SHALL identify only
attempt lifecycle events.  The event module SHALL import from results only
`AttemptReference`, `AttemptStatus`, `FailureInfo`, `FlowStatus`, `SkipReason`, and
`TaskReference`; results SHALL not import events.

#### Scenario: A no-work task has an unambiguous identity
- **WHEN** a partitioned task has no selected keys
- **THEN** its `task_finished` event has `task=TaskReference(task_name=...)`, no
  attempt, status `skipped`, and reason `no_partition_keys`

#### Scenario: Importing results does not require events
- **WHEN** application code imports `kazeflow.results`
- **THEN** no event class or consumer protocol is imported as a dependency

### Requirement: Event kinds have exact payload validation rules
`flow_started` SHALL carry neither task nor attempt and have status
`FlowStatus.RUNNING`; `flow_finished` SHALL carry neither task nor attempt and have
a terminal `FlowStatus`.  `task_started` and `task_finished` SHALL carry a task and
no attempt; task start SHALL have `AttemptStatus.RUNNING` and task finish a terminal
`AttemptStatus`.  `attempt_started` and `attempt_finished` SHALL carry an attempt
and no task; attempt start SHALL have `AttemptStatus.RUNNING` and attempt finish a
terminal `AttemptStatus`.

`reason` and `blocked_by` SHALL follow the skipped-value rules of the result model.
`failure` SHALL be present exactly for `attempt_finished` with failed status and
SHALL be absent for every other event.  Construction SHALL reject incompatible kind,
identity, status, reason, blocker, or failure combinations.

#### Scenario: A terminal failed attempt is complete without a result object
- **WHEN** an attempt finishes with failed status
- **THEN** its event has an attempt reference and failure metadata but no task field
  and no result object

#### Scenario: A dependency-blocked task never starts
- **WHEN** a task is skipped before execution because of a dependency
- **THEN** it has a task-finished event with blockers and no task-started event is
  required

### Requirement: Event payloads and records exclude result objects and raw payloads
`ExecutionEvent` SHALL NOT accept or carry a `RunResult`, `TaskResult`,
`AttemptResult`, raw asset output, or raw exception object.  Its `to_record()` SHALL
return exactly these keys:

```text
{"run_id", "sequence", "occurred_at", "kind", "task", "attempt", "status",
 "reason", "blocked_by", "failure"}
```

The `task`, `attempt`, and `failure` values SHALL be their result-model record
projections or `null`; enum values SHALL be strings; blockers SHALL be an ordered
array of attempt-reference records; and no output, exception, result, or raw
partition-key field SHALL appear.

#### Scenario: A consumer can show failed progress without owning execution data
- **WHEN** a consumer receives a failed attempt-finished event
- **THEN** it can read its failure metadata and attempt identity but cannot obtain a
  raw exception, raw output, or result snapshot from the event

### Requirement: Complete event streams have observable causal order
`validate_event_sequence` SHALL validate a complete run stream.  A valid stream is
non-empty, has one run id, begins at sequence 1, increments by one for every event,
starts with `flow_started`, and ends with `flow_finished`.  Each task and attempt
SHALL have exactly one finish event and at most one start event; every start SHALL
precede its matching finish; and every task-finished event SHALL follow all
attempt-finished events for that task.  Dependency-blocked tasks or attempts and
no-work tasks SHALL have finish events with zero starts.  The contract imposes no
additional ordering between independent concurrent branches.

#### Scenario: Independent attempts may finish in observed order
- **WHEN** two independent attempts run concurrently
- **THEN** their finish events have consecutive observed sequence numbers in either
  order while both precede their respective task-finished events

#### Scenario: An invalid event stream is rejected
- **WHEN** a stream skips a sequence number, finishes a task before one of its
  attempts, or places a start after its matching finish
- **THEN** `validate_event_sequence` raises `ValueError`

### Requirement: Event values and consumers are passive model-layer constructs
`ExecutionEvent` values, `ExecutionEventConsumer`, and `validate_event_sequence`
SHALL not print, configure logging, render a UI, create files or databases, persist
records, dispatch events, or schedule work.  Consumer registration, dispatch,
asynchronous adaptation, and consumer-exception handling remain M2/M3 integration
decisions.

#### Scenario: A passive consumer uses no presentation dependency
- **WHEN** an object implements `ExecutionEventConsumer` to append events to memory
- **THEN** it can do so without importing Rich or a persistence library
