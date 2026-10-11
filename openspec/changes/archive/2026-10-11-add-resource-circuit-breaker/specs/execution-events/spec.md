## MODIFIED Requirements

### Requirement: Event kinds have exact payload validation rules
`flow_started` SHALL carry neither task nor attempt and have status
`FlowStatus.RUNNING`; `flow_finished` SHALL carry neither task nor attempt and have
a terminal `FlowStatus`.  `task_started` and `task_finished` SHALL carry a task and
no attempt; task start SHALL have `AttemptStatus.RUNNING` and task finish a terminal
`AttemptStatus`.  `attempt_started` and `attempt_finished` SHALL carry an attempt
and no task; attempt start SHALL have `AttemptStatus.RUNNING` and attempt finish a
terminal `AttemptStatus`.

`reason` and `blocked_by` SHALL follow the skipped-value rules of the result model. Circuit-skipped attempt finishes SHALL carry exactly one source attempt. Task finishes with mixed skip causes SHALL use `mixed_blocked` and retain ordered blockers; this reason is invalid for attempt finishes.
`invocation_started` and `invocation_finished` SHALL carry an attempt and a positive one-based invocation number, no task or skip fields. Start SHALL be running; finish SHALL be success or failed. Only failed invocation finishes MAY carry finite non-negative retry-delay/cooldown seconds. Other events SHALL carry no invocation fields.

`failure` SHALL be present exactly for failed `attempt_finished` or failed `invocation_finished` and SHALL be absent for every other event.  Construction SHALL reject incompatible kind,
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
`AttemptResult`, raw asset output, or raw exception object.  Its explicit version 1 projection SHALL preserve the historical keys for legacy lifecycle kinds and reject invocation kinds. Its default version 3 `to_record()` SHALL return exactly these keys:

```text
{"run_id", "sequence", "occurred_at", "kind", "task", "attempt", "status",
 "reason", "blocked_by", "failure", "schema_version", "invocation_number",
 "retry_delay_seconds", "cooldown_seconds"}
```

Version 3 SHALL contain `schema_version: 3` and support circuit/mixed-blocked reasons. Explicit version 2 SHALL retain the previous shape and reject new reasons; version 1 SHALL likewise reject new reasons. Unsupported and boolean versions SHALL be rejected.

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
attempt-finished events for that task.  Dependency-blocked and circuit-skipped pending attempts and no-work tasks SHALL allow finish events with zero starts. Circuit-skipped retrying attempts SHALL retain their existing start and numbered history. Circuit skip source references SHALL identify an earlier failed attempt finish in this stream.  The contract imposes no
additional ordering between independent concurrent branches.

#### Scenario: Independent attempts may finish in observed order
- **WHEN** two independent attempts run concurrently
- **THEN** their finish events have consecutive observed sequence numbers in either
  order while both precede their respective task-finished events

#### Scenario: An invalid event stream is rejected
- **WHEN** a stream skips a sequence number, finishes a task before one of its
  attempts, or places a start after its matching finish
- **THEN** `validate_event_sequence` raises `ValueError`

Numbered invocation pairs SHALL be contiguous per logical attempt, occur after its logical start and before its logical finish, and have exactly one start/finish per number. A new invocation SHALL follow a failed prior invocation with a retry delay; duplicate, overlapping, skipped-number, post-success or unfinished invocations SHALL be rejected. Invocations SHALL only be emitted for resource policies with a classifier or a maximum count above one; ordinary runs retain their legacy event-kind sequence. A success/failed logical finish with numbered invocations SHALL match its final invocation status and SHALL NOT occur while another retry is scheduled.

#### Scenario: Retry lifecycle remains causal
- **WHEN** an enabled attempt retries after its first invocation fails
- **THEN** invocation 1 finishes before invocation 2 starts and the logical attempt finishes only after invocation 2
