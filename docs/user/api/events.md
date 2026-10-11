# Execution events

Events are optional lifecycle observations for progress and logging adapters. They
do not authorize execution or replace the final result.

## `EventKind`

::: kazeflow.EventKind

## `ExecutionEvent`

::: kazeflow.ExecutionEvent

## `ExecutionEventConsumer`

::: kazeflow.ExecutionEventConsumer

Retry-enabled resource operations emit `invocation_started` and
`invocation_finished` events with the logical attempt identity and one-based
`invocation_number`. A failed finish may include `retry_delay_seconds` and
`cooldown_seconds`; these fields contain requested waits, not elapsed wall time.
The ordinary attempt lifecycle still ends once per task or partition, and TUI
progress continues to count logical attempts.

`ExecutionEvent.to_record()` defaults to schema version 2, which includes nullable
invocation fields. Pass `schema_version=1` to project legacy event kinds; new
invocation event kinds cannot be represented in that older shape.
