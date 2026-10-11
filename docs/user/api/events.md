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

`ExecutionEvent.to_record()` defaults to schema version 3. Event v3 accepts the
resource circuit skip reason and its source reference. Pass `schema_version=1` for
legacy event kinds or `2` for the earlier numbered-invocation shape; both older
versions reject circuit and mixed-blocked reasons. The source attempt finishes as
failed before pending peers are skipped, so the event stream keeps the causal
ordering without a separate circuit event.
