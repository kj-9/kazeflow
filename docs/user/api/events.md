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

`ExecutionEvent.to_record()` defaults to schema version 4. Event v4 adds nullable
`resolution` and `materialization_id` fields to successful `attempt_finished`
events. They match the terminal result: an ordinary success is `executed` without
an ID, a committed execution is `executed` with its ID, and reuse is `materialized`
with its ID. Other event kinds and non-successful finishes have null fields.
Explicit versions 1–3 retain their prior shapes. Version 3 accepts resource circuit
skip reasons and their source reference; versions 1 and 2 reject circuit and
mixed-blocked reasons. The source attempt finishes as failed before pending peers
are skipped, so the event stream keeps the causal ordering without a separate
circuit event. Materialization IDs are opaque application-defined diagnostic data
and are not automatically redacted or resolved to artifacts.
