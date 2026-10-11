## MODIFIED Requirements

### Requirement: Event payloads and records exclude result objects and raw payloads
`ExecutionEvent` SHALL NOT accept or carry a `RunResult`, `TaskResult`,
`AttemptResult`, raw asset output, or raw exception object.  Its explicit version 1 projection SHALL preserve the historical keys for legacy lifecycle kinds and reject invocation kinds. Its default version 4 `to_record()` SHALL return exactly these keys:

```text
{"run_id", "sequence", "occurred_at", "kind", "task", "attempt", "status",
 "reason", "blocked_by", "failure", "schema_version", "invocation_number",
 "retry_delay_seconds", "cooldown_seconds", "resolution", "materialization_id"}
```

Default version 4 SHALL contain `schema_version: 4`, preserve invocation metadata and support circuit/mixed-blocked reasons. It SHALL expose nullable resolution and materialization_id, with non-null values valid only for successful attempt finishes and matching terminal result provenance. Explicit version 3 SHALL preserve its historical shape with `schema_version: 3` and omit provenance. Explicit version 2 SHALL retain the previous shape and reject new reasons; version 1 SHALL likewise reject new reasons. Unsupported and boolean versions SHALL be rejected.

The `task`, `attempt`, and `failure` values SHALL be their result-model record
projections or `null`; enum values SHALL be strings; blockers SHALL be an ordered
array of attempt-reference records; and no output, exception, result, or raw
partition-key field SHALL appear.

#### Scenario: A consumer can show failed progress without owning execution data
- **WHEN** a consumer receives a failed attempt-finished event
- **THEN** it can read its failure metadata and attempt identity but cannot obtain a
  raw exception, raw output, or result snapshot from the event
