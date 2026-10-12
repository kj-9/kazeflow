## Why

The final Issue #1 audit identified per-resource execution, admission-wait and retry statistics in its result-query scope. Partition outcome queries deliberately deferred statistics; this separate ROADMAP follow-up completes that scope without changing portable contracts.

## What Changes

- Add immutable ResourceStatistics snapshots and live RunResult.resources plus resource_statistics(name) lookup.
- Count actual resource body-plus-commit operations and their outcomes/retry starts, observed resource admission wait episodes and elapsed durations.
- Include zero statistics for selected resources whose attempts are materialized or never start a body; reset each run.
- Document exact timing/accounting boundaries and preserve all portable projections.
- Scope: run-local in-memory counters. Non-goals: telemetry backend, scheduler events, persisted/CLI statistics, dependency/global-only queue measurement, automatic recommendations or cross-run totals.

## Capabilities

### New Capabilities
- `resource-execution-statistics`: validated live summaries of actual invocation work and observed admission waits.

### Modified Capabilities
- `run-results`: append optional live resource statistics to the exact RunResult model; existing record/event/CLI/SQLite version shapes remain unchanged.

## Impact

Additive stdlib-only public ResourceStatistics model and RunResult field/query. Executor records monotonic timings without invoking additional adapters or observers. Historical stored records cannot fabricate statistics.
