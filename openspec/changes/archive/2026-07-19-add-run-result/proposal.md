## Why

M1 needs immutable, inspectable execution data before the executor can be
stabilized or presentation can be separated.  The current executor keeps mutable
outputs on `Flow`, returns no structured outcome, and exposes progress only through
Rich-coupled code, leaving callers and future adapters without a neutral contract.

## What Changes

- Add standard-library-only value models for flow, task, and partition results,
  their lifecycle status, timestamps, duration, failure metadata, and explicit
  skip/blocking information.
- Freeze the exact neutral seam shared by both owners: `FlowStatus`,
  `AttemptStatus`, `SkipReason`, `TaskReference`, `AttemptReference`,
  `FailureInfo`, `AttemptResult`, `TaskResult`, `RunResult`, `EventKind`,
  `ExecutionEvent`, and `ExecutionEventConsumer`.
- Define the boundary between raw in-process outputs or exceptions and a
  serializable run-record projection with fixed keys, nesting, ordering, and
  omissions.
- Add neutral execution lifecycle event value types and a consumer protocol for
  progress, logging, and future persistence adapters.
- Freeze the one-way model boundary: `results.py` has no event dependency;
  `events.py` may import neutral value types from `results.py` but events never
  carry result objects.

**BREAKING (planned for M2 integration):** `run()` and `run_async()` will later
return `RunResult` instead of `None`; this change only supplies the unintegrated
models and does not alter those entry points.

## Capabilities

### New Capabilities

- `run-results`: Immutable core result models and portable record projections for
  a completed flow, task attempt, and partition attempt.
- `execution-events`: Neutral, ordered lifecycle event values and observer protocol
  for consumers that must not own execution or presentation.

### Modified Capabilities

<!-- No existing living capability requirement changes. -->

## Impact

- Advances ROADMAP M1, Workstream B, after the archived execution-contracts gate.
- Adds only `src/kazeflow/results.py`, `tests/test_results.py`,
  `src/kazeflow/events.py`, and `tests/test_events.py` in separate ownership
  tracks; it does not edit executor or public-export hotspots.
- The data models use only Python's standard library and introduce no presentation,
  persistence, JSON library, database, daemon, scheduler, or runtime dependency.
- The public model names become available for M2 to integrate; current `Flow`,
  asset decorators, direct function calls, and run-entry-point behavior remain
  unchanged until that later, single-owner integration.
