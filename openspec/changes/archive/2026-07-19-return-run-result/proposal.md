## Why

M1 establishes immutable plans, results, and passive events, but the current executor
still executes directly against mutable `Flow.asset_outputs`, stops scheduling after a
failure, loses ready work when a concurrency limit is reached, and presents a Rich UI
as part of every run.  M2 integrates those fixed model contracts into one deterministic
core execution path so that a caller can inspect work before running it and receive a
complete, portable `RunResult` afterwards.

## What Changes

- Add a `Flow.plan()` entry point and make both public entry points build and consume
  the validated `FlowPlan` before any asset is scheduled; make the owning targets and
  registry explicit while retaining a checked compatibility path for `Flow(graph)`.
- Make synchronous `run()` and asynchronous `Flow.run_async()` return completed
  `RunResult` values, including task failure metadata rather than raising asset
  failures.
- Replace the current ready-set handling with a deterministic, bounded drain that
  reaches a terminal outcome for every planned task and selected partition attempt
  exactly once.
- Preserve falsey partition keys and explicit empty selections; apply dependency
  blocking at matching partition granularity while allowing unrelated branches to
  continue, and freeze the injected mapping shape for partitioned dependencies.
- Emit ordered `ExecutionEvent` values to an explicitly supplied passive consumer;
  the default observer is no-op.
- **BREAKING:** remove automatic Rich tree/progress/log presentation from core
  execution and stop treating `Flow.asset_outputs` as the execution-result authority.
- **BREAKING:** reject synchronous `run()` from a thread that already has a running
  event loop, directing callers to `await run_async()`.
- **BREAKING:** a partitioned downstream no longer receives a full upstream partition
  map; it receives a one-entry mapping for its matching key. Cross-partition work
  migrates to a non-partitioned reducer.

## Capabilities

### New Capabilities

- `core-executor-integration`: A single-owner executor integration that consumes
  validated flow plans and produces deterministic results and ordered lifecycle events.

### Modified Capabilities

- None. The existing planning, result, event, and execution-contract specifications
  define the model and semantic contracts this change implements without changing their
  requirements.

## Impact

- Affected implementation: `src/kazeflow/flow.py`, `src/kazeflow/assets.py`, and the
  public exports needed for the existing entry points; one executor owner owns all of
  these integration edits in M2.
- Affected API: `Flow.plan()` is added; `run()` and `Flow.run_async()` return
  `RunResult`; callers that ignored the former `None` return remain source-compatible.
  New `Flow` construction owns explicit targets and a registry; legacy `Flow(graph)`
  remains a checked, deprecated compatibility form.
- Affected tests: executor completion, result, event-order, preflight, failure,
  partition argument shape, rerun, external cancellation, and active-loop tests are
  added or updated by that owner.
- Core remains standard-library-only. This change adds no dependency, persistence,
  TUI renderer, package metadata, or optional-extra work.
