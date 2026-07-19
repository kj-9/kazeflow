## Why

M0 establishes the execution contracts that M1--M4 share before their owners begin
parallel implementation.  The current executor has no structured plan or returned
result, mixes Rich presentation with core execution, and leaves failure, partition,
and event-loop behavior ambiguous; implementing the later milestones without these
decisions would create incompatible models and scheduler semantics.

## What Changes

- Define the public execution vocabulary for flow, task, and partition lifecycle
  status, including the terminal meaning of `success`, `failed`, `skipped`, and
  `cancelled`.
- Define failure propagation: a failed task does not stop independent ready work;
  its direct and transitive dependents are terminally `skipped`.  Cancellation is
  distinct from failure.
- Define the planned `run()` and `run_async()` result/exception contract: both return
  a `RunResult` for completed task failures, while invalid definitions and run
  configuration raise before execution. This M0 change introduces no task-failure
  raising option.
- Define timestamp, duration, partition-key, task-output, serializable-record, and
  core-presentation boundaries that M1 and M2 must implement.
- Define that synchronous `run()` is unavailable from a running event loop and must
  fail before scheduling work, directing callers to `run_async()`.
- Record compatibility and migration expectations for the existing `None` return
  value and implicit Rich terminal UI.  **BREAKING (planned):** future M2/M3 releases
  will return a result and remove implicit Rich presentation from the core execution
  path; callers requiring a renderer must select one explicitly.

## Capabilities

### New Capabilities

- `execution-contracts`: Stable, externally observable semantics for execution
  lifecycle, failure, time, partition input, result data, presentation, and sync/async
  entry points.

### Modified Capabilities

<!-- No existing living capability specs are present. -->

## Impact

- Advances ROADMAP M0 and gates all executor integration work until the contract is
  accepted and archived.
- Defines the requirements consumed by planned M1 `FlowPlan`, `RunResult`, and
  execution-event models; M2 executor integration; and M3 presentation separation.
- Future implementation will affect the public `run`, `Flow.run_async`, `RunConfig`,
  `AssetContext`, and package exports, plus `flow.py`, `assets.py`, presentation
  adapters, and their tests.  This change itself adds only OpenSpec artifacts.
- The contract requires that the core remain Python-standard-library-only, with no
  implicit terminal output, persistence, daemon, scheduler, database, remote worker,
  control plane, or sandbox.  Rich and any persistence backend remain explicit
  optional consumers/adapters.
