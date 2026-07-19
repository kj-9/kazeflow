## ADDED Requirements

### Requirement: Flow owns explicit targets and a registry
`Flow(targets, *, registry=default_registry)` SHALL accept direct targets only as a
non-empty `list[str]` or `tuple[str, ...]`, and SHALL retain their canonical target
tuple and the exact supplied `AssetRegistry` as its execution-definition source.
`Flow.plan(run_config=None)` SHALL resolve those owned targets only against that owned
registry and SHALL return its `FlowPlan` without invoking an asset. Module-level
`run(asset_names, ..., *, registry=default_registry)` SHALL construct `Flow` with those
exact values.

For source compatibility, `Flow(graph)` SHALL temporarily accept only a legacy
`dict[str, set[str]]`. It SHALL derive direct targets as the lexical graph keys that are
not a dependency of another graph key, retain a deprecated copy at `flow.graph`, and
reject planning unless the graph's closure and direct dependency sets exactly equal the
plan resolved from those targets in its registry. The legacy graph SHALL NOT be an
alternative asset/dependency source; new code SHALL use direct targets.

#### Scenario: A flow uses its explicitly supplied registry
- **WHEN** a caller constructs `Flow(["target"], registry=custom_registry)` while the
  default registry contains different assets
- **THEN** planning and execution resolve `target` only from `custom_registry`

#### Scenario: A legacy graph must match its registry plan
- **WHEN** a caller constructs `Flow(graph)` whose dependencies differ from the
  registry closure inferred from its direct targets
- **THEN** planning raises `ValueError` before an asset or event is invoked

### Requirement: Executor preflights and exposes the inspectable plan
Module-level `run()` and `Flow.run_async()` SHALL obtain the validated `FlowPlan` from
`Flow.plan()` before emitting any execution event or scheduling an asset. They SHALL
accept the existing `RunConfig` keys `max_concurrency` and `partition_keys` and apply
the `FlowPlan` validation rules. Unknown targets, missing dependencies, cycles, invalid
concurrency, omitted required partition selection, `None` keys, unhashable keys, and
duplicate-equal keys SHALL raise before any asset function is invoked.

#### Scenario: Plan inspection has no execution side effect
- **WHEN** a caller calls `Flow.plan()` for a flow whose asset would change observable
  state if invoked
- **THEN** the caller receives its deterministic `FlowPlan` and the asset has not run

#### Scenario: Invalid configuration fails before an event or asset
- **WHEN** a caller supplies an invalid `RunConfig` for `run()` or `Flow.run_async()`
- **THEN** the call raises validation error before a flow-start event or any asset
  invocation

### Requirement: Completed entry points return ordered run results
The module-level synchronous `run()` and `Flow.run_async()` SHALL return a terminal
`RunResult` for every normally completed execution. `RunResult.tasks` SHALL be in the
exact `FlowPlan.tasks` order, and each partitioned `TaskResult.attempts` SHALL retain
the exact selected partition-key order. Successful outputs and raw exceptions SHALL be
available only through the in-memory result model according to the run-results
contract; asset exceptions SHALL produce a failed result rather than escape from a
normally completed flow. Existing callers that ignore the former `None` result SHALL
remain source-compatible.

#### Scenario: A sync and an async successful invocation return results
- **WHEN** equivalent synchronous and asynchronous callers execute a valid flow
- **THEN** each receives a terminal `RunResult` with task order matching its plan

#### Scenario: An asset failure is represented in a result
- **WHEN** an asset raises while executing a valid plan
- **THEN** the call returns a failed `RunResult` whose failed attempt has serializable
  failure metadata and any raw exception only in memory

### Requirement: External asyncio cancellation propagates without a synthetic result
M2 SHALL expose no public cancellation parameter or cancellation-result API. If a
caller externally cancels the task awaiting `Flow.run_async()`, the method SHALL stop
scheduling pending attempts, request cancellation of executor-created pending/running
asyncio tasks, and re-raise `asyncio.CancelledError`. A coroutine asset SHALL receive
normal asyncio cancellation. A synchronous asset already submitted to an executor
SHALL have its awaitable wrapper cancelled but MAY continue physically running; its
eventual output or exception SHALL be discarded and SHALL not become dependency input
or a result. External cancellation SHALL NOT be represented as `FailureInfo`, a
terminal `RunResult`, or a completed-flow status.

The event consumer MAY receive only the lifecycle prefix emitted before external
cancellation. The executor SHALL emit no synthetic terminal events, SHALL not promise
an event stream accepted by `validate_event_sequence`, and SHALL not internally
validate that partial stream. Terminal `RunResult`, exactly-once, and complete-event
guarantees apply only to normal completion of `run_async()`, including completed asset
failures.

#### Scenario: An externally cancelled async run has no synthetic completion
- **WHEN** a caller cancels an awaiting `Flow.run_async()` while a coroutine asset is
  running and another planned attempt is pending
- **THEN** `CancelledError` propagates, the pending attempt is not started, the running
  task is requested to cancel, and no terminal `RunResult` or flow-finished event is
  returned or emitted

#### Scenario: A cancelled thread wrapper cannot leak output
- **WHEN** a caller externally cancels `Flow.run_async()` while a synchronous asset is
  already running in the executor
- **THEN** `CancelledError` propagates and the asset's eventual output is not supplied
  to a dependent or retained in a run result

### Requirement: Executor drains planned work exactly once under a concurrency bound
For a valid normally completed plan, the executor SHALL reach exactly one terminal
result for every selected unpartitioned attempt and every selected partition attempt
before returning. It SHALL not lose work when more ready tasks or partitions exist than
`max_concurrency`; it SHALL schedule no more than the configured positive bound at a
time and SHALL continue draining newly available work until all planned tasks are
terminal. The task and attempt result order SHALL remain plan/selection order even when
independent work completes in a different observed order.

#### Scenario: More ready tasks than slots all complete
- **WHEN** several independent planned tasks become ready with `max_concurrency` less
  than their count
- **THEN** each task's asset is invoked exactly once, no more than the limit run at a
  time, and every task has one terminal result

#### Scenario: More selected partitions than slots all complete
- **WHEN** a partitioned task has more selected keys than `max_concurrency`
- **THEN** every selected key has exactly one terminal attempt result and no more than
  the configured number of attempts run at a time

### Requirement: Failure continuation and dependency blocking preserve granularity
After a task or partition attempt fails, the executor SHALL allow independent ready
branches and attempts already running to reach their own terminal results. It SHALL not
invoke work whose dependency is failed, cancelled, or dependency-blocked skipped;
instead it SHALL record `skipped(dependency_blocked)` with the required `blocked_by`
references. When both sides are partitioned, only the downstream attempt with the
matching partition key is blocked; unaffected keys remain runnable. A non-partitioned
dependent SHALL not receive a partial partition mapping after a failed, cancelled, or
dependency-blocked aggregate.

#### Scenario: A failed branch does not stop an independent branch
- **WHEN** one ready branch fails while another ready branch is independent
- **THEN** the independent branch completes and the flow result contains both its
  terminal outcome and the failed branch outcome

#### Scenario: A partition failure blocks only its matching dependent key
- **WHEN** an upstream partition for key `0` fails and the upstream partition for key
  `""` succeeds
- **THEN** only the downstream partition for key `0` is dependency-blocked, while the
  downstream partition for key `""` remains eligible

### Requirement: Dependency arguments have explicit partition shapes
For each declared dependency parameter of a runnable partitioned asset, an
unpartitioned upstream SHALL supply its raw successful output and a partitioned
upstream SHALL supply exactly `{current_partition_key: matching_successful_output}`.
When a partitioned asset has multiple partitioned dependencies, each named parameter
SHALL receive its own one-entry mapping. A failed, cancelled, or dependency-blocked
sibling key SHALL not be inserted into that mapping or block a different matching key;
a non-successful matching key SHALL block that downstream attempt before invocation.

A non-partitioned downstream SHALL receive a full ordered mapping of every selected
key and output for each partitioned dependency only after that dependency aggregate is
`success`; it SHALL receive `{}` after `skipped(no_partition_keys)` and SHALL never
receive a partial map. Unpartitioned dependencies SHALL continue to supply raw outputs.
The former full-map input to a partitioned downstream is removed; such callers SHALL
migrate matching-key access to `dependency[context.partition_key]` and cross-key work
to a non-partitioned reducer.

#### Scenario: A matching successful key receives its one-entry mapping
- **WHEN** a partitioned upstream and downstream both run for keys `0` and `""`
- **THEN** downstream key `0` receives `{0: upstream_output}` and downstream key `""`
  receives `{"": upstream_output}`, never the other key's output

#### Scenario: A failed sibling key does not contaminate another key
- **WHEN** upstream key `0` fails and upstream key `""` succeeds
- **THEN** downstream key `0` is blocked and downstream key `""` executes with only
  the one-entry mapping for `""`

#### Scenario: Multiple partitioned dependencies are distinct one-entry maps
- **WHEN** a partitioned asset depends on partitioned `left` and `right` for key `0`
- **THEN** its `left` parameter receives `{0: left_output}` and its `right` parameter
  receives `{0: right_output}`

#### Scenario: A reducer receives a full map only after aggregate success
- **WHEN** a non-partitioned reducer depends on a partitioned upstream whose selected
  keys all succeeded
- **THEN** it receives the ordered full mapping of all selected keys and outputs

### Requirement: Partition presence, empty work, and reruns follow model contracts
The executor SHALL use partition presence rather than truthiness to identify an
attempt. Supplied `0`, `""`, and `False` keys SHALL each execute as present partitions.
An explicit empty selection SHALL produce the required zero-attempt
`skipped(no_partition_keys)` task result and SHALL allow an otherwise-runnable
non-partitioned downstream to execute with `{}`. Each invocation SHALL use fresh
execution state and SHALL not read outputs retained from a previous run of the same
`Flow`.

#### Scenario: Falsey selected keys execute as partitions
- **WHEN** a partitioned task is executed with valid selected keys including `0` or
  `""`
- **THEN** its result has present-partition attempts for those exact keys rather than
  unpartitioned attempts

#### Scenario: Empty partitions permit a reducer
- **WHEN** a partitioned upstream has an explicit empty selection and an otherwise
  runnable non-partitioned downstream depends on it
- **THEN** the upstream is `skipped(no_partition_keys)` and the downstream runs with
  an empty mapping

#### Scenario: A rerun cannot consume stale output
- **WHEN** the same flow object is run twice and the first run produced an output that
  would alter the second run if retained
- **THEN** the second run does not receive the first run's output except through work
  executed during the second invocation

### Requirement: Executor emits a complete passive event stream with a no-op default
`run()` and `Flow.run_async()` SHALL accept an optional `ExecutionEventConsumer`; when
none is supplied, execution SHALL use a no-op consumer and SHALL not print, configure
global logging, create files, create a database, import a third-party presentation
library, or render terminal UI. For each normally completed run, a supplied consumer
SHALL receive one complete stream that conforms to `validate_event_sequence`, with
sequence numbers beginning at one and increasing in observed emission order. The stream
SHALL include flow start and finish, starts/finishes for scheduled tasks and attempts,
terminal finishes without starts for dependency-blocked/no-work work as permitted by
the event contract, and failure metadata on failed attempt finishes.

#### Scenario: Default execution has no presentation side effect
- **WHEN** a caller executes a flow without an event consumer
- **THEN** it receives its `RunResult` without terminal UI output, global logging
  configuration, persistence, or a third-party presentation import

#### Scenario: A consumer receives causally valid failure events
- **WHEN** a planned attempt fails and a consumer records all events
- **THEN** the recorded stream validates, includes the failed attempt finish with
  failure metadata, and ends with the failed flow finish

### Requirement: Synchronous run rejects a currently running event loop
The module-level synchronous `run()` SHALL detect a running event loop in its calling
thread before planning, event emission, or asset execution. In that context it SHALL
raise `RuntimeError` that directs the caller to await `Flow.run_async()`; outside that
context it SHALL execute the same normally completed semantics and return the same kind
of `RunResult` as the asynchronous entry point.

#### Scenario: Sync run from async code fails before work
- **WHEN** code running in an active event loop calls module-level `run()`
- **THEN** it raises `RuntimeError` naming `run_async()` and no asset or event consumer
  is invoked

### Requirement: Automatic Rich presentation is removed from core execution
Core executor code SHALL not construct or enter a Rich renderer, call automatic flow
tree/progress display, use renderer progress identifiers as scheduling state, or use a
Rich logger to execute an asset. Asset context logging supplied by core SHALL be
standard-library-only and silent unless a caller configures its own logger. Existing
decorator registration, dependency inference, and direct execution of asset functions
remain available; callers that relied on automatic Rich display SHALL migrate to an
explicit optional renderer supplied by later presentation work.

#### Scenario: Core executes with only the standard library path
- **WHEN** a caller runs a valid core flow without choosing presentation
- **THEN** execution semantics, results, and events are available without automatic
  Rich presentation behavior
