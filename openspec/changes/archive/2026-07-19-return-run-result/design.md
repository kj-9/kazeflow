## Context

M1 supplied four dependency-free model layers: `plan` validates and orders selected
work, `results` validates terminal snapshots, and `events` validates passive lifecycle
records. The current executor predates them. It accepts an unvalidated graph, uses a
one-shot ready set, treats falsey keys as unpartitioned, stops scheduling after a
failure, retains mutable `Flow.asset_outputs` between runs, and unconditionally creates
Rich presentation objects.

This is M2, the Roadmap's single-owner executor integration. It preserves the
script-first asset decorator and direct callable use, returns model-layer values, and
keeps core limited to the Python standard library. It is intentionally before M3's
optional Rich adapter and M4's package-metadata cleanup.

## Goals / Non-Goals

**Goals:**

- Give every new flow an explicit direct-target tuple and exact registry source, while
  retaining a constrained legacy `Flow(graph)` migration path.
- Use one preflighted `FlowPlan` as deterministic work input and construct one terminal
  `RunResult` for each normally completed invocation.
- Drain ready work deterministically under `max_concurrency`, reaching terminal
  outcomes for all scheduled, blocked, and no-work attempts.
- Freeze partition argument shapes that preserve matching-key isolation and provide
  non-partitioned reducers with the only supported full partition map.
- Emit complete ordered events for normal completion and define external asyncio
  cancellation separately rather than pretending it is a completed flow.
- Remove automatic Rich presentation from execution and preserve a serial ownership
  boundary for the integration hotspot.

**Non-Goals:**

- Altering M1 model schemas; adding a public cancellation API, retries,
  `raise_on_failure`, persistence, a scheduler/daemon, remote execution, or a sandbox.
- Implementing a Rich renderer, text logger, package dependency removal, docs refresh,
  or storage adapter. Those are M3/M4/M5/M6 work.
- Preserving `Flow.asset_outputs` as an authoritative result API, automatic terminal
  presentation, manually authored execution graphs, or full-map inputs for a
  partitioned downstream.

## Decisions

### Explicit constructor, target ownership, and legacy graph migration

The exact public surface is:

```python
Flow(targets: list[str] | tuple[str, ...] | dict[str, set[str]], *,
     registry: AssetRegistry = default_registry)  # dict form is deprecated
Flow.plan(run_config: RunConfig | None = None) -> FlowPlan
Flow.run_async(run_config: RunConfig | None = None, *,
               event_consumer: ExecutionEventConsumer | None = None) -> RunResult
run(asset_names: list[str] | tuple[str, ...], run_config: RunConfig | None = None, *,
    registry: AssetRegistry = default_registry,
    event_consumer: ExecutionEventConsumer | None = None) -> RunResult
```

A `Flow` owns the canonical direct target tuple and the exact `AssetRegistry` supplied
at construction. `Flow.plan()` normalizes the existing `RunConfig` keys
(`max_concurrency`, `partition_keys`) into `PlanConfig` and calls
`build_flow_plan(self.targets, registry=self.registry)`. The module entry point creates
`Flow(asset_names, registry=registry)`. The registry is never implicitly substituted
after construction and a flow does not infer targets at run time.

`Flow(graph)` remains temporarily accepted only for source compatibility. It accepts a
`dict[str, set[str]]`, derives direct targets as graph keys that are not dependencies of
another graph key (in lexical order), retains a copy as deprecated `flow.graph`, and
uses the supplied/default registry as the source of truth. `plan()` SHALL reject this
form unless the graph's full closure and every direct dependency set exactly equal the
closure resolved from those direct targets in that registry. Thus a hand-authored graph
cannot be used as an alternate execution definition. New code MUST pass direct targets;
the legacy form and `flow.graph` are slated for a later breaking removal.

All invalid-definition/configuration errors occur before a flow-start event or asset
invocation. Ignored former `None` returns remain source-compatible.

Alternative considered: keep `Flow(graph)` as the primary execution definition.
Rejected because it duplicates registry metadata, makes target ownership ambiguous, and
would allow planning/execution drift.

### Per-run state and deterministic completion accounting

Each `run_async` invocation creates a fresh run id, timing state, output store, attempt
state, pending-ready queue, event sequence, and result builders. `Flow.asset_outputs`
is cleared at run start and may exist only as a deprecated compatibility mirror for the
current successful invocation; it never feeds dependency resolution or a later run.

The executor represents each `TaskPlan` as one unpartitioned attempt, its ordered
partition attempts, or the zero-attempt `no_partition_keys` task. A stable pending
queue drains in plan/partition order, starts at most the validated concurrency bound,
records every completion, unlocks newly eligible work, and continues until all planned
tasks are terminal. It does not rely on a one-shot `TopologicalSorter.get_ready()`.
Task finalization waits for all selected attempts and emits exactly one aggregate result
in plan order; the terminal `RunResult` contains every task in that order.

Alternative considered: preserve `TopologicalSorter` as scheduler state. Rejected
because task-level readiness cannot express partition-granular outcomes and the current
use discards ready work above capacity.

### Dependency eligibility and exact argument shapes

The executor uses `AttemptReference.partition_key_present`, never truthiness, to
distinguish unpartitioned work. A partitioned-to-partitioned dependency evaluates only
the matching key. If that matching upstream attempt failed, was cancelled, or is
dependency-blocked, the downstream matching attempt is dependency-blocked with that
attempt reference; a failed sibling key is irrelevant to any other downstream key.

For each declared asset dependency parameter, a runnable partitioned downstream gets:

- an unpartitioned upstream's raw successful output; or
- a partitioned upstream's one-entry mapping `{current_key: output}`.

With multiple partitioned upstream dependencies, each named parameter gets its own
one-entry mapping. All required matching upstream attempts must be successful. A
failed sibling key is never inserted into a mapping and never blocks a matching
successful key.

A non-partitioned downstream is the only full-map reducer. For every partitioned
dependency it receives an ordered mapping of all selected keys to outputs only after
that upstream aggregate is `success`; if the aggregate is failed, cancelled, or
dependency-blocked, the reducer is blocked rather than receiving a partial mapping.
It receives `{}` after `skipped(no_partition_keys)`. Unpartitioned upstreams continue
to contribute their raw output. This intentionally changes current partitioned
downstream behavior: such assets migrate from assuming a whole map to reading
`dependency[context.partition_key]`; cross-key aggregation moves to a non-partitioned
reducer.

Independent branches and already-running attempts continue after asset failure. Asset
exceptions create `FailureInfo` and an in-memory exception on the failed attempt; they
do not escape from a normally completed run.

### Results, normal events, and external cancellation

The executor records aware UTC lifecycle timestamps and monotonic durations, then
constructs only terminal M1 result values. At the same transitions it synchronously
dispatches an optional `ExecutionEventConsumer` (flow/task/attempt starts and finishes)
or a no-op consumer. One run-local counter provides consecutive observed sequence
numbers; independent concurrent finishes may appear in either observed order. For a
normally completed invocation the executor validates its complete stream before
returning the result. A selected consumer's own exception propagates and is not
misrepresented as an asset failure.

M2 has no public cancellation API. External cancellation of the task awaiting
`Flow.run_async()` is a distinct exceptional exit: it stops scheduling pending work,
requests cancellation of all executor-created pending/running asyncio tasks, and
propagates `asyncio.CancelledError`. Coroutine assets receive ordinary asyncio
cancellation. A synchronous asset already submitted through `run_in_executor` cannot
be force-stopped by the standard library; its awaitable wrapper is cancelled and any
later output/exception is discarded, never used as dependency input or result data.

External cancellation creates no synthetic `RunResult`, `FailureInfo`, or terminal
event. A consumer can observe only the prefix already emitted; there is no
flow-finished guarantee and no complete-stream validation. Exactly-once terminal-result
and complete-event guarantees apply only to normal completion, including completed
asset failures. Treating external cancellation as a result was rejected because it
would require public cancellation semantics and reliable accounting for non-cancellable
thread-pool work.

### Presentation removal and serial ownership

Core removes automatic flow-tree/Rich renderer/progress/logger calls. `AssetContext`
gets a silent standard-library logger without global configuration. The decorator,
dependency inference, direct asset calls, and `RunConfig` keys stay available. Module
`run()` detects a current-thread event loop before planning and raises `RuntimeError`
that names `run_async()`; otherwise it delegates to the same async semantics.

One executor integration owner exclusively changes `src/kazeflow/flow.py`,
`src/kazeflow/assets.py`, `src/kazeflow/__init__.py` only if needed for current entry
points, `tests/test_execution.py`, and targeted `tests/test_flow.py`. This owner adapts
to but does not modify M1 `plan.py`, `results.py`, or `events.py`. TUI/logger,
packaging, persistence, docs, and OpenSpec sync/archive are outside the write scope.

## Risks / Trade-offs

- [Existing callers use `Flow(graph)`] → retain a checked deprecation shim, but require
  it to match the owned registry plan exactly.
- [Async completion order varies] → require deterministic plan/result order and only
  consecutive observed event ordering for independent concurrency.
- [Partition input behavior regresses] → test one-entry maps, failed sibling isolation,
  multiple upstream mappings, reducer full-map/blocking, and empty reducers separately.
- [External cancellation leaves a worker thread active] → propagate cancellation, stop
  scheduling, discard wrapper outcome, and make no false terminal guarantee.
- [Automatic Rich removal surprises users] → treat it as a deliberate breaking
  presentation migration; M3 will provide an explicit optional renderer.

## Migration Plan

1. Replace the manager path with plan-driven per-run state and add completion tests
   before removing TUI calls.
2. New code constructs `Flow` with direct target sequences and an explicit registry
   when not using `default_registry`. Legacy `Flow(graph)` remains only when it exactly
   represents that registry closure. Keep `RunConfig` keys and return `RunResult`.
3. Update partitioned downstream assets to read the matching one-entry map using
   `context.partition_key`; move full-map aggregation into non-partitioned reducers.
4. Remove automatic Rich presentation and use the no-op observer/logger path. M3 owns
   any explicit renderer migration.
5. Verify preflight, completion, partition arguments, cancellation, events, all tests,
   and strict OpenSpec validation. Rollback is normal source rollback; no persistent
   state or schema is involved.

## Open Questions

- None for M2. Richer consumer isolation/dispatch and a public cancellation feature are
  explicitly deferred.
