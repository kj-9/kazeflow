## Context

M1 Workstream A introduces the first execution-before-inspection data model.
Today, `AssetRegistry.build_graph()` returns a closure with dependency sets and
`Flow` immediately couples topology, scheduling, output retention, and Rich
presentation.  There is no stable object an application or a later executor
can inspect without beginning that execution path.  The archived execution
contracts already fix the meaning of omitted, empty, falsey, and duplicate
partition keys; this change makes those rules visible at plan construction.

The roadmap assigns one owner to the new `plan.py` and `tests/test_plan.py` in
Wave 1.  It expressly defers shared `flow.py`, `assets.py`, and `__init__.py`
integration until the plan/result/event interfaces are fixed.

## Goals / Non-Goals

**Goals:**

- Provide a small, immutable, standard-library-only value model for inspected
  flow work: targets, direct dependencies, deterministic dependency-first
  order, selected partitions, and primary run configuration.
- Resolve an existing registry's selected dependency closure and validate it
  before any task is executed.
- Give later execution, rendering, and persistence work one structured source
  for plan metadata without coupling this change to those consumers.

**Non-Goals:**

- Changing `Flow`, `run()`, `run_async()`, the asset decorator, the registry,
  public package exports, or existing execution behavior.
- Scheduling work, producing `RunResult`, retaining outputs, applying failure
  or cancellation lifecycle behavior, or rendering a plan.
- Adding third-party dependencies, persistence, a daemon, scheduler, remote
  worker, control plane, or sandbox.

## Decisions

### Use frozen tuple-based value models

`PlanConfig`, `TaskPlan`, and `FlowPlan` will be frozen standard-library data
models.  Their collections are normalized to tuples so a built plan is safe to
hand to multiple readers and compares predictably.  `TaskPlan.partition_keys`
uses `None` only for an unpartitioned task; a tuple, including `()`, is the
partitioned selection.  This preserves the execution-contract distinction
between one unpartitioned attempt and explicit no-work.

Alternative considered: expose dicts/lists copied from `Flow` or retain
`Asset` objects and functions in the plan.  Those alternatives leave mutable
or executable state in the inspection model and would make renderer and
persistence boundaries unreliable.

### Make `build_flow_plan()` a metadata-only registry adapter

The new module will expose
`build_flow_plan(targets, *, config=None, registry=default_registry)`.  It may
read the existing registry's asset names, dependency metadata, and partitioned
marker, but it does not call asset functions or modify registry state.  Taking
an optional registry keeps the minimal API useful for isolated tests and
avoids changing `AssetRegistry` in this wave.

Alternative considered: add a planning method to `Flow` or change
`AssetRegistry.build_graph()`.  Both are shared integration surfaces whose
ownership is deferred by the roadmap; a separate adapter has a one-way
dependency from `plan.py` to existing metadata.

### Canonicalize target selection at the public boundary

The builder accepts targets only as a `list` or `tuple` of one or more non-empty
strings.  A lone `str` is rejected with `TypeError`, rather than implicitly
treated as one target; so are `bytes`, `bytearray`, sets, iterators, `None`, and
a list or tuple with non-string elements.  Empty names, an empty list or tuple,
duplicate names, and unknown names are value errors.  A valid direct selection
is normalized to `tuple(sorted(targets))` and stored as `FlowPlan.targets`;
this is deliberately separate from the dependency closure stored in
`FlowPlan.tasks`.

This means target input order has no semantic effect: equal registry metadata,
configuration, and selected target set produce equal frozen `FlowPlan` values.
The M0 execution contracts and roadmap require deterministic inspectable plans
but do not assign semantic meaning to target input order, so lexical
canonicalization is the narrowest stable rule.

Alternative considered: accept a lone string as shorthand, arbitrary
iterables, or preserve caller order.  Those choices make a string's character
iteration easy to miss, allow one-shot/unstable input sources, or give two
equivalent selections unequal plan values.

### Canonicalize topology rather than registry iteration

The builder will derive the selected transitive closure, sort direct dependency
names, and use lexical name ordering as the deterministic tie-breaker for
topological readiness.  It will reject cycles and missing definitions with
`ValueError`.  It validates targets and configuration before returning a plan.

Alternative considered: reuse the current graph's set order or the executor's
`TopologicalSorter` output directly.  Set iteration does not give the required
stable result, and an executor-owned solution would make inspection depend on
presentation and scheduling implementation.

### Normalize and validate partition configuration at the boundary

`PlanConfig` stores an immutable tuple selection.  Planning validates the
execution-contract rules: partitioned selected work needs an explicit
selection; `None` and unhashable keys are invalid; equality duplicates are
invalid even when their hashes collide or their values differ (`0` and
`False`); and falsey non-`None` keys are data.  `max_concurrency` is `None` or
a positive non-boolean integer.  An empty tuple is valid planning data; its
eventual `skipped(no_partition_keys)` result remains executor/result work.

Alternative considered: defer all validation until execution.  That would
allow users to inspect plans that cannot be run and violate the contract that
invalid configuration is detected before scheduling.

## Risks / Trade-offs

- [Later integrations need a richer run configuration] → Keep `PlanConfig`
  deliberately small and immutable; future fields require their own planned
  compatibility decision rather than mutable ad-hoc dictionaries.
- [The current registry stores dependency information using sets] → Canonical
  sorting in the plan builder protects plan stability without modifying shared
  registry code in this wave.
- [Assets can be registered with malformed or cyclic metadata] → Raise
  `ValueError` before a plan is returned and cover these cases in dedicated
  tests.
- [Callers may expect a lone asset name or input ordering to be preserved] →
  Reject lone strings explicitly and document `FlowPlan.targets` as the
  lexically canonical target set; a caller that needs one target passes
  `["name"]` or `("name",)`.
- [A no-op empty partition selection could be mistaken for an unpartitioned
  task] → Make the `None` versus `()` distinction a public model invariant and
  test it directly.

## Migration Plan

1. Add `src/kazeflow/plan.py` and `tests/test_plan.py` only; existing callers
   and execution paths remain unchanged.
2. Consumers that want pre-execution inspection opt in through
   `kazeflow.plan`; no top-level re-export or `Flow` method is added here.
3. In M2, the single executor owner will adapt `Flow` and run entry points to
   consume the agreed `FlowPlan`; that work will preserve existing asset
   decorator and direct-function-call usage according to the execution
   contracts.
4. Because this adds an unused module and no existing behavior changes,
   rollback consists of removing the new module and dedicated tests before
   integration; no data migration is required.

## Open Questions

None for this change.  The future mapping from legacy `run_config` dictionaries
to `PlanConfig`, public top-level re-export, and executor consumption are
intentionally owned by later integration work.
