## Context

See `proposal.md` for the Issue #1 motivation. `Asset` currently owns dependency and partition metadata; `Flow` owns targets and a registry; `build_flow_plan()` makes an immutable `FlowPlan`; `_Executor._drain()` uses one global count and `_next_action()` chooses ready attempts. CLI text and typed JSON are projections of the plan, and the published `kazeflow.plan` v1 schema is a compatibility contract. The same registry asset can be reused by more than one flow, so resource policy values belong to the flow, not the registry.

## Goals / Non-Goals

**Goals:** Bound actual concurrent invocations using one named resource within a run, without reserving a global slot while waiting. Make the constraint visible before execution and preserve terminal/ordering guarantees.

**Non-Goals:** A permit per HTTP request, time-based rate limiting, retries, failure classification, circuit state, cross-run/process coordination, or #2 materialization. This design does not make an arbitrary Python asset safe to import or run.

## Decisions

### Public definitions and validation

Add a frozen `ResourcePolicy(max_concurrency: int)` in a small core module, exported from `kazeflow`. `Flow(..., resources: Mapping[str, ResourcePolicy] | None = None)` copies the mapping for definition ownership. `@asset(..., resource: str | None = None)` and `AssetRegistry.register()` retain one optional resource name as asset metadata. The existing module-level `run()` remains unchanged; callers needing resources use `Flow`.

`Flow.plan()` passes its owned mapping to `build_flow_plan(..., resources=...)`. Planning validates names, types, positive non-boolean limits, and every selected asset reference. An unselected asset's undeclared resource does not invalidate the selected closure. Store only selected resources in an immutable, name-sorted `ResourcePlan` tuple on `FlowPlan`; add optional `resource_name` to `TaskPlan`. No mutable semaphore or permit object enters the plan. This keeps repeated plans deterministic and side-effect free.

Alternative rejected: a process-global resource registry. It would couple independent runs and make inspection depend on ambient mutable state.

### Scheduling and failure semantics

Each `_Executor` owns active-invocation counts by resource name. `_next_action()` skips a ready attempt whose resource has no capacity and continues looking for other ready attempts. `_start_attempt()` increments the count immediately before scheduling. Completion decrements it exactly once on both success and ordinary asset failure; this follows the existing terminalization path. Resource-waiting attempts are not launched as tasks, emit no start event, and do not use a global slot. When only saturated attempts remain, `_drain()` waits for a running invocation to finish and rechecks eligibility. Existing deterministic plan/selection order remains the tie-breaker among eligible attempts.

External cancellation follows the current contract: `_cancel_running()` cancels executor-created wrappers and propagates `CancelledError` without a synthetic result. Executor-local counts are discarded. A synchronous function already running in a thread may physically continue after cancellation; this feature makes no limit guarantee across the cancelled run and any separately started run.

Alternative rejected: per-resource semaphores acquired inside already-scheduled tasks. Those waiting tasks would consume global slots and could starve unrelated work.

### CLI compatibility

Text plan output adds a compact selected-resource summary and per-task association; its exact byte layout remains non-normative. Machine-readable `kazeflow.plan` becomes schema version 2 for every plan, with name-sorted `resources` and a nullable per-task `resource`. Retain v1 schema/goldens for consumers of older wheels; add v2 schema/goldens and a migration note. Other CLI document versions and portable run-record schema are unchanged. The plan's resource name is public definition metadata, not a raw partition key; Docs still warn that application-chosen names can reveal information.

Alternative rejected: silently adding fields to version 1. Version 1's published shape and golden fixtures are already normative.

## Risks / Trade-offs

- [Asset makes several API calls in one invocation] → Document that the limit counts invocations, not requests; defer rate limiting to the next Issue #1 change.
- [Busy or deadlocked scheduling when all ready attempts are resource-blocked] → Select unrelated work, wait only while an invocation is running, and test saturation/failure/cancellation cases.
- [Sync asset continues after external cancellation] → Keep the existing cancellation caveat; no cross-run guarantee.
- [Plan JSON migration breaks version-1-only consumers] → Bump only `kazeflow.plan` to v2, retain v1 artifacts, and publish migration guidance.
- [Scope creep toward resource orchestration] → Keep policy to an in-memory positive concurrency bound and no external state.

## Migration Plan

Existing flows without resource declarations continue to run identically; the new Python arguments are optional. Consumers of plan JSON must accept `kazeflow.plan` v2 and read the new resource fields or reject an unknown schema version explicitly. Keep v1 schema files for historical validation. Rollback of code reverts the emitted plan document to v1; no stored data migration is needed because resource state is run-local.

## Implementation ownership

One Luna agent owns `src/kazeflow/assets.py`, `flow.py`, `plan.py`, `__init__.py`, the new policy module, and focused core tests in the contract-first wave; these are shared hotspots and must not be edited concurrently. Once the Python plan shape is fixed, a separate Luna agent may own `src/kazeflow/cli.py`, CLI schemas/goldens, CLI tests, and user Docs. Sol reviews the public API, concurrency invariants, schema migration, and final verification before integration. Neither agent commits or pushes.
