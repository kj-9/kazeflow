## Why

Issue #1 describes flows that call the same constrained external service from several assets and partitions. Today only a flow-wide `max_concurrency` exists, so limiting that service also needlessly serializes unrelated work. The latest issue design groups this concern under a flow-scoped `ResourcePolicy`; this change establishes its smallest useful, testable slice.

This is post-M17 work prompted by Issue #1, not part of the documentation-focused M18 milestone. It must preserve the project goal of inspectable, infrastructure-free execution.

## What Changes

- Let a flow register named, flow-scoped resources with positive `max_concurrency` limits and let assets declare which one they use.
- Include the resolved resource association and limit in `FlowPlan`; reject undefined resource references and invalid limits before any asset runs.
- Enforce both the existing global bound and each resource bound across all assets and partitions in one run, without constraining unrelated work.
- Show resource limits in human plan output and publish a versioned machine-readable plan document that includes them. Existing `kazeflow.plan` version 1 documents remain unchanged.
- Preserve existing behavior for flows that declare no resources. Add no required runtime dependency.

Scope is one resource name per asset and invocation-level concurrency only. Rate limiting, retry, cooldown, circuit breaker, cross-process coordination, and materialization (#2) are separate changes. A resource limit does not count individual API calls made inside one asset invocation or promise API quota compliance.

## Capabilities

### New Capabilities

- `flow-scoped-resource-concurrency`: Declaring, preflighting, and enforcing a shared in-run concurrency limit on named resources.

### Modified Capabilities

- `flow-planning`: Include validated resource associations and policies in immutable plans.
- `core-executor-integration`: Accept flow-owned resources and enforce the limits while draining planned attempts.
- `flow-plan-cli`: Display planned resource use and limits in text output.
- `cli-json-contracts`: Introduce a new version of the plan document for resource-aware plans without silently changing version 1.

## Impact

Public Python API adds an optional `resources` argument to `Flow` and an optional resource declaration to `@asset` / registry registration. Existing call sites remain valid. Planning models, executor scheduling, CLI plan projection, published JSON Schema/goldens, Docs, and tests change. The core continues to use only the Python standard library, and resource state exists only for one flow run; no daemon, database, or external coordinator is added.
