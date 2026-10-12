## Why

Issue #2 ROADMAP follow-up 8 needs a bounded set of partitions to progress through a sequence of expensive assets, avoiding starting every upstream partition before downstream work. Group limits must remain independent of global/resource invocation limits and retain reviewable plans.

## What Changes

- Add immutable PartitionGroup(assets, max_active_keys=1), Flow-owned declarations and planned selected groups.
- Schedule each group's active selected keys depth-first through a validated direct-dependency chain; release a key only when every member is terminal.
- Preserve defaults, matching-key dependency semantics, task/attempt result order, retry/circuit/materialization and external cancellation semantics.
- Expose groups in plan text and JSON plan v5/schema collection v7; run/history versions stay4.
- Scope: explicit disjoint linear groups. Non-goals: arbitrary grouped DAGs, automatic grouping, remote scheduling, aggregation or new CLI config switches.

## Capabilities

### New Capabilities
- `depth-first-partition-groups`: validated grouping and bounded depth-first admission.

### Modified Capabilities
- `flow-planning`: append selected groups to immutable plan metadata.
- `core-executor-integration`: Flow owns optional group declarations.
- `cli-json-contracts`: plan v5 projection with groups and historical schemas retained.

## Impact

Additive Python API/configuration and plan field. New CLI plan version requires consumer migration; result/event/history/store contracts unchanged. Core remains stdlib-only, and groups are metadata inspected without calling assets/adapters.
