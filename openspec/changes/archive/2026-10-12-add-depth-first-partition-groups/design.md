## Context

Existing executor selects task then attempt order; partitioned dependencies already gate on matching attempt results. Grouping changes admission only. See proposal.

## Goals / Non-Goals

Bound progressing keys while preserving existing result and dependency semantics. No automatic groups, memory eviction, CLI flags, arbitrary grouped DAGs or multi-run coordination.

## Decisions

- New groups.py defines frozen PartitionGroup(assets:tuple[str,...], max_active_keys:int=1), snapshots list/tuple assets; rejects text/non-sequence, less than2, duplicate/empty/nonstr names, nonpositive/bool limits. Export from package root. Flow(partition_groups=None) stores tuple and passes it to build_flow_plan(..., partition_groups=None).
- Append FlowPlan.partition_groups:tuple[PartitionGroup,...]=() after resources, preserving old positional signatures. Builder validates all declaration types, disjointness, known partitioned assets and direct predecessor chain. For selected closure, group wholly absent -> omit, partially present -> reject. Validate no external dependency node of a member has any transitive dependency on any group member (dependency-convex restriction avoids hidden unpartitioned barrier deadlocks). Sort selected groups by first asset plan position. All member selections already share canonical domain/keys through existing planner.
- Executor _GroupState contains declaration, active selection indices and next index. Map member task names to group; no raw-key hash/equality added for admission. At the first member's plan position, produce candidates for active indices in order and only their next unfinished member. Refresh slots by dropping indices whose every member is terminal, then fill to cap in selection order. Other members are skipped in ordinary candidate traversal. Outside tasks retain original plan/attempt admission order and dependency gating.
- Keep existing _eligibility/_blockers/_inputs per-key matching unchanged. A direct chain means later members await predecessor success or become blocked. Active group slots persist during retry/rate/cooldown/preparation; body/global capacity remains separate. Materialized hits complete stages without resource permits. Each run creates fresh group state. Keep finalization/event/result construction unchanged.
- Plan text prints groups; plan JSON version5 adds partition_groups with assets/max_active_keys. New schema collection/goldens v7 retains historical v1–v6 untouched. Run/history4, other types1, portable4/event4/DB1 unchanged. No new CLI group flags: entries declare Flow groups. Graph edge structure unchanged.

## Ownership

Planning agent: groups.py, plan.py, tests/test_partition_groups_plan.py, tests/test_plan.py if required.
Root: flow.py, __init__.py, tests/test_partition_groups.py, tests/test_partition_group_edges.py (independent test owner), tests/test_public_api.py, tests/test_documentation_contracts.py, OpenSpec/ROADMAP/review/package/commit.
CLI agent: cli.py, docs/user/schemas/cli/v7, tests/fixtures/cli-json/v7, tests/test_cli.py, tests/test_cli_json_contracts.py.
Docs agent: docs/user Markdown excluding schemas, docs/release-notes.md, scripts/smoke_wheel_install.py, new examples/partition_groups.py.

## Risks / Trade-offs

Wait holds key slot → explicit distinction from invocation capacity and deterministic evidence.
Partial selection/leave-reenter graph → reject at preflight; no guess/automatic regrouping.
Opaque keys → ordinal admission; existing normalization/equality contracts unchanged.
Plan consumers migrate → v5 with historical schema collection v6 retained.

## Migration Plan

Add opt-in Python declarations and plan field. No behavior change for ungrouped flows. Ship schema collection v7 and guidance. CI retains Python3.10–3.13 and zero runtime dependencies.
