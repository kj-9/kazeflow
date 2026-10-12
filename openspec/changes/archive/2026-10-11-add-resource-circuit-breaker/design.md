## Context

See proposal for Issue #1 scope. Current classifier returns Fail or Retry, executor controls logical attempts, and references hide raw keys in portable records.

## Goals / Non-Goals

Extend the classifier without implicit heuristics. Keep run-local state and existing materialization/body boundary. No thresholds, probes, timer reopening, persistence of breaker state or query API.

## Decisions

- Public frozen zero-field `OpenCircuit()` in resources.py, exported from package. FailureAction adds this action; no policy or plan fields change. Root owns resources.py, __init__.py, flow.py.
- Executor map resource->first source AttemptReference. Open only for a validated body classifier decision. Mark source failed and emit its finish before skipping peers. Running futures include admitted body operations and preparation; do not cancel. No additional circuit event: failed source and attributed skips expose the causal relation.
- Admission precedence: dependency blocked first; unresolved dependencies wait; materialization preparation/load can run; open circuit then rejects prepared miss/nonmaterialized body before capacity/time admission. Retry waits do not wait out cooldown after opening.
- Skips use `SkipReason.RESOURCE_CIRCUIT_OPEN`, exactly one `blocked_by` source; prior invocations retained, including scheduled delay. No new fields. Attempt skip cannot use MIXED_BLOCKED. All-skipped partition aggregates use shared reason or MIXED_BLOCKED with concatenated blockers. Mixed success/skipped aggregates keep None/empty as before; their task event uses shared skipped reason or MIXED_BLOCKED with concatenated skip blockers.
- Portable result v3 shape equals v2, adds enum meanings. Zero-argument v1 remains; v1/v2 reject circuit/mixed reasons recursively. Event default v3 retains v2 fields with schema_version3; explicit old versions reject new reasons. Event sequence validator requires source earlier failed logical finish (exact in-memory reference), allows pending no-start and retry-history skips.
- Store writes record3, reads1/2/3 without layout migration. CLI run/history envelope3; plan4 and other1 unchanged. Publish schema collection v5 using per-document versions and preserve previous artifacts. Record version conditions must select shapes without ambiguous oneOf for empty results. Schema v1/v2 record definitions must forbid new enums.
- Models owner: results.py, events.py, sqlite_store.py and dedicated existing result/event/storage tests. Projection owner: cli.py, v5 schemas/goldens, CLI/docs tests, user Docs/release notes, smoke script. Behavior tests owner: test_resource_circuit.py and example. Root owns OpenSpec/roadmap/integration and final tests. No simultaneous hotspot edits.

## Risks / Trade-offs

- Concurrent failures → first observed source is stable; already-admitted work can still fail/succeed and side effects remain application-owned.
- Portable partition references omit keys → source identity is exact in memory; portable records preserve existing privacy and can be less specific.
- Enum expansion → explicit versions prevent older consumers silently interpreting new reasons.

## Migration Plan

Ship new result/event3 and CLI history3 documentation alongside retained old schemas. Store layout1 and old rows unchanged. Core remains stdlib only. Rollback can read old records but rejects new record3 rows without rewriting them.
