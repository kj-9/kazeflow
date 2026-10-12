## Context

Current executor already owns resource policy/admission state and distinguishes preparation, actual body invocation and terminal outcomes. Invocation history is optional, so aggregate statistics cannot be derived from it reliably. See proposal.

## Goals / Non-Goals

Add passive run-local live statistics without new events or changing portable records. No scheduler/telemetry backend, persistence, CLI schema changes or automatic analysis.

## Decisions

- New resource_statistics.py defines frozen slots ResourceStatistics(name:str, invocations:int=0, succeeded:int=0, failed:int=0, retries:int=0, wait_count:int=0, wait_duration:timedelta=timedelta(), invocation_duration:timedelta=timedelta()). Validate counts/durations and completed-operation invariants. Export from package root.
- Append RunResult.resources:tuple[ResourceStatistics,...]=() after tasks; validate tuple/types/unique names. Add exact lookup resource_statistics(name) with TypeError nonstr/KeyError missing. All existing to_record versions omit the field; SQLite/CLI/event shapes unchanged. No reconstructing stats from lossy historical records.
- Executor owns mutable counters per selected plan resource and two monotonic fields per attempt (actual operation start and observed wait start). _invoke_body records actual body start and retries based on invocation_number>1; _finish_running_attempt closes completed operation before classification/final events. Preparation outcome and resolve/load failures have no operation start. Commit remains part of operation duration/failure. External cancellation/observer failures still yield no completed result.
- _next_action marks observed waits only on otherwise eligible body work denied by resource concurrency/rate/retry/cooldown. It does not scan globally blocked/dependency-blocked work for metrics. Repeat denial does not add episodes. Actual body start closes wait; _finish_blocked closes an existing wait on terminal skip. Sums may overlap and include subsequent scheduler/global delay once a wait episode began; docs must disclose these bounds, not label duration exclusive per-cause waiting.
- Fresh executor builds frozen summaries in plan resource order when RunResult is constructed. Resource None has no counters; unselected policies omitted. Selected materialization-only/no-work resources remain zero.

## Ownership

Model agent: resource_statistics.py, results.py, tests/test_resource_statistics_models.py, tests/test_results.py.
Root: flow.py, __init__.py, tests/test_resource_statistics.py, tests/test_resource_statistics_waits.py (independent test owner), tests/test_public_api.py, OpenSpec/ROADMAP/final integration/package/commit.
Docs agent: docs/user/api/plans-results.md, docs/user/results.md, docs/user/partitions.md, docs/release-notes.md, new examples/resource_statistics.py, scripts/smoke_wheel_install.py.

## Risks / Trade-offs

Ambiguous wait attribution → explicitly observed resource-admission intervals, no global-only/dependency measurement and no exclusive cause claims.
Overlapping durations → documented summed durations instead of wall-clock totals.
Optional history absent → instrument operations directly; avoid deriving counts from attempts.

## Migration Plan

Additive live model/query/field only, appended default preserves old callers. No portable/event/CLI/database changes; stdlib-only core and supported3.10–3.13 CI retained.
