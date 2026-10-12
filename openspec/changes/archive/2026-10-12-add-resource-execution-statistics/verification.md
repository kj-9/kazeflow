# Verification: add-resource-execution-statistics

## Completeness

6/6 tasks complete. Three statistics requirements and modified exact RunResult contract are covered.

## Correctness

Model/result tests cover immutable validated counts/durations, completed-operation invariants, resource tuple uniqueness/types, exact lookup errors/identity, old positional construction and unchanged portable versions1–4.

Runtime tests cover actual operations without invocation history, recovered and exhausted retries, resolve/load/commit failures, materialized zero counters, selected/unselected resources, plan resource ordering, fresh/empty run counters and unchanged records. Independent wait tests cover repeated concurrency denial counted once, rate admission, retry/shared cooldown, circuit-skipped pending wait, dependency/global-only queue exclusion and unrelated progress.

Actual body-plus-commit duration is measured monotonically. Observed waiting is deliberately not exclusive per-cause or entire global/dependency delay. Sum durations can overlap; documentation discloses these boundaries. Existing scheduling and event/cancellation/failure tests remain passing.

## Coherence

Mutable accounting is run-local executor state only. Public snapshots are frozen, independent of optional invocation history, and omitted from all existing portable/CLI/event/SQLite projections. Selected resources with no started bodies remain zero. No observer or adapter operations added.

## Validation

- Model/result focused tests:81 passed.
- Runtime/wait evidence:14 passed.
- `make test`:578 passed on Python3.13.
- `make ci-check`:69 files formatted, Ruff/ty passed.
- `make docs-check`: strict build and13 selected pages/search index passed.
- Statistics example passed.
- Offline wheel build, metadata and isolated core install smoke passed on Python3.12. All16 packaged Python modules match current source. CI3.10–3.13 retained;3.10/3.11 not run locally.
- OpenSpec doctor/strict validation passed.
- Read-only Issue1/2 acceptance audit found no remaining required implementation item after this change; GitHub issues remain open pending authorized publication and CI.

No unresolved critical issues or warnings. Ready for archive.
