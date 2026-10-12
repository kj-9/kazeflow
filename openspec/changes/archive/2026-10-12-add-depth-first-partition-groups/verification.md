# Verification: add-depth-first-partition-groups

## Completeness

7/7 tasks and three new grouping requirements are complete. Three existing capability contracts are updated.

## Correctness

Planning tests cover immutable declaration snapshots, invalid names/limits, disjoint direct chains, known same-domain partitioned members, partial selection and leave/re-enter dependency rejection, wholly unselected omission, deterministic selected group order and no body/adapter work.

Core tests cover selected-key ordinal depth-first progression, cap2 while finish order differs, slot release after failed upstream and blocked downstream, unpartitioned prerequisite/aggregate barriers, reusable materialized stages and fresh state per run. Independent edge tests cover retry/cooldown slot retention with unrelated global/resource progress, circuit cause/terminal exactly-once, external cancellation and observer failure without synthetic finishes, empty selection and unchanged ungrouped order. Task/attempt result order and complete event sequence validation are preserved.

CLI/schema tests cover declared grouped Flow entry, text metadata, explicit empty groups and plan5/schema collection7 with historical v1–v6 unchanged. Run/history4, portable4/event4/DB1 remain unchanged.

## Coherence

One executor owner integrates candidate admission without changing dependency/input matching or finalization. Group slots persist across waits; invocation admission stays governed by existing policies. Docs distinguish these capacities and exclude memory eviction, arbitrary grouped DAGs and cross-run guarantees.

## Validation

- Planning: 55 focused tests passed.
- Independent edge evidence: 5 tests passed; core grouping: 5 tests passed.
- CLI/schema: 47 tests passed.
- `make test`: 490 passed on Python3.13.
- `make ci-check`: format61 files, Ruff and ty passed.
- `make docs-check`: strict build and13-page/search-index check passed.
- Runnable group example passed.
- Offline wheel build, metadata and isolated core install smoke passed on Python3.12. All15 packaged Python modules match source. Supported3.10–3.13 CI matrix retained;3.10/3.11 not run locally.
- OpenSpec doctor and strict validation passed.

No unresolved critical issues or warnings. Ready for archive.
