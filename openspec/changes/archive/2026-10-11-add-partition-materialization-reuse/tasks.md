## 1. Integration boundary

- [x] 1.1 Isolate or complete the active Issue #1 implementation before editing shared core hotspots; verify `git status --short --branch` and `openspec list` show an unambiguous change boundary.

## 2. Materialization contract

- [x] 2.1 Add `src/kazeflow/materialization.py` with the context, committed record, and resolve/load/commit protocol; verify focused tests cover identity validation, invalid records, and sync/async adapter calls.
- [x] 2.2 Add optional adapter and version declaration to asset registration and public exports (single owner for `assets.py` and `__init__.py`); verify direct asset execution and existing public API tests still pass.

## 3. Planning and results

- [x] 3.1 Validate declaration completeness and direct-dependency identity eligibility in `plan.py`, and add the participation flag to `TaskPlan`; verify planning tests cover an eligible root, a dependency without materialization, and zero adapter I/O.
- [x] 3.2 Add in-memory resolution and committed materialization ID to `AttemptResult` in `results.py` without changing `to_record()`; verify result tests cover executed/materialized success, failure, and exact portable record keys.

## 4. Executor integration

- [x] 4.1 Integrate per-key resolve/validate/load and invoke/commit paths in `flow.py` (one executor owner); verify tests cover reused output, one-key hit/one-key miss, version/input-ID mismatch, and one terminal result per normally completed selected attempt.
- [x] 4.2 Preserve commit-before-downstream, independent-branch continuation, cancellation, and plan-order results; verify focused tests cover resolve/load/commit failure, falsey keys, dependency blocking, and cancellation without synthetic results.

## 5. Documentation and verification

- [x] 5.1 Document the explicit opt-in API, version-bump obligation, adapter trust boundary, absence of a bundled store, and unchanged CLI/record schema; verify documentation examples run and `make docs-check` passes.
- [x] 5.2 Run `uv run pytest`, `make ci-check`, `uv build`, and clean-environment install smoke tests; verify every command succeeds and Python 3.10–3.13 compatibility is maintained.
- [x] 5.3 Run `openspec doctor`, `openspec validate --all --strict`, and change verification; verify no unresolved critical findings remain.
- [x] 5.4 After implementation verification, sync this delta to living specs and archive it serially, without concurrent edits to the same capability specs; verify `openspec list` and strict validation show the intended final state.
