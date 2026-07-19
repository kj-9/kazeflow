## 1. M0 Contract Artifacts

- [x] 1.1 Create the M0 proposal identifying `execution-contracts` as the new capability, its compatibility impact, and its core/non-goal boundaries. Owner: M0 spec owner; files: `openspec/changes/define-execution-contracts/**` only.
- [x] 1.2 Define externally observable lifecycle, failure, timestamp/duration, partition, runtime-output/record, presentation, and event-loop requirements with testable scenarios. Owner: M0 spec owner; files: `openspec/changes/define-execution-contracts/specs/execution-contracts/spec.md`.
- [x] 1.3 Define aggregate partition status and key-granular dependency propagation, including the distinction between `no_partition_keys` and dependency-blocking skips. Owner: M0 spec owner; files: `openspec/changes/define-execution-contracts/specs/execution-contracts/spec.md`.
- [x] 1.4 Record public API compatibility, failure semantics, M1/M2/M3 ownership, scheduler/order/rerun correctness gaps, and the M3/M4 no-op-observer target in the design. Owner: M0 spec owner; files: `openspec/changes/define-execution-contracts/design.md`.

## 2. M0 Verification

- [x] 2.1 Run `openspec doctor` and `openspec validate --all --strict`; correct only M0 artifacts until strict validation succeeds.
- [x] 2.2 Run the unchanged baseline with `UV_CACHE_DIR=/private/tmp/kazeflow-uv-cache uv run --frozen pytest -q` and confirm 11 tests pass without changing `uv.lock`.
- [x] 2.3 Record the current `uv run --locked` pre-release-mode/lock-policy mismatch as outside M0 and avoid updating `uv.lock` as part of this specification-only change.
- [x] 2.4 Check the M0 artifact diff for whitespace errors and confirm no files outside `openspec/changes/define-execution-contracts/**` were edited by this change.

## 3. Serial Completion

- [x] 3.1 Obtain approval for the M0 execution contract and resolve any remaining review comments within this change's owned artifacts.
- [x] 3.2 Archive `define-execution-contracts` and sync its capability spec serially before starting M1 implementation.
