# Verification: add-materialization-invalidation

## Completeness

7/7 tasks complete. The three new requirements and modified file identity contract are implemented with dedicated scenario evidence.

## Correctness

- Optional exported protocol leaves existing Materialization resolve/load/commit contract unchanged. Independent tests cover structural asynchronous invalidation and public exports.
- File schema2 refs/manifests contain matching epochs; legacy1 remains readable without a ledger. Strict ledger schema1 requires identity, UUID epoch and non-empty reason.
- Independent tests cover exact typed falsey identity and version/input/domain/asset isolation, repeated revocation, absent identity revocation, copied roots with stale refs/objects and fresh-process rebuild, old record rejection and atomic publication failure preservation.
- Live flow evidence verifies invalidated upstream executes and its new committed ID changes downstream input identity; later reuse avoids execution.
- Adapter regression tests cover legacy schemas, available/incompatible/corrupt inspection without codec calls, null ledger epoch, malformed storage and dangling ledger/ref files.
- Review warning about a dangling invalidations directory was fixed and independently tested: resolve/load/commit/invalidate fail closed instead of assuming absence.
- Error keyword reason preserves existing Exception args, including zero and multiple arguments.
- Inspection is immutable and its constructor validates status/record/reason invariants. Invalid arguments fail before writes; read-only operations do not mutate storage.

## Coherence

Executor, portable result/event/CLI/SQLite versions and runtime dependencies are unchanged. No automatic invalidation, cascade or deletion. Persistent exact-identity ledger and quiescent invalidation follow design. Copies must retain ledger; full rollback/deletion or malicious edits are outside guarantee. Documentation and API reference cover this boundary and schema2 migration; protocol reference is in navigation.

## Validation

- Dedicated invalidation and file-adapter tests: 61 passed.
- `make test`: 461 passed on Python 3.13.
- `make ci-check`: 56 files formatted, Ruff and ty passed.
- `make docs-check`: strict build plus 13 selected pages/search index passed.
- Example ran successfully twice using the same root.
- `uv build --offline`, wheel metadata and isolated core install smoke passed on supported Homebrew Python 3.12. All 14 packaged Python modules match current source.
- Supported Python3.10–3.13 CI matrix retained; 3.10/3.11 not run locally.
- OpenSpec doctor and strict validation passed.

No unresolved critical issues or warnings. Ready for archive.
