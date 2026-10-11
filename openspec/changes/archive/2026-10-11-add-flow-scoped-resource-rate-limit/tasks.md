## 1. Public policy and planning (single core owner)

- [x] 1.1 Add frozen RateLimit and optional ResourcePolicy.rate_limit in resources.py and __init__.py; verify positive/finite validation, malformed policy preflight, and existing public API compatibility tests.
- [x] 1.2 Add immutable selected rate metadata in plan.py; verify deterministic plans, explicit absence, unused resources, and zero adapter/body calls.
- [x] 1.3 Implement internal sliding-window admission arithmetic with controllable monotonic time; verify exact expiry boundaries, failed-start retention, independent resource state, and repeated-run reset in focused tests.

## 2. Executor phases (same core owner; serial integration)

- [x] 2.1 Separate materialization preparation from waiting-body and invocation/commit phases in flow.py; verify existing materialization/failure/cancellation tests and exactly one lifecycle start/finish per normally completed attempt.
- [x] 2.2 Integrate rate and concurrency admission with earliest-deadline waiting in flow.py; verify shared limits across assets/keys, unrelated progress, no-running-future waits, no busy polling, and deterministic result order.
- [x] 2.3 Verify hit/miss materialization permits, commit-before-downstream, independent failure continuation, falsey keys, and cancellation while waiting; resolve findings without altering portable result/event shapes.

## 3. CLI contract and documentation (projection owner after model freeze)

- [x] 3.1 Update cli.py text summary and plan JSON v3 projection; verify rate/no-rate/no-resource plans, stderr separation, deterministic ordering, and unchanged other document versions.
- [x] 3.2 Publish v3 schema/goldens while preserving v1/v2 artifacts; verify schema validation, golden compatibility, and wheel CLI smoke tests.
- [x] 3.3 Update Python/CLI guides, migration notes, and invocation-vs-request/run-local explanation; verify executable examples and make docs-check.

## 4. Final integration and archive (root; serial)

- [x] 4.1 Review all requirements/scenarios against code and focused evidence; verify no unresolved critical correctness or compatibility findings.
- [x] 4.2 Run make test, make ci-check, wheel build/metadata and clean core install checks, openspec doctor, and openspec validate --all --strict; verify all required checks pass and retain Python 3.10–3.13 CI coverage.
- [x] 4.3 Sync/archive serially after implementation verification; verify living specs, openspec list, and strict validation, and retain the user's separate authorization requirement for commit/push and GitHub issue writes.
