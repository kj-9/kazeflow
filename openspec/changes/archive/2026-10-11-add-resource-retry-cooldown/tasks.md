## 1. Shared contracts (model/storage owner; freeze before executor integration)

- [x] 1.1 Implement InvocationResult, immutable history validation and explicit result record v2 while preserving default v1; verify result tests and privacy/order evidence.
- [x] 1.2 Add numbered invocation events, exact payload/causal validation and event record v2; verify legacy and retry event tests, observer behavior.
- [x] 1.3 Save portable record v2 independently of SQLite layout v1 and read mixed historical records; verify storage/migration tests and unchanged old rows.

## 2. Policy and executor (root; serial integration)

- [x] 2.1 Add Fail/Retry classifier policy, selected immutable planning metadata and context invocation number; verify validation/preflight/public API tests.
- [x] 2.2 Integrate numbered retry scheduling, renewed permits and shared cooldown after model freeze; verify budget, independent progress, rate/concurrency and overlapping cooldown tests.
- [x] 2.3 Preserve materialization failure boundaries, prepared identity, terminal once semantics and cancellation; verify hit/miss/commit/load/observer/cancellation tests.

## 3. Independent behavior tests (tests owner)

- [x] 3.1 Add retry/cooldown behavior tests and runnable example; verify transient recovery, permanent/exhausted failures, falsey keys, classifier failure, fresh-run state and no sleeping-slot ownership.

## 4. Projection and user migration (projection owner)

- [x] 4.1 Update CLI plan v4 and run/history v2 with schemas/goldens; verify current documents plus preserved historical artifacts and mixed stored record versions.
- [x] 4.2 Document policies, versions, numbered history, failure and side-effect boundaries; verify executable example and strict Docs build.
- [x] 4.3 Update installed-wheel smoke and metadata checks; verify build and clean core install without required dependencies.

## 5. Final integration (root; serial)

- [x] 5.1 Review scenario coverage and shared contracts, run make test and make ci-check; verify no unresolved critical findings and retain Python 3.10–3.13 CI matrix.
- [x] 5.2 Run openspec doctor and strict validation, sync/archive and update roadmap; verify living specs, no active change and clean diff formatting. Retain uncommitted changes.
