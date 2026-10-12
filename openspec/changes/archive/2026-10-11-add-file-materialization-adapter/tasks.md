## 1. Adapter implementation (adapter owner)

- [x] 1.1 Add explicit codec API, strict JSON codec, and typed canonical identity; verify no-write construction/planning, falsey keys, unsupported outputs/keys, and codec errors.
- [x] 1.2 Implement portable manifest and resolve/load integrity checks; verify fresh process, copied root, identity changes, corruption, schema/codec mismatches, and retained old records.
- [x] 1.3 Implement immutable generation and atomic reference publication; verify injected pre-publication failures and concurrent writers/readers.

## 2. Independent behavior evidence (tests owner)

- [x] 2.1 Add adapter tests for codec/identity/manifest failure semantics; verify focused pytest suite.
- [x] 2.2 Add executor restart and subprocess reuse tests, fault injection and concurrency evidence; verify upstream reuse and failed downstream retry without changing core contracts.

## 3. User documentation and package (docs owner)

- [x] 3.1 Add explicit adapter API, manifest/lifecycle/privacy limits and runnable restart example; update navigation and current no-store claim, verify example in fresh processes and make docs-check.
- [x] 3.2 Extend clean installed wheel smoke for adapter import/commit/load and stdlib-only behavior; verify wheel build, metadata, and core-only smoke.

## 4. Root integration (serial)

- [x] 4.1 Review requirements/scenarios, failure/concurrency edge cases and compatibility; resolve findings and verify no critical gaps.
- [x] 4.2 Run make test, make ci-check, openspec doctor and strict validation; verify all gates pass and Python 3.10–3.13 CI matrix remains.
- [x] 4.3 Sync/archive serially and update roadmap; verify living specs, no active change and strict validation, retain uncommitted work.
