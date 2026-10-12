## 1. Shared models (model owner)

- [x] 1.1 Add circuit/mixed skip validation and result record v3; verify result model/version tests.
- [x] 1.2 Add event v3 and causal circuit skip validation; verify event payload/sequence tests.
- [x] 1.3 Save record v3 and read mixed v1/v2/v3 rows without migration; verify SQLite tests.

## 2. Executor (root, serial)

- [x] 2.1 Add OpenCircuit classifier action and run-local admission behavior; verify source failure, permit-free skips and existing retry tests.
- [x] 2.2 Integrate materialization/running-work/aggregate boundaries; verify behavior suite and independent review.

## 3. Behavior evidence (test owner)

- [x] 3.1 Add dedicated tests and example for first source, falsey keys, retries, fresh runs, dependencies, hits/misses and concurrent completion; verify focused suite and example.

## 4. Projection and migration (projection owner)

- [x] 4.1 Update run/history v3 and schema collection v5 with historical compatibility tests; verify CLI/schema tests.
- [x] 4.2 Document public action, failure/side-effect boundaries and version migration; verify strict Docs build.
- [x] 4.3 Extend wheel smoke; verify offline build, metadata and isolated core install.

## 5. Final integration (root)

- [x] 5.1 Review scenarios and run make test/make ci-check; retain supported CI matrix and verify no unresolved findings.
- [x] 5.2 Run doctor/strict validation, archive serially and update roadmap; verify living specs and diff check.
- [x] 5.3 Commit the verified change at a coherent granularity; verify status excludes local generated files.
