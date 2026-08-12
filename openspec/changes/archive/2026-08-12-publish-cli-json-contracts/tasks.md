## 1. JSON projection and stream integrity

- [x] 1.1 Add a shared typed-envelope projection in `src/kazeflow/cli.py` and migrate assets, partitions, plan, completed run, and history JSON output to their documented version-1 document types.
- [x] 1.2 Route ordinary `sys.stdout` writes from entry loading, explicit factories, and approved asset execution to stderr in JSON mode without changing text, Mermaid, or DOT behavior.
- [x] 1.3 Emit a typed declined-run document on interactive JSON decline and preserve the documented exit/document matrix for completed failures and CLI errors.
- [x] 1.4 Extend `tests/test_cli.py` with all document types, noisy entry/factory/asset cases, decline, adapter failure, version separation, portable-data boundaries, and deterministic ordering.

## 2. Published schemas and compatibility evidence

- [x] 2.1 Check in strict Draft 2020-12 schemas for every version-1 CLI document type, with shared portable-record definitions and distinct CLI, record, and store version fields.
- [x] 2.2 Check in representative normalized golden documents for every command outcome and add contract tests that validate both goldens and live CLI output against the published schemas.
- [x] 2.3 Add `jsonschema` only to the development dependency group, update the lockfile, and verify that base wheel metadata still has no required third-party runtime dependencies.
- [x] 2.4 Extend installed-wheel smoke coverage to assert typed JSON envelopes and representative schema-compatible data without requiring schema validation at runtime.

## 3. Hosted automation documentation

- [x] 3.1 Publish a command-indexed JSON automation reference with the common envelope, complete version-1 field shapes, schema links, and representative outputs.
- [x] 3.2 Document stdout/stderr routing, the exit/document matrix, deterministic array semantics, the structural privacy boundary, and trusted-Python limitations.
- [x] 3.3 Publish the alpha compatibility and migration policy, including separate CLI-document, portable-record, SQLite-store, and package versions.

## 4. Integration and release evidence

- [x] 4.1 Run focused CLI/schema/documentation tests, `make test`, `make ci-check`, `make docs-check`, and strict OpenSpec validation.
- [x] 4.2 Build the distribution and run core-only plus optional-TUI wheel smoke tests across the supported packaging checks.
- [x] 4.3 Perform an adversarial implementation review, resolve contract or regression findings, and record verification evidence.
- [x] 4.4 Mark tasks complete, sync/archive the OpenSpec change serially, update M17 roadmap status, and commit/push the completed milestone in reviewable units.
