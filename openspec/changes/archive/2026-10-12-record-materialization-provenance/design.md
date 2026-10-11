## Context

See proposal. Results retain provenance today; event and portable projections omit it.

## Goals / Non-Goals

Add observation only, retaining existing executor state and failure semantics. No plan-time adapter I/O or artifact reconstruction.

## Decisions

- Portable result v4 adds resolution/materialization_id to each attempt; default remains v1, v1–v3 unchanged. Success resolution None is retained for manually constructed unknown results; ID requires a known resolution. materialized requires ID; executed permits ID for completed commit.
- ExecutionEvent appends optional fields, validated only on successful ATTEMPT_FINISHED; default projection advances to4. Explicit1–3 omit new fields. flow success emit copies outcome fields only. No extra events or changed sequencing.
- SQLite CURRENT_RECORD_SCHEMA_VERSION=4 with supported versions1–4 and DB schema1. Validate presence/types/semantic invariants and reject new fields on old records. Preserve historical bytes, no automatic migration.
- CLI record and history document versions advance4; plan4, others1. New schema collection/goldens v6 copies historical definitions and adds v4 variants. Older row versions remain distinct. Verbose live attempt text reports resolution and ID; history show already prints saved records. Compare retains existing status summary semantics (full records in left/right envelopes carry provenance).
- IDs are opaque, adapter-defined diagnostic metadata, possibly sensitive. Docs disclose new exposure; never serialize raw keys/output or implicitly load artifacts. No automatic redaction.

## Ownership

Root: results.py, events.py, flow.py, tests/test_provenance.py, tests/test_events.py, tests/test_results.py, OpenSpec/ROADMAP/final integration/commit.
Storage agent: sqlite_store.py, tests/test_sqlite_store.py and migrations tests, new dedicated storage tests.
CLI agent: cli.py, docs/user/schemas/cli/v6, tests/fixtures/cli-json/v6, tests/test_cli.py, tests/test_cli_json_contracts.py.
Docs agent: user docs excluding schema files, release notes, scripts/smoke_wheel_install.py, examples/materialization_resume.py. No other files edited concurrently.

## Risks / Trade-offs

Consumer migration → explicit new per-type versions, historical schemas unchanged.
Metadata leakage → document opaque IDs and unchanged raw-object exclusion.
Historical mixed rows → version-aware validators and tests; no fabricated provenance.

## Migration Plan

Ship additive model fields plus new projection versions and CLI schema collection. Existing explicit old projections and saved rows remain available. CI retains Python3.10–3.13 and stdlib core.
