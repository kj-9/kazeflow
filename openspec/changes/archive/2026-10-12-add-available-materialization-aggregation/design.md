## Context

Normal unpartitioned consumers require all selected partition inputs to succeed. That behavior must remain. File resolve already validates committed identity, revocation and integrity, with caller-owned raw contexts. See proposal.

## Goals / Non-Goals

Provide explicit selection/collection only. Aggregation uses ordinary Python after run result inspection. No executor reducer variant, root scanning, auto-resume or sink I/O in core.

## Decisions

- Add exported optional MaterializationCollection protocol in materialization.py: available(contexts: Sequence[MaterializationContext]) -> tuple[MaterializationRecord,...] | Awaitable[tuple[MaterializationRecord,...]]. Keep Materialization and executor unchanged. Synchronous FileMaterialization implements it.
- File available snapshots a non-text Sequence, validates every context with existing _identity, and rejects duplicate canonical identity bytes before storage reads. No raw-key hashing/equality. Then call resolve in context order, skip None and return tuple of committed records. Empty selection bypasses I/O. This reuses stale/missing/corrupt semantics without duplicating storage/version logic or decoding. No snapshot consistency across calls/records; load can reject later revocation.
- Explicit contexts may include different assets/versions/domains/input IDs if the application chooses them. No inferred prior-run outputs or enumeration APIs. A prior compatible record may be included only through explicit exact context selection.
- Example uses a locally owned registry and explicit partitions, one body failure, then builds exact contexts and calls available/load from ordinary Python after RunResult. It deliberately publishes partial available rows; failed RunResult is printed/preserved. A shared local JSON snapshot has rows and receipt (publication_id, materialization_ids, row_count). publication_id is deterministic SHA256 of canonical rows plus included record IDs. Same selection yields same ID.
- Example publish_snapshot(path:Path, rows:Sequence[object], materialization_ids:Sequence[str]) -> dict[str,object] validates/encodes before writing, writes synced temporary file in destination directory and replaces destination once. Receipt is part of the same encoded file, never a separate premature success marker. Failure cleanup preserves prior destination. This is a local application example, not an external transaction guarantee. Repeated example invocation with an existing root works.
- No file-format, portable record/event/CLI/SQLite version changes. IDs, rows, stored keys and receipts are application data with no automatic redaction.

## Ownership

Root: materialization.py, __init__.py, tests/test_available_materializations.py, tests/test_public_api.py, OpenSpec/ROADMAP/final review/package/commit.
Adapter agent: file_materialization.py and tests/test_file_materialization.py only.
Example/Docs agent: new examples/available_materializations.py, docs/user/partitions.md, docs/user/guides/persistence.md, docs/user/api/materialization.md, docs/user/api/file-materialization.md, docs/user/concepts/trust-boundary.md, docs/release-notes.md, scripts/smoke_wheel_install.py.
Independent publication test owner: new tests/test_available_publication.py only.

## Risks / Trade-offs

Partial aggregate can be deliberate data loss → explicit context choice and failed producer diagnostics, never silent corrupt omission.
Collection/load races → no multi-record transaction; coordinate users and honor load errors.
Sink/receipt atomicity → local single-file envelope only; external app owns idempotency/receipt strategy.

## Migration Plan

Additive optional API only; no existing dependencies/schema changes. Ship example and evidence, retain Python3.10–3.13 CI and core-only wheel smoke.
