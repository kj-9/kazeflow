# Verification: add-available-materialization-aggregation

## Completeness

7/7 tasks and all three collection/publication requirements are covered.

## Correctness

Independent collection tests cover optional sync/async protocol, no-I/O empty selection, snapshot validation before storage reads, duplicate/unsupported identities, typed falsey ordering, explicit past/current contexts, absent version/input identities, missing/revoked omission, post-collection load revocation, no decode/write and explicit corrupt/codec errors.

Actual partially failed producer run retains FAILED status and blocked normal aggregate while ordinary Python deliberately loads available outputs. Independent publication tests cover rows/receipt in one file, deterministic publication ID and source-ID sensitivity, replacement failure preserving previous bytes and cleaning temporaries, invalid JSON before write, and repeated existing-root example execution retaining producer failure while reusing available committed artifacts.

## Coherence

Existing executor and file/portable/event/CLI/SQLite schemas unchanged. Optional collection is adapter-owned, exact contexts control all historical inclusion, and external publication remains application-owned. Example has local registry, strict JSON validation, explicit selection and a single atomic local rows/receipt envelope. No inferred keys, directory scanning, auto-resume or external exactly-once promise.

## Validation

- Focused collection/publication/file/export tests:77 passed.
- `make test`:519 passed on Python3.13.
- `make ci-check`:64 files formatted, Ruff and ty passed.
- `make docs-check`: strict build and13 selected pages/search index passed.
- Same-root example ran twice with identical publication identity and failed producer diagnostics.
- Offline wheel build, metadata and isolated core install smoke passed on Python3.12. All15 packaged modules match source. Python3.10–3.13 CI retained;3.10/3.11 not run locally.
- OpenSpec doctor and strict validation passed.

No unresolved critical issues or warnings. Ready for archive.
