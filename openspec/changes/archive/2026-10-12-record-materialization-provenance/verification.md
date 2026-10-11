# Verification: record-materialization-provenance

## Completeness

7/7 tasks complete. All requirements and scenarios in five capability deltas are implemented and covered.

## Correctness

- `tests/test_provenance.py` covers result/event explicit versions, null/known success metadata, malformed metadata, invalid lifecycle/version inputs, recovered body retries with commit IDs, reuse with matching IDs and no body invocation, and commit failure blocking downstream without successful metadata. Event sequences remain valid.
- Result v1 default and explicit v1–v3 shapes remain unchanged; event default advances v4 and explicit historical versions omit new fields.
- `tests/test_sqlite_store.py` covers v4 semantic rejection, both required fields, executed/reused IDs, and mixed v1–v4 records without rewriting historical rows. Database stays v1.
- CLI tests cover actual committed then reused JSON run, SQLite history and verbose metadata. Schema tests bind record shapes to versions and reject malformed provenance. Schema collection v6 and 15 goldens are published; tracked v1–v5 schema/golden files have zero diff.
- Documentation, two-process example and installed-wheel smoke explain/verify metadata. Raw keys/outputs/exceptions remain excluded; opaque IDs are disclosed as application-defined metadata without redaction or artifact lookup.

## Coherence

Implementation follows fixed ownership and additive event fields. Only success event emission changes in executor; no invocation/admission/storage lifecycle change. Existing spec ambiguity about executed committed IDs is reconciled with already-tested behavior. Compare summaries retain status semantics; full stored envelopes retain provenance.

## Validation

- Core focused tests: 90 passed.
- Storage/migration tests: 47 passed.
- CLI/schema tests: 45 passed.
- `make test`: 442 passed (local Python 3.13).
- `make ci-check`: format (54 files), Ruff, ty passed.
- `make docs-check`: strict build and 13 pages/search index passed.
- Two explicit example process runs passed with matching executed/materialized IDs.
- `uv build --offline`, wheel metadata and isolated core wheel smoke passed. All 14 Python modules match source.
- Wheel smoke passed on system Python 3.14 and supported Homebrew Python 3.12. uv-managed Python 3.13's venv ensurepip aborted with SIGABRT; supported 3.12 clean install supplies the package gate. CI matrix 3.10–3.13 retained; 3.10/3.11 not run locally.
- Initial smoke retained old CLI v3 asserts; corrected to v4 and rerun successfully. Initial example format check corrected and passed.
- OpenSpec doctor and strict validation passed.

No unresolved implementation issues. Ready for archive.
