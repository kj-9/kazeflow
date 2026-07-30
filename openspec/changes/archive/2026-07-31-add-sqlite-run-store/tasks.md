## 1. Adapter and schema implementation

- [x] 1.1 **Persistence owner only:** add `src/kazeflow/sqlite_store.py` with the
  explicit `SQLiteRunStore` public API (`save`, `load`, `list_runs`) using only
  standard-library SQLite/JSON modules; do not edit core execution or root exports.
- [x] 1.2 **Persistence owner only:** implement version-1 schema initialization,
  deterministic record storage/listing, duplicate/missing-record errors, and exact
  lossy serialization from `RunResult.to_record()`.
- [x] 1.3 **Persistence owner only:** implement schema-version checks and only
  supported transactional forward migrations; reject newer schemas before mutation
  and preserve records/version on migration failure.

## 2. Independent persistence verification

- [x] 2.1 **Store-test owner only:** add dedicated save/load/list tests for successful,
  failed, cancelled, skipped, and partitioned RunResult-derived records, including
  task/attempt order, failure metadata, blockers, duplicate saves, and missing loads.
- [x] 2.2 **Serialization-boundary test owner only:** add dedicated tests that prove
  raw outputs, exception objects, and raw partition keys are absent while presence of
  `0`, `""`, and `False` partitions remains represented and non-serializable raw
  values do not prevent a round trip.
- [x] 2.3 **Migration-test owner only:** add isolated schema-version fixtures/tests
  for fresh initialization, future-version rejection, supported forward migration,
  and transactional rollback on migration failure.
- [x] 2.4 **Core-boundary test owner only:** add or extend focused import/execution
  coverage proving a caller that never imports or constructs the adapter creates no
  database and keeps core plan/run/result behavior unchanged.

## 3. Documentation and integration

- [x] 3.1 **Documentation owner only:** document explicit local store construction,
  save/load/list use, schema compatibility expectations, and the lossy record
  boundary; do not imply automatic persistence, remote storage, or a sandbox.
- [x] 3.2 **Integration owner only:** review that `flow.py`, `assets.py`,
  `results.py`, `__init__.py`, `pyproject.toml`, `uv.lock`, and CI remain untouched;
  run dedicated tests, `make test`, `make ci-check`, and a core-only import/run
  smoke without importing `kazeflow.sqlite_store`.

## 4. Serial OpenSpec completion

- [x] 4.1 **Root owner only:** verify adapter implementation against the schema and
  lossy-record requirements, then run `openspec doctor` and strict change/all
  validation after all tests pass.
- [x] 4.2 **Root owner only:** sync the SQLite store capability spec and archive the
  completed change serially; do not overlap another capability's spec sync/archive.
