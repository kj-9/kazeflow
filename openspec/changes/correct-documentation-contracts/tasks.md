## 1. Behavioral evidence

- [x] 1.1 Add focused documentation-contract tests in a new test module that prove
  current partitioned planning rejects an omitted selection, preserves an explicit
  textual CLI key, and keeps `DatePartitionDef.range()` a separately invoked Python
  helper.
- [x] 1.2 Add a portable-record regression fixture whose exception repeats a
  partition key, asserting that the structural key field is omitted while portable
  failure metadata remains application-controlled.
- [x] 1.3 Reuse or extend cancellation contract evidence to assert that external
  `Flow.run_async()` cancellation propagates without a synthetic terminal result.

## 2. Partition and trust guidance

- [x] 2.1 Update `docs/user/partitions.md` and the Partition example pages to require
  explicit current selections, remove definition-owned CLI validation claims, and
  demonstrate `DatePartitionDef.range()` only through explicit Python configuration.
- [x] 2.2 Expand `docs/user/concepts/trust-boundary.md` with distinct entry-loading,
  asset-execution, and portable-failure-metadata boundaries, including the fact that
  portable records can contain sensitive application values.

## 3. Result and cancellation guidance

- [x] 3.1 Update result, persistence, CLI output, and failure-example pages so raw
  structural-field omission is never presented as generic redaction or secrecy.
- [x] 3.2 Update failure and planning/result concepts to separate representable
  cancelled statuses from external asyncio cancellation, which returns no synthetic
  `RunResult`.

## 4. First-run diagnostics and generated contract

- [x] 4.1 Extend getting-started with supported Python checks, virtual-environment
  creation/activation, core installation, `kazeflow --help`, and quoted TUI-extra
  installation without duplicating the reference pages.
- [x] 4.2 Extend the generated documentation contract checker/tests to require the
  corrected Partition, record-sensitivity, cancellation, and first-run statements
  on the intended pages and search index.

## 5. Serial integration and verification

- [x] 5.1 Reconcile terminology and cross-links across all changed pages, build with
  `make docs-check`, and visually inspect the affected desktop/mobile journeys.
- [x] 5.2 Run `make test`, `make ci-check`, wheel metadata/core smoke checks,
  `git diff --check`, `openspec doctor`, and `openspec validate --all --strict`.
- [ ] 5.3 Verify implementation against the change artifacts, sync the delta to the
  hosted-user-documentation living spec, archive the change, commit in reviewable
  units, push, and confirm CI plus the corrected public Pages content.
