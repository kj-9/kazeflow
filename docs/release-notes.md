# Release notes

## 0.1.0a5: Resource policies and materialization workflows

This alpha adds flow-scoped resource admission, explicit materialization reuse,
and richer partition scheduling and result inspection. The core remains usable
with the Python standard library alone; the Rich TUI remains an optional extra.

- Named `ResourcePolicy` values can limit concurrent invocations and body starts
  over a rolling time window. Policies can also set bounded retries, classify
  body failures to open a run-scoped circuit, and apply shared cooldowns. Retries
  reacquire resource permits; requested waits release execution capacity. A
  classifier and an asset body can have application side effects and may run
  more than once.
- Live `RunResult` values expose resource-operation, outcome, retry, and observed
  admission-wait statistics, plus `failed_partition_keys(task_name)` and
  `circuit_skipped_partition_keys(task_name)`. The partition-key helpers inspect only the
  current in-memory result and return original keys. They do not resume or start
  another run; choosing keys for a later run remains explicit application logic.
  Statistics and raw keys are not reconstructed from portable history.
- Explicit materialization contexts can reuse compatible committed outputs.
  The optional `MaterializationCollection` protocol and
  `FileMaterialization.available(contexts)` select existing records from supplied
  identities; the adapter does not scan history or infer keys. There is no
  automatic partition selection or run resume. Successful attempts expose whether output was executed or reused and its adapter-defined
  materialization ID in result, event, CLI, and stored-record projections.
- The opt-in local `FileMaterialization` adapter uses a caller-supplied codec and
  integrity-checked immutable generations. `inspect()` reports storage state;
  `invalidate(context, reason=...)` explicitly revokes one exact identity without
  deleting its generations; invalidation is never automatic. New references and
  manifests use file format v2; updated readers accept legacy v1 when there is no revocation ledger. Older
  releases cannot read v2, so stop old writers and quiesce users of the store
  before adopting it. Invalidation does not lock processes, cascade, or guarantee
  revocation after a full-root rollback or ledger deletion. Copy the complete
  store, including its invalidation ledger, to preserve revocations.
- Explicit `PartitionGroup` declarations can schedule disjoint linear chains
  depth-first. A key retains its slot through the chain; a terminal key frees
  a slot for another selected key. Group key limits are separate from global
  concurrency and resource policies. Groups affect admission; they do not evict in-memory outputs or
  provide exactly-once execution across runs.
- Plan JSON advances to v5; run-result and history envelopes advance to v4; all
  other CLI document types remain v1. The schema collection advances to v7.
  Portable result records support v1–v4 and Python `RunResult.to_record()` still
  defaults to v1 unless a version is requested. Event projections support v1–v4
  and default to v4. SQLite database layout remains v1 and can retain records
  v1–v4 without rewriting older rows. Each version is a separate contract.
- Raw partition keys are omitted from portable history and CLI plan projections.
  The CLI plan also omits callback code. Materialization IDs, invalidation reasons,
  stored keys, and outputs are application data and receive no automatic
  redaction. Applications should choose what they persist and publish.

Consumers should check document type and schema version independently and
explicitly accept the versions supported by their installed alpha. The current
contracts are summarized in the [compatibility guide](user/releases/compatibility.md).
See the [JSON automation contract](user/cli/json.md) and materialization
references for field definitions and migration details.

## CLI compatibility policy

The public `kazeflow` command names, documented options, exit statuses, and JSON
schemas are versioned public interfaces. JSON output is the supported interface
for automation; human-facing text is intended for review and can evolve in layout
while preserving its documented meaning.

Before a compatible release removes or renames a documented CLI command or
option, the project will publish a deprecation, a migration path, and a release
note. Breaking CLI changes require an explicitly documented pre-1.0 compatibility
boundary.

## 0.1.0a4: Validated partitions and typed automation output

- Partition definitions now own CLI key parsing and normalization. Date partitions
  accept strict ISO dates, bounded inclusive ranges, and explicit empty selections;
  invalid keys, reversed ranges, and incompatible dependency domains fail before an
  asset body is invoked.
- `kazeflow partitions` inspects partition definition kind, domain, key format, and
  range support without executing assets. Plan and graph projections expose safe
  selection metadata while continuing to omit raw partition keys.
- Every completed `--format json` outcome now uses the typed envelope
  `{document_type, schema_version, data}`. Interactive declines have their own
  `kazeflow.run-declined` document, and completed asset failures remain structured
  `kazeflow.run-result` documents with exit status `1`.
- JSON mode routes ordinary output from entry modules, explicit factories, and
  asset bodies to stderr so stdout remains one parseable document. This is stream
  routing for trusted Python, not a sandbox or redaction guarantee.
- Draft 2020-12 JSON Schemas, normalized golden examples, and installed-wheel
  compatibility checks now cover all public machine-readable CLI commands.
- GitHub Pages is the user-documentation source of truth, with searchable task,
  concept, CLI, partition, result/history, trust-boundary, and automation
  references.

This is an alpha-breaking JSON migration: consumers of `0.1.0a3` JSON must read
the typed envelope and explicitly accept each `(document_type, schema_version)`
pair.

## 0.1.0a3: CLI review workflow

- `kazeflow plan` presents a concise graph-oriented text review by default.
- Mermaid and DOT plan projections are available without adding a runtime
  dependency or invoking an external renderer.
- `kazeflow run --tui` presents plan-aware task state and overall progress only
  when the optional TUI extra is explicitly installed and selected.
- Text `kazeflow run` output now summarizes each task's terminal status, duration,
  and safe failure or skip context. `--verbose` adds safe attempt-level detail
  without exposing raw outputs, partition keys, exceptions, or tracebacks.
- The CLI quick start and partition guide now cover a complete
  script-to-plan-to-selected-run-to-history workflow.
- Scripts that parsed the former linear text plan should migrate to
  `kazeflow plan --format json`.
