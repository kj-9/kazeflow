# Release notes

## Unreleased: Materialization provenance

- Portable result record v4 preserves the resolution (`executed` or
  `materialized`) and materialization ID on successful attempts. Failed, skipped,
  and cancelled attempts carry null provenance. Python `RunResult.to_record()`
  continues to default to v1; versions 1–3 retain their existing shapes.
- Event projection defaults to v4 and adds nullable provenance to successful
  attempt-finished events. Explicit event versions 1–3 keep their existing shapes.
- CLI run and history envelopes advance to v4, while plan remains v4 and other
  documents remain v1. The schema collection advances to v6. SQLite database
  layout remains v1; new rows use portable record v4 and historical v1–v3 rows
  remain readable.
- Materialization IDs are opaque adapter-defined diagnostic strings. They may
  contain application information and are not automatically redacted. History
  inspection displays saved IDs without contacting adapters or loading outputs.
- Added a runnable example and installed-wheel smoke coverage for committed and
  reused provenance in results, events, and portable record v4.

## Unreleased: Partition outcome queries

- Live `RunResult` objects expose `failed_partition_keys(task_name)` and
  `circuit_skipped_partition_keys(task_name)` to return original partition keys for
  matching terminal outcomes, in attempt order. The queries preserve falsey keys and
  distinguish circuit skips from other skipped work.
- The methods are in-memory only. Portable records continue to omit raw partition
  keys; selecting keys for another run remains an explicit application decision.
- Added a runnable example showing how to inspect outcomes and explicitly choose keys
  for a later run.

## Unreleased: Local file materialization

- Added opt-in `kazeflow.file_materialization.FileMaterialization` for reuse of
  committed asset outputs across processes with explicit codecs and a versioned
  integrity-checked manifest. The standard-library `JsonCodec` accepts strict JSON
  values only; the adapter does not encrypt or redact stored outputs or keys.
- Immutable generations are published through an atomic local reference update.
  The store retains old generations and does not provide invalidation, cleanup,
  locking, power-loss durability, or network filesystem guarantees. The adapter is
  synchronous and a commit already running in a worker thread may finish after flow
  cancellation.
- Added a three-process example that commits a producer output, fails downstream,
  then retries downstream while reusing the producer output.

## Unreleased: Flow-scoped resource limits

- `Flow` can declare named `ResourcePolicy` limits for simultaneous invocations
  and asset body invocation starts over a rolling time window. Rate limits are
  shared within one run, count body starts rather than API requests, and do not
  coordinate separate runs.
- The initial resource policy projection added plan schema v3 with selected
  concurrency/rate limits and task-resource associations. The retry extension
  below advances the current main contracts to plan v4 and run/history v2; the
  circuit-breaker extension advances run/history to v3.
- Plan v1 through v3 schemas and goldens remain available for historical use.

## Unreleased: Resource circuit breaker

- Resource failure classifiers can return `OpenCircuit()` to stop pending body work
  on that resource for the rest of the current run. The first source attempt is
  recorded; admitted operations finish normally, dependency-blocked outcomes retain
  precedence, and compatible materialization hits can complete without a body call.
- New circuit skips retain a `blocked_by` source reference. Portable result and event
  schema v3 accept `resource_circuit_open` and aggregate `mixed_blocked` reasons;
  older record/event versions reject those values.
- CLI run and history envelopes advance to v3. The schema collection advances to v5,
  while plan remains v4 and database layout remains v1. Stores can read and retain
  mixed record v1/v2/v3 rows without rewriting existing records.

## Unreleased: Resource retry and cooldown

- Resource policies can opt into bounded retries with an explicit synchronous
  failure classifier. Each retry reacquires concurrency and rolling rate permits;
  requested backoff and resource cooldown waits release execution capacity.
- One terminal logical attempt now retains numbered invocation history. Only asset
  body exceptions are classified; materialization preparation/commit and observer
  failures are not automatically retried. Application side effects may repeat.
- Plan JSON advances to v4 with retry budget and classifier-enabled metadata. Run
  result and history documents advance to v2 and include invocation records. SQLite
  layout remains schema v1 and can store record v1, v2, and v3 rows.
- Plan v1-v3 examples remain available for historical consumers; run/history v1
  records remain readable. Consumers should explicitly accept the new per-type
  schema versions.

## CLI compatibility policy

The public `kazeflow` command names, documented options, exit statuses, and JSON
schemas are versioned public interfaces. JSON output is the supported interface for
automation; human-facing text is intended for review and can evolve in layout while
preserving its documented meaning.

Before a compatible release removes or renames a documented CLI command or option,
the project will publish a deprecation, a migration path, and a release note.
Breaking CLI changes require an explicitly documented pre-1.0 compatibility boundary.

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
- JSON mode routes ordinary output from entry modules, explicit factories, and asset
  bodies to stderr so stdout remains one parseable document. This is stream routing
  for trusted Python, not a sandbox or redaction guarantee.
- Draft 2020-12 JSON Schemas, normalized golden examples, and installed-wheel
  compatibility checks now cover all public machine-readable CLI commands.
- GitHub Pages is the user-documentation source of truth, with searchable task,
  concept, CLI, Partition, result/history, trust-boundary, and automation references.

This is an alpha-breaking JSON migration: consumers of `0.1.0a3` JSON must read the
new typed envelope and explicitly accept each `(document_type, schema_version)` pair.

## 0.1.0a3: CLI review workflow

- `kazeflow plan` presents a concise graph-oriented text review by default.
- Mermaid and DOT plan projections are available without adding a runtime
  dependency or invoking an external renderer.
- `kazeflow run --tui` presents plan-aware task state and overall progress only when
  the optional TUI extra is explicitly installed and selected.
- Text `kazeflow run` output now summarizes each task's terminal status, duration,
  and safe failure or skip context. `--verbose` adds safe attempt-level detail
  without exposing raw outputs, partition keys, exceptions, or tracebacks.
- The CLI quick start and partition guide now cover a complete
  script-to-plan-to-selected-run-to-history workflow.
- Scripts that parsed the former linear text plan should migrate to
  `kazeflow plan --format json`.
