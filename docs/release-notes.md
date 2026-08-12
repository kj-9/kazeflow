# Release notes

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
