# Release notes

## CLI compatibility policy

The public `kazeflow` command names, documented options, exit statuses, and JSON
schemas are versioned public interfaces. JSON output is the supported interface for
automation; human-facing text is intended for review and can evolve in layout while
preserving its documented meaning.

Before a compatible release removes or renames a documented CLI command or option,
the project will publish a deprecation, a migration path, and a release note.
Breaking CLI changes require an explicitly documented pre-1.0 compatibility boundary.

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
