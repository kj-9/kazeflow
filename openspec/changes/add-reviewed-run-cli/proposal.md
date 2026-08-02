## Why

`kazeflow plan` lets a caller inspect a trusted script, but execution still
requires a separate Python wrapper. M9 completes the review-first CLI loop by
adding a deliberate `kazeflow run` command that shows the selected work and
requires an explicit decision before invoking asset bodies.

## What Changes

- Add `kazeflow run ENTRY` using the existing plan and executor semantics.
- Require an explicit pre-execution decision: an interactive confirmation for a
  terminal, or `--yes` for non-interactive use such as CI.
- Provide deterministic text summaries and one-document JSON run results with
  the established portable, lossy boundary.
- Permit Rich presentation and SQLite persistence only through explicit CLI
  options; neither is initialized by default.
- Define cancellation, declined confirmation, asset failure, entry/configuration
  failure, and selected-adapter failure behavior and exit statuses.

## Capabilities

### New Capabilities

- `reviewed-run-cli`: Provides a deliberate, review-first CLI execution command
  for script-defined flows.

### Modified Capabilities

- None. This change implements the active CLI contract; its living-spec sync is
  deferred until the contract change is complete and archived.

## Impact

- Advances ROADMAP M9 after M8's inspection commands.
- Affects the stdlib CLI adapter, CLI tests, documentation, installed-wheel
  smoke tests, and optional TUI/SQLite adapter boundaries.
- Adds no mandatory runtime dependency and preserves existing Python `run()` and
  `Flow.run_async()` behavior. It does not add scheduling, sandboxing, remote
  execution, implicit persistence, or automatic approval.
