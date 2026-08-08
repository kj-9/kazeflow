## 1. Public CLI contract and projections

- [x] 1.1 Update the CLI contract tests for supported formats, stable exits, and
  stdout/stderr boundaries. Owner: `tests/test_cli.py`.
- [x] 1.2 Implement deterministic ASCII, Mermaid, and DOT projections from a
  resolved `FlowPlan`, including safe task labels and partition metadata. Owner:
  `src/kazeflow/cli.py`.
- [x] 1.3 Add `plan` format and text-only detail-option parsing, validation, help,
  and usage diagnostics without changing the JSON projection. Owner:
  `src/kazeflow/cli.py`.
- [x] 1.4 Extend the optional Rich renderer with plan-aware safe task-state display
  and overall progress, retaining its event-consumer boundary. Owner:
  `src/kazeflow/tui.py`, `tests/test_tui.py`.
- [x] 1.5 Wire the enhanced renderer through explicit CLI `--tui` selection and
  cover stderr/JSON separation and result-semantic equivalence. Owner:
  `src/kazeflow/cli.py`, `tests/test_cli.py`.

## 2. Documentation and release verification

- [x] 2.1 Document the public CLI compatibility policy, text-plan graph, Mermaid,
  DOT, detail usage, and interactive TUI mode in the README and CLI guide. Owner:
  `README.md`, `docs/cli.md`.
- [x] 2.2 Extend wheel smoke coverage for core-only and explicitly selected TUI and
  SQLite CLI paths on supported Python versions. Owner:
  `scripts/smoke_wheel_install.py` and dedicated smoke tests.
- [x] 2.3 Add release-note and migration guidance for CLI compatibility and the
  former linear text plan. Owner: release documentation.

## 3. Integration verification

- [x] 3.1 Run focused CLI projection, error-boundary, target, branch, and
  partition tests, including JSON compatibility fixtures.
- [x] 3.2 Run `make test`, `make ci-check`, `uv build`, and installed-wheel
  core-only/TUI/SQLite smoke checks.
- [x] 3.3 Run `openspec validate stabilize-cli-public-api --strict`, verify the
  implementation against this change, sync affected living specs, and archive the
  change serially.
