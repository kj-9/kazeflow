## Why

M7--M10 delivered the CLI contract, inspection, deliberate execution, and local
history. M11 makes those commands safe to rely on as a public interface by fixing
their compatibility expectations, help/error behavior, and release verification.

## What Changes

- Define a versioned public-CLI compatibility policy for command names, options,
  exit statuses, and JSON schemas.
- Standardize `--help` and diagnostic behavior, and redesign the human-facing
  `plan` projection as a concise summary plus a deterministic dependency graph.
- Offer Mermaid and DOT projections for a resolved plan, while preserving the
  existing JSON schema and machine-readable output boundary.
- Improve the explicitly selected Rich TUI so an interactive user can read queued,
  running, completed, skipped, and failed work at a glance without changing run
  semantics or default non-interactive output.
- Expand installed-wheel and Python 3.10--3.13 smoke coverage for core-only and
  optional CLI paths, then document the stable review and CI workflows.
- Add release notes and a CLI migration policy for future breaking changes.

This change does not add static analysis, new orchestration features, third-party
runtime dependencies, or alter flow execution semantics. Graphs describe the
resolved `FlowPlan`; they are not a security or sandbox guarantee.

## Capabilities

### New Capabilities

- `public-cli-stability`: Defines CLI compatibility, release verification,
  migration expectations, and human-readable plan rendering for the published
  command surface.

### Modified Capabilities

- `flow-cli-contract`: Clarifies compatibility guarantees for existing command,
  exit-status, and JSON-output requirements.
- `optional-tui-rendering`: Clarifies the interactive `--tui` presentation contract
  while preserving the optional event-consumer boundary.

## Impact

- Advances ROADMAP M11.
- Affects `src/kazeflow/cli.py`, package/release smoke coverage, documentation,
  and CLI tests; it keeps the core stdlib-only and does not add dependencies.
- Existing CLI command and JSON consumers gain an explicit compatibility policy;
  no breaking command rename or Python API removal is in scope.
