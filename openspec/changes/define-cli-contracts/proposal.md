## Why

`FlowPlan` and `RunResult` already make a flow reviewable from Python, but using
them from a shell currently requires application-specific glue code. M7 defines
`kazeflow` as the primary interface for human, CI, and AI-assisted flow review
and execution, while retaining the Python API for definition, tests, and custom
integration.

## What Changes

- Define `kazeflow assets`, `kazeflow plan`, and `kazeflow run` as the basic
  command-line workflow for a Python script or module.
- Let a bare Python file be the primary entry form. A module-level `flow` is an
  optional default declaration; without it, the CLI discovers loaded assets and
  proposes their terminal assets as targets for review.
- Require an explicit `--target` before running when that discovery produces
  more than one terminal candidate. A factory is never discovered implicitly.
- Define the user-visible contract for command structure, entry-point loading,
  plan-before-run review, text and JSON output streams, and exit-status classes.
- Define the boundary between resolving user Python and invoking asset functions:
  planning does not invoke assets, while module import and an explicitly selected
  factory can execute arbitrary user Python outside kazeflow's control.
- Define how optional Rich presentation and explicit SQLite run storage may be
  selected from the CLI without becoming implicit core dependencies.
- Define compatibility expectations for CLI help, exit statuses, and
  machine-readable projections before a public implementation is added.

## Capabilities

### New Capabilities

- `flow-cli-contract`: Defines the observable, CLI-first contract for loading,
  discovering, reviewing, executing, and optionally recording a Python flow.

### Modified Capabilities

- None.

## Impact

- Advances ROADMAP M7 and provides the specification gate for later M8--M11 CLI
  implementation changes.
- Will add a console-script entry point and a CLI module in a subsequent
  implementation change; this contract change does not add either one yet.
- The core remains Python-standard-library-only. Rich stays an optional extra and
  SQLite persistence stays caller-selected.
- Existing Python APIs and their execution semantics remain compatible. They are
  the escape hatch for definition, tests, and custom integration, not the
  primary documented plan/run workflow. No sandbox, scheduler, daemon, remote
  worker, or implicit cache is introduced.
