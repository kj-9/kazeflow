## Why

The approved CLI-first contract defines how kazeflow will load and review Python
flow scripts, but users still cannot inspect a script from the shell. M8 delivers
the first usable CLI surface: listing assets and producing a deterministic,
non-executing plan before any later `run` command is introduced.

## What Changes

- Add the stdlib-only `kazeflow` console command with `assets` and `plan`
  subcommands.
- Load a bare Python script, list its registered assets, and use a declared
  module-level `flow` or derived terminal-asset candidates according to the
  approved CLI contract.
- Add deterministic text and lossy JSON projections for asset listings and
  `FlowPlan` values, with JSON written only to stdout.
- Add target and supported run-configuration options for plan inspection, plus
  entry, loading, and configuration diagnostics with the approved exit-status
  classes.
- Add core-only installed-wheel CLI smoke tests and command integration tests.

## Capabilities

### New Capabilities

- `flow-plan-cli`: Provides the executable `kazeflow assets` and `kazeflow plan`
  inspection workflow for script-defined assets and flows.

### Modified Capabilities

- None. This change implements the active `define-cli-contracts` contract; its
  living-spec sync remains serial work when that contract change is archived.

## Impact

- Advances ROADMAP M8 and implements the first part of the active
  `define-cli-contracts` change.
- Adds a stdlib-only CLI module, console-script metadata, documentation, and
  focused integration/smoke tests. `pyproject.toml` is a single-owner hotspot.
- Does not execute asset bodies for either command, add a third-party runtime
  dependency, persist runs, import Rich by default, or change the existing Python
  plan/run APIs.
