## Why

M4 made the core install and public API release-ready, but users still need a clear
way to inspect a human- or AI-authored flow before executing it and to interpret its
outcome afterward.  M5 turns the existing plan and result values into a documented,
repeatable review workflow without expanding the runtime.

## What Changes

- Document the standard `plan → review → run → result` workflow using public core
  APIs for both human-written and AI-generated flows.
- Add a minimal README example and a separate pre-execution review example that
  checks selected targets, dependency-first task order, partitions, and run
  configuration before running.
- Define the distinct roles of `FlowPlan`, `RunResult`, and logs, including what
  review can and cannot establish.
- State explicitly that reviewability is decision support, not a security sandbox,
  a proof of safety, or a guarantee of an asset's side effects.
- Provide a release-ready command that validates the core-only public workflow from
  an installed wheel, without selecting optional presentation features.

## Capabilities

### New Capabilities

- `reviewable-flow-workflow`: Documented public workflow and safety boundary for
  planning, reviewing, running, and assessing small human- or AI-authored flows.

### Modified Capabilities

- `core-installation-smoke`: The core-only installed-wheel smoke and README
  documentation requirements gain an explicit release-ready public workflow command
  and review-oriented documentation coverage.

## Impact

This M5 documentation-focused change affects `README.md`, documentation/examples,
and focused documentation or installed-wheel smoke coverage.  It makes no executor,
result, event, package metadata, dependency, persistence, or public API contract
change.  The standard-library-only core remains compatible with Python 3.10 through
3.13, and optional Rich rendering remains an explicit `tui` extra.
