## Why

Kazeflow currently derives execution order inside the TUI-coupled executor, and
the registry graph uses unordered dependency sets.  A caller therefore cannot
inspect a stable, validated description of the selected work before any asset
is scheduled.  M1 needs a small, display- and persistence-independent plan
model before the executor can be safely integrated with the execution
contracts.

## What Changes

- Add immutable `PlanConfig`, `TaskPlan`, and `FlowPlan` core data models in a
  new `kazeflow.plan` module.
- Add a `build_flow_plan()` entry point that resolves a selected target closure
  from the existing asset registry without invoking asset functions, with one
  canonical target-selection input and lexical target projection.
- Make the plan's task sequence deterministic and dependency-first, and reject
  missing assets, dependency cycles, invalid targets, and invalid planning
  configuration before returning a plan.
- Represent an unpartitioned task separately from a partitioned task with an
  explicitly empty selection, while preserving falsey partition keys and
  rejecting `None` and equality-duplicate keys.
- Keep this wave limited to the new plan module and its dedicated tests; later
  executor integration will adapt existing `Flow` and `run()` entry points.

## Capabilities

### New Capabilities

- `flow-planning`: Build an immutable, deterministic, and validated description
  of selected flow work without executing assets.

### Modified Capabilities

- None.

## Impact

This advances M1, Workstream A (`add-flow-plan`) in `docs/ROADMAP.md`.  The
implementation is confined to `src/kazeflow/plan.py` and `tests/test_plan.py`;
it reads existing asset metadata but does not change `flow.py`, `assets.py`,
`__init__.py`, execution behavior, or existing public entry points.  The new
module is a standard-library-only core API and adds no runtime dependency.  It
introduces an opt-in public import path, `kazeflow.plan`, with no breaking
compatibility change; integration and top-level re-export decisions remain for
later milestones.
