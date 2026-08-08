# Flow plan CLI

## Purpose

Specify stdlib CLI inspection for script-defined flows.

## Requirements

### Requirement: Inspect scripts through `kazeflow`

`kazeflow assets` and `kazeflow plan` SHALL use the flow CLI entry contract and
supplement the Python APIs.

#### Scenario: Inspect a script
- **WHEN** a caller supplies a valid script entry
- **THEN** the inspection command uses the shared CLI contract

### Requirement: Deterministic asset listing

`assets` SHALL list assets discovered while loading a bare script, even without a
declared `flow`, in deterministic order. A script with neither a flow nor assets
SHALL be an entry-resolution failure.

#### Scenario: List discovered assets
- **WHEN** a bare script registers assets without a flow
- **THEN** `assets` lists them deterministically

### Requirement: Inspectable non-executing plans

`plan` SHALL show selected targets, dependency-first order, partition selection,
and normalized supported configuration without invoking an asset body. It SHALL
support explicit targets and derive all terminal assets when an undeclared script
has no selected target.

#### Scenario: Build a plan
- **WHEN** a caller selects a valid target and configuration
- **THEN** `plan` shows its dependency-first plan without asset execution

### Requirement: Review safety boundary

Inspection SHALL preserve the distinction between potentially side-effectful entry
loading and non-execution of asset bodies; it is not sandboxing or approval.

#### Scenario: Load with side effects
- **WHEN** an inspected entry has top-level side effects
- **THEN** they remain distinct from non-invocation of asset bodies

### Requirement: Output and core boundary

Text and one-document JSON plans SHALL separate stdout from diagnostics. Plans
SHALL use a documented lossy projection that omits arbitrary raw partition keys.
Default inspection SHALL not require Rich, SQLite, persistence, execution, or a
mandatory third-party runtime dependency.

#### Scenario: Use core-only inspection
- **WHEN** an installation has no optional extras
- **THEN** `assets` and `plan` remain available
