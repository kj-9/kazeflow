# Flow plan CLI

## Purpose

Specify stdlib CLI inspection for script-defined flows.
## Requirements
### Requirement: Inspect scripts through `kazeflow`

`kazeflow assets`, `kazeflow partitions`, and `kazeflow plan` SHALL use the flow
CLI entry contract and supplement the Python APIs.

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

`plan` SHALL show selected targets, dependency-first order, normalized partition
selection kind, partition domain, per-task selection count, and normalized supported
configuration without invoking an asset body. It SHALL support explicit targets and
derive all terminal assets when an undeclared script has no selected target. Explicit
keys, one inclusive `--partition-range START END`, and `--empty-partitions` SHALL be
mutually exclusive. The existing repeatable `--partition-key` and `--partition` alias
SHALL remain available. Supplying a selector to an unpartitioned closure SHALL fail as
a usage error rather than silently ignoring it.

#### Scenario: Build a plan
- **WHEN** a caller selects valid repeatable keys and configuration
- **THEN** `plan` shows its dependency-first normalized plan without asset execution

#### Scenario: Build a bounded range plan
- **WHEN** a caller supplies one valid date range
- **THEN** `plan` reports range selection and the inclusive normalized count without exposing raw key values

#### Scenario: Select no partition work explicitly
- **WHEN** a caller supplies `--empty-partitions` for a partitioned closure
- **THEN** `plan` distinguishes explicit empty selection from omitted selection

#### Scenario: Run the reviewed normalized selection
- **WHEN** a caller approves a preflight plan produced by a stateful custom definition
- **THEN** `run` executes that exact normalized plan without parsing, normalizing, or expanding its selection again

#### Scenario: Conflicting or irrelevant selectors are rejected
- **WHEN** a caller combines selection forms or selects partitions for an unpartitioned closure
- **THEN** the CLI writes a diagnostic to stderr, leaves stdout empty, exits with status `2`, and invokes no asset body

### Requirement: Review safety boundary

Inspection SHALL preserve the distinction between potentially side-effectful entry
loading and non-execution of asset bodies; it is not sandboxing or approval.

#### Scenario: Load with side effects
- **WHEN** an inspected entry has top-level side effects
- **THEN** they remain distinct from non-invocation of asset bodies

### Requirement: Output and core boundary
Text and one-document typed JSON plans and partition-definition inspection SHALL
separate stdout from diagnostics. In JSON mode, user-Python stdout emitted while
loading the entry or factory SHALL be written to stderr so stdout contains only the
completed typed document. Their documented lossy projections SHALL expose selection
kind, stable domain, definition metadata, and counts while omitting arbitrary raw
partition keys. Default inspection SHALL not require Rich, SQLite, persistence,
execution, or a mandatory third-party runtime dependency.

#### Scenario: Use core-only inspection
- **WHEN** an installation has no optional extras
- **THEN** `assets`, `partitions`, and `plan` remain available

#### Scenario: Preserve a JSON inspection document
- **WHEN** loaded entry code prints while a caller requests JSON assets, partitions,
  or plan output
- **THEN** stdout remains one typed inspection document and the entry text is sent
  to stderr
