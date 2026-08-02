## ADDED Requirements

### Requirement: Inspect scripts through the `kazeflow` command
The stdlib-only `kazeflow` executable SHALL provide `assets` and `plan` as
inspection commands for script-defined flows. Both commands SHALL accept the
entry forms and use the entry-resolution rules defined by the `flow-cli-contract`
capability. They SHALL supplement the Python APIs without requiring callers to
use the CLI.

#### Scenario: Inspect a declared flow script
- **WHEN** a caller invokes either inspection command with a loadable bare
  Python script that declares a module-level `flow`
- **THEN** the command resolves that declared flow according to the CLI
  contract and performs the requested inspection

#### Scenario: Inspect an explicitly selected flow attribute
- **WHEN** a caller invokes an inspection command with a valid
  `module:attribute` or `path/to/file.py:attribute` entry resolving to a flow
- **THEN** the command performs the requested inspection for that flow

### Requirement: List discovered assets without requiring a declared flow
`kazeflow assets` SHALL list the assets discovered while loading a bare script,
including when that script does not declare a module-level `flow`. The listing
SHALL be deterministic for equivalent loaded definitions. A script with neither
a declared flow nor discovered assets SHALL be reported as an entry-resolution
failure.

#### Scenario: List assets from an undeclared script
- **WHEN** a caller invokes `kazeflow assets` for a loadable bare script that
  registers assets but declares no module-level `flow`
- **THEN** stdout presents the discovered assets in a deterministic order and
  the command exits with status `0`

#### Scenario: Reject a script with no inspectable definition
- **WHEN** a caller invokes `kazeflow assets` for a loadable bare script with
  neither a module-level flow nor discovered assets
- **THEN** the command writes a diagnostic to stderr, emits no successful
  listing, and exits with status `3`

### Requirement: Build an inspectable plan without executing asset bodies
`kazeflow plan` SHALL display the selected targets, dependency-first execution
order, partition selection, and normalized execution configuration using the
existing flow planning semantics. It SHALL support explicit target selection
and supported explicit planning configuration. For an undeclared bare script
without an explicit target, it SHALL plan the terminal discovered asset target
or targets derived under the `flow-cli-contract` capability. Equivalent entry
definitions and selections SHALL yield deterministic text and machine-readable
plan projections.

#### Scenario: Plan a selected target
- **WHEN** a caller invokes `kazeflow plan` with a valid entry, explicit target,
  and valid supported planning configuration
- **THEN** the displayed plan contains that target's dependency closure and
  reflects the corresponding partition and normalized configuration semantics

#### Scenario: Plan multiple derived terminal targets
- **WHEN** a caller invokes `kazeflow plan` without a target for an undeclared
  bare script with multiple terminal discovered assets
- **THEN** the command displays the derived terminal targets and their combined
  plan without requiring the caller to choose one first

#### Scenario: Reject an invalid plan selection
- **WHEN** a caller supplies an unknown target, a cyclic or missing dependency
  definition, or invalid planning configuration
- **THEN** the command writes a diagnostic to stderr, emits no successful plan,
  and exits with status `2`

### Requirement: Preserve the loading and review safety boundary
Both inspection commands SHALL load entries only as required by the inherited
CLI entry-resolution contract. They SHALL not invoke an asset body while
listing assets or creating a plan. The commands SHALL preserve the distinction
between potentially side-effectful entry loading and non-execution of asset
bodies, and SHALL not present inspection as sandboxing, a safety proof, or
automatic approval to run a flow.

#### Scenario: Plan a script whose asset body has a side effect
- **WHEN** a caller invokes `kazeflow plan` for a loadable script containing an
  asset whose body would have an observable side effect if invoked
- **THEN** the command displays the plan without that asset-body side effect

#### Scenario: Load a side-effectful script
- **WHEN** a caller invokes an inspection command for a script with a top-level
  import-time side effect
- **THEN** any such side effect remains attributable to loading user Python and
  is not represented as asset-body execution by the command

### Requirement: Separate text, JSON, diagnostics, and status classes
`assets` and `plan` SHALL provide human-oriented text output and a JSON output
mode. In JSON mode, a successful command SHALL write exactly one JSON document
to stdout; diagnostics SHALL be written only to stderr. The plan JSON
projection SHALL be a documented lossy representation and SHALL not expose raw
arbitrary Python partition-key objects. Successful inspections SHALL exit with
status `0`; supplied command-line or configuration failures SHALL exit with
status `2`; and entry-resolution failures SHALL exit with status `3`, as
defined by the inherited CLI contract.

#### Scenario: Emit a JSON plan
- **WHEN** a caller requests JSON output for a successful plan inspection
- **THEN** stdout contains exactly one machine-readable plan document, stderr
  contains no diagnostic, and the command exits with status `0`

#### Scenario: Report a configuration error in JSON mode
- **WHEN** a caller requests JSON output with invalid command-line arguments or
  planning configuration
- **THEN** stdout contains no successful inspection document, stderr contains
  the diagnostic, and the command exits with status `2`

### Requirement: Keep inspection on the zero-dependency core path
The default `assets` and `plan` commands SHALL operate with Python standard
library facilities and the kazeflow core only. They SHALL not require or
initialize Rich, create a database, persist a run record, execute a flow, or
introduce a mandatory third-party runtime dependency.

#### Scenario: Inspect with a core-only installation
- **WHEN** a caller installs kazeflow without optional extras and runs a valid
  `assets` or `plan` command
- **THEN** the command completes its inspection without importing or requiring
  an optional presentation or persistence feature
