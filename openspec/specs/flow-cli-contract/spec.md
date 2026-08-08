# Flow CLI contract

## Purpose

Define the stable, script-first contract shared by the public `kazeflow` CLI.

## Requirements

### Requirement: CLI-first command surface

The public executable SHALL be `kazeflow`, not `kz`. `assets`, `plan`, and `run`
SHALL supplement the existing Python APIs.

#### Scenario: Invoke the public CLI
- **WHEN** a caller invokes a supported `kazeflow` command
- **THEN** the command supplements the corresponding Python API behavior

### Requirement: Script-first entry resolution

The CLI SHALL accept bare Python file paths, `module:attribute`, and
`file.py:attribute`. An explicit attribute may be a `Flow` or an explicitly named,
zero-argument factory returning a `Flow`, invoked at most once. Entry resolution
failures SHALL exit `3`; invalid syntax SHALL exit `2`.

#### Scenario: Resolve an explicit flow
- **WHEN** an explicit entry resolves to a `Flow` or zero-argument factory
- **THEN** the CLI resolves it under the documented failure classification

### Requirement: Asset discovery and target selection

For bare files, the CLI SHALL list assets registered during loading. A module-level
`flow` supplies default targets; otherwise terminal discovered assets are suggested.
An ambiguous derived `run` without `--target` SHALL exit `2` without invoking an
asset body.

#### Scenario: Plan derived terminal targets
- **WHEN** a bare script has no declared flow and a caller plans it without targets
- **THEN** the CLI uses its discovered terminal assets

### Requirement: Honest review safety boundary

The CLI SHALL document that entry loading and explicit factories execute arbitrary
user Python and may have top-level side effects. `plan` SHALL not invoke an asset
body and SHALL not be represented as sandboxing or automatic approval.

#### Scenario: Inspect trusted code
- **WHEN** a caller plans a side-effectful entry
- **THEN** no asset body is invoked by planning and loading remains distinct

### Requirement: Existing flow semantics

CLI plans and runs SHALL use existing flow planning and result semantics. A run
uses the same resolved entry and selections within one invocation, but need not
consume the exact `FlowPlan` object printed during preflight.

#### Scenario: Reuse resolved selections
- **WHEN** a caller reviews and runs within one invocation
- **THEN** the resolved entry and selected options are retained

### Requirement: Output and status classification

JSON success output SHALL be exactly one stdout document; diagnostics SHALL use
stderr. Portable JSON SHALL exclude raw outputs, exception objects, and raw
partition keys. Statuses SHALL be `0` success, `1` completed asset failure, `2`
usage/configuration failure, `3` entry resolution failure, and `4` infrastructure
or selected-adapter failure. Text output SHALL be a human-facing review projection,
not a byte-for-byte automation contract; graph and detail format selection SHALL
follow the public CLI compatibility policy.

#### Scenario: Emit portable JSON
- **WHEN** a successful command selects JSON
- **THEN** stdout contains one lossy JSON document and diagnostics use stderr

#### Scenario: Reject an invalid output selection
- **WHEN** a caller combines incompatible documented output options
- **THEN** the command exits `2` and writes its diagnostic only to stderr

### Requirement: Explicit optional features

The default CLI path SHALL not initialize Rich, create a database, or persist a
record. Rich and SQLite persistence require explicit selection; a persistence or
event-consumer failure after a terminal result takes precedence over asset failure.

#### Scenario: Use the default core path
- **WHEN** no optional CLI feature is selected
- **THEN** Rich and SQLite persistence are not initialized

### Requirement: Run detail option preserves output boundaries

The public `run` command SHALL support a text-only `--verbose` option for terminal
result detail. JSON output SHALL remain exactly one portable `RunResult` document
on stdout, and an incompatible verbose/JSON selection SHALL be classified as usage
error before entry loading.

#### Scenario: Preserve JSON automation output
- **WHEN** a caller requests a successful JSON run without verbose detail
- **THEN** stdout contains exactly the portable result document and no text summary
