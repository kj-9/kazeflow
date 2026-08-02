## ADDED Requirements

### Requirement: Review before a CLI execution decision
`kazeflow run` SHALL resolve its entry, targets, and supported run configuration
under the inherited `flow-cli-contract` and `flow-plan-cli` contracts, and SHALL
build a pre-execution plan before it invokes an asset body.  The command SHALL
write a human-readable summary of that selected plan to stderr before it asks
for confirmation or starts execution.  The summary SHALL identify the selected
targets, dependency-first work, partition selection, and normalized execution
configuration sufficiently for a caller to review the requested work.

Within one invocation, the command SHALL use the same resolved entry and
selected options for its preflight and execution.  It SHALL NOT promise that
the executor consumes the same `FlowPlan` object that produced the preflight,
because the existing execution API may plan again.  Entry loading and an
explicitly selected factory retain the inherited arbitrary-Python side-effect
boundary; preflight itself SHALL NOT invoke an asset body.

#### Scenario: Review a selected run before execution
- **WHEN** a caller invokes `kazeflow run` with a valid entry, explicit target,
  valid supported configuration, and an execution decision that permits running
- **THEN** stderr first contains a summary of the selected plan and the command
  invokes asset bodies only after that review stage completes

#### Scenario: Preserve the plan safety boundary during preflight
- **WHEN** a caller invokes `kazeflow run` for a script whose asset body would
  have an observable side effect if called
- **THEN** producing the pre-execution summary does not cause that asset-body
  side effect

### Requirement: Require an explicit decision to execute
When both stdin and stderr are TTYs, `kazeflow run` SHALL prompt on stderr for
an execution decision after its pre-execution summary.  It SHALL execute only
when the caller responds `y` or `yes`, case-insensitively; every other response
and EOF SHALL be a deliberate decline.  When either stdin or stderr is not a
TTY, the command SHALL require `--yes` and SHALL NOT prompt.  `--yes` SHALL
permit execution without an interactive prompt in either terminal mode.

A deliberate decline SHALL be a successful no-op: it exits with status `0`,
does not invoke an asset body, does not initialize a selected TUI or store,
does not create a `RunResult`, and writes a cancellation diagnostic to stderr.

#### Scenario: Confirm from an interactive terminal
- **WHEN** stdin and stderr are TTYs, a valid `run` invocation omits `--yes`,
  and the caller responds `yes` to the prompt
- **THEN** the command proceeds to execute the selected flow

#### Scenario: Decline from an interactive terminal
- **WHEN** stdin and stderr are TTYs and the caller responds anything other
  than `y` or `yes`, or supplies EOF, to the confirmation prompt
- **THEN** the command exits with status `0` without running assets, creating a
  run result, initializing a TUI or store, or emitting a terminal run result

#### Scenario: Reject missing non-interactive approval
- **WHEN** either stdin or stderr is not a TTY and a caller invokes `run`
  without `--yes`
- **THEN** the command writes a diagnostic to stderr, does not prompt or invoke
  an asset body, and exits with status `2`

### Requirement: Emit completed run results with separate machine output
After a confirmed execution reaches a terminal `RunResult`, `kazeflow run`
SHALL emit a deterministic human-oriented result summary in text mode.  With
`--format json`, stdout SHALL contain exactly one JSON document: the portable,
lossy `RunResult` record.  The JSON record SHALL retain the inherited run-record
boundary and SHALL NOT expose raw asset outputs, exception objects, or raw
partition-key values.  The pre-execution summary, confirmation prompt,
cancellation notice, progress presentation, and diagnostics SHALL use stderr
so they do not mix with JSON stdout.

#### Scenario: Emit one portable JSON result
- **WHEN** a confirmed run completes and the caller selects `--format json`
- **THEN** stdout contains exactly one portable `RunResult` JSON document and
  stderr contains the preflight review and any human-oriented interaction

#### Scenario: Report an asset failure as a completed result
- **WHEN** a confirmed run reaches a terminal `RunResult` with one or more
  failed asset attempts and no selected adapter fails
- **THEN** the command emits that completed result and exits with status `1`

### Requirement: Keep TUI presentation and SQLite persistence explicit
By default, `kazeflow run` SHALL use the core execution path and SHALL NOT
import or initialize Rich, create a SQLite database, or persist a run record.
`--tui` SHALL explicitly select the optional Rich event presentation path and
MUST fail with status `4` if the TUI extra is unavailable or the selected
consumer cannot be initialized or used.  The command SHALL initialize the
selected TUI only after execution has been approved and before it begins the
run.

`--store PATH` SHALL explicitly select SQLite persistence.  The command SHALL
construct and use the selected store only after a terminal `RunResult` is
available, and SHALL save that result before emitting it as a successful final
CLI result.  A selected store failure SHALL exit with status `4`, emit its
diagnostic to stderr, and suppress a successful final result document.  When a
selected TUI or store fails after or alongside an asset failure, the adapter or
infrastructure failure SHALL take precedence over status `1`.

#### Scenario: Run on the core-only default path
- **WHEN** a caller confirms a valid `run` invocation without `--tui` or
  `--store`
- **THEN** the command executes without initializing Rich or SQLite persistence

#### Scenario: Select an unavailable TUI
- **WHEN** a caller confirms a run with `--tui` but the optional TUI feature is
  unavailable
- **THEN** the command reports the selected-adapter failure on stderr, does not
  invoke an asset body, emits no successful final result, and exits with status
  `4`

#### Scenario: Fail to store a completed asset failure
- **WHEN** a confirmed run reaches a failed terminal `RunResult` and the
  explicitly selected SQLite store then fails
- **THEN** the command reports the store failure on stderr, emits no successful
  final result document, and exits with status `4`

### Requirement: Preserve inherited CLI failure classifications
`kazeflow run` SHALL use status `0` for a successful completed run and for a
deliberate declined confirmation; status `1` for a completed terminal run with
asset failure; status `2` for invalid command syntax, supplied configuration,
or missing required non-interactive `--yes`; status `3` for entry-resolution
failure; and status `4` for execution infrastructure or an explicitly selected
adapter failure.  A failure before a terminal result SHALL NOT be represented as
a completed `RunResult`.  In JSON mode, configuration, entry-resolution, and
infrastructure failures SHALL write diagnostics only to stderr and SHALL not
emit a successful run document.

#### Scenario: Reject an ambiguous discovered run target
- **WHEN** a bare undeclared script has multiple inherited suggested terminal
  targets and a caller supplies neither `--target` nor a valid disambiguating
  selection to `run`
- **THEN** the command does not invoke an asset body, writes a diagnostic to
  stderr, and exits with status `2`

#### Scenario: Report an entry-resolution failure before review
- **WHEN** a caller supplies an unloadable entry or an explicit entry that does
  not resolve to a `Flow` under the inherited entry-resolution contract
- **THEN** the command emits no pre-execution summary or run result, writes a
  diagnostic to stderr, and exits with status `3`
