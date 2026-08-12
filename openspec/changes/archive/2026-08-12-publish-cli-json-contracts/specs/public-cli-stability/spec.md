## MODIFIED Requirements

### Requirement: Public CLI compatibility policy
The system SHALL publish `kazeflow` command names, documented options, exit-status
mapping, and typed JSON document schemas as the public CLI interface. Every
completed JSON document SHALL identify its document type and independently scoped
schema version. A compatible release SHALL not remove or rename one of those
interfaces, or make an incompatible change to a published document type/version,
without a documented deprecation and migration path. Human-readable text SHALL
preserve its documented meaning but is not a byte-for-byte automation interface.

#### Scenario: Consume a portable JSON result
- **WHEN** an automation invokes a documented command with `--format json`
- **THEN** it receives one documented JSON envelope whose type and version can be
  determined without parsing text layout or knowing the command invocation

#### Scenario: Change a public option
- **WHEN** a future release intends to remove or rename a documented option or make
  an incompatible change to a published JSON document version
- **THEN** it documents a prior deprecation, migration path, and release-note entry

### Requirement: Interactive progress is an explicit presentation mode
The system SHALL render live execution progress only when a caller explicitly
selects `kazeflow run --tui` with the optional TUI feature available. The display
SHALL identify overall completion and safe task-level waiting, running, succeeded,
skipped, or failed states. It SHALL use stderr and SHALL not change execution
selection, event ordering, `RunResult`, typed JSON stdout, or exit-status
classification.

#### Scenario: Observe a successful interactive run
- **WHEN** a caller runs a multi-task flow with `--tui`
- **THEN** the display presents overall completion and task states while the final
  result retains the same statuses as an equivalent non-TUI run

#### Scenario: Preserve JSON stdout during interactive progress
- **WHEN** a caller selects both `--tui` and `--format json`
- **THEN** live progress is written to stderr and stdout contains only the terminal
  typed portable result document
