# Public CLI stability

## Purpose

Define compatibility, legible plan projection, and release verification for the
published `kazeflow` CLI.

## Requirements

### Requirement: Public CLI compatibility policy
The system SHALL publish `kazeflow` command names, documented options, exit-status
mapping, and JSON schemas as the public CLI interface. A compatible release SHALL
not remove or rename one of those interfaces without a documented deprecation and
migration path. Human-readable text SHALL preserve its documented meaning but is
not a byte-for-byte automation interface.

#### Scenario: Consume a portable JSON result
- **WHEN** an automation invokes a documented command with `--format json`
- **THEN** it receives the documented one-document JSON schema without requiring
  parsing of text layout

#### Scenario: Change a public option
- **WHEN** a future release intends to remove or rename a documented option
- **THEN** it documents a prior deprecation, migration path, and release-note entry

### Requirement: Legible default plan rendering
The system SHALL render a successful text `plan` as a concise plan summary followed
by a deterministic ASCII dependency graph for the selected task closure. The
rendering SHALL identify selected targets, preserve each selected dependency edge,
and identify whether each task is partitioned. Default text SHALL avoid raw
partition-key values.

#### Scenario: Render a linear plan
- **WHEN** a plan has one selected target with a linear three-task dependency chain
- **THEN** text output displays the target, all three tasks, and both dependency
  edges in dependency-first graph order

#### Scenario: Render a branching plan
- **WHEN** a plan has branches that merge into a selected target
- **THEN** text output represents every branch and merge deterministically without
  duplicating a task as independent work

### Requirement: Export resolved plan graphs
The system SHALL support `kazeflow plan ENTRY --format mermaid` and `--format dot`.
Each successful command SHALL emit exactly one deterministic graph document to
stdout for the same resolved selected plan, with diagnostics on stderr. It SHALL
not invoke an external renderer or require Graphviz, Mermaid, Rich, or SQLite.

#### Scenario: Export Mermaid
- **WHEN** a caller selects Mermaid format for a resolved plan
- **THEN** stdout contains a Mermaid flowchart with the selected task nodes and
  dependency edges in deterministic order

#### Scenario: Export DOT
- **WHEN** a caller selects DOT format for a resolved plan
- **THEN** stdout contains a DOT directed graph with the selected task nodes and
  dependency edges in deterministic order

### Requirement: Deliberate plan detail selection
The system SHALL provide a documented text-mode mechanism for expanding normalized
configuration, task dependencies, and partition metadata without changing the
default summary. It SHALL reject a detail option combined with a non-text graph or
JSON format as a usage error.

#### Scenario: Expand a text plan
- **WHEN** a caller requests the documented detail option with text plan output
- **THEN** the text output includes the additional plan metadata

#### Scenario: Reject ambiguous detail output
- **WHEN** a caller combines the detail option with Mermaid, DOT, or JSON output
- **THEN** the command exits `2`, writes a diagnostic to stderr, and writes no
  successful document to stdout

### Requirement: Installed CLI release verification
The system SHALL verify the public CLI from an installed wheel on supported Python
versions and SHALL cover the core-only path plus explicitly selected optional TUI
and SQLite paths. Release documentation SHALL state the supported versions and the
stable review and CI invocation patterns.

#### Scenario: Verify a core-only installed wheel
- **WHEN** release validation installs the wheel without optional extras
- **THEN** `kazeflow assets`, `plan`, and `run` operate for a script-defined flow

### Requirement: Interactive progress is an explicit presentation mode
The system SHALL render live execution progress only when a caller explicitly
selects `kazeflow run --tui` with the optional TUI feature available. The display
SHALL identify overall completion and safe task-level waiting, running, succeeded,
skipped, or failed states. It SHALL use stderr and SHALL not change execution
selection, event ordering, `RunResult`, JSON stdout, or exit-status classification.

#### Scenario: Observe a successful interactive run
- **WHEN** a caller runs a multi-task flow with `--tui`
- **THEN** the display presents overall completion and task states while the final
  result retains the same statuses as an equivalent non-TUI run

#### Scenario: Preserve JSON stdout during interactive progress
- **WHEN** a caller selects both `--tui` and `--format json`
- **THEN** live progress is written to stderr and stdout contains only the terminal
  portable JSON result
