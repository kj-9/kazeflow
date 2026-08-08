## ADDED Requirements

### Requirement: Rich rendering presents plan-aware live progress

The optional Rich renderer SHALL accept safe descriptors for the selected resolved
plan in addition to neutral execution events. During an explicitly selected CLI TUI
run, it SHALL present overall progress and each planned task as waiting, running,
succeeded, skipped, or failed. It SHALL use only task names, partition presence or
count metadata, and event status or duration for that presentation; it SHALL not
read raw outputs, exception objects, or executor scheduling state.

#### Scenario: Show work that has not started
- **WHEN** an interactive run has a resolved plan with a task whose attempt has not
  started
- **THEN** the renderer identifies that task as waiting without inspecting executor
  internals

#### Scenario: Show a terminal task state
- **WHEN** the renderer receives a terminal task event for a planned task
- **THEN** it replaces that task's waiting or running state with the corresponding
  succeeded, skipped, or failed state and advances overall progress once

### Requirement: Interactive rendering remains an optional stderr consumer

The CLI SHALL initialize the enhanced renderer only for an explicit `--tui`
selection and SHALL route its live display to stderr. Core-only execution and JSON
stdout SHALL remain independent of Rich presentation.

#### Scenario: Leave a default run quiet
- **WHEN** a caller runs without `--tui`
- **THEN** no Rich module or live progress display is initialized
