## Purpose

Define explicit, optional Rich terminal rendering that consumes neutral execution
events without changing core execution behavior.

## Requirements

### Requirement: Rich terminal rendering is an explicit event consumer
The optional TUI module SHALL expose a Rich-backed execution-event consumer that a
caller explicitly constructs and passes to `run()` or `Flow.run_async()`.  Importing
or constructing that renderer SHALL be the only action in this change that imports
Rich.  The renderer SHALL derive its displayed flow, task, and attempt progress solely
from `ExecutionEvent` fields and SHALL not access executor scheduling state, renderer
progress identifiers, `Flow.asset_outputs`, raw asset outputs, or raw exception
objects.

#### Scenario: A caller opts into Rich rendering
- **WHEN** a caller constructs the optional renderer, enters any required presentation
  context, and passes it as `event_consumer`
- **THEN** the renderer receives the run event stream and shows lifecycle progress
  without changing the returned `RunResult`

#### Scenario: Renderer state does not own execution state
- **WHEN** a run has partitioned attempts, a failed attempt, or a dependency-blocked
  task
- **THEN** the renderer can represent the received terminal events without reading a
  raw output, exception, or executor progress identifier

### Requirement: Optional rendering preserves core execution semantics
Selecting the Rich renderer SHALL not alter plan validation, selected work,
concurrency bounds, partition selection, dependency blocking, cancellation behavior,
event ordering, result ordering, terminal statuses, or failure metadata.  A renderer
failure is governed by the execution-events consumer-failure contract and SHALL NOT be
reported as an asset failure.

#### Scenario: Rendered and unrendered executions agree
- **WHEN** equivalent valid flows run once without a consumer and once with the Rich
  renderer
- **THEN** their returned results have the same plan order, task/attempt terminal
  statuses, partition identities, and failure metadata

#### Scenario: A renderer cannot convert an asset result
- **WHEN** an asset fails while the Rich renderer is selected
- **THEN** the run result records the asset failure exactly as it would without the
  renderer

### Requirement: Legacy automatic Rich behavior has an explicit migration path
Core execution SHALL not automatically create or enter a Rich renderer.  The Rich
renderer SHALL be available only to callers that install the `tui` optional extra,
import the optional renderer module, and explicitly select it as an event consumer.
Documentation and public renderer usage SHALL direct callers that relied on legacy
automatic terminal display to install `kazeflow[tui]` and explicitly select the
renderer.  A core-only installation SHALL retain plan/run/result behavior without
importing Rich.

#### Scenario: Default core execution remains quiet
- **WHEN** a caller installs the base distribution and runs a flow without importing
  or passing the optional renderer
- **THEN** execution emits no terminal UI and still returns its `RunResult` without
  requiring Rich

#### Scenario: A legacy TUI caller migrates
- **WHEN** a caller previously relied on automatic Rich display or imports
  `kazeflow.tui`
- **THEN** the documented migration is to install `kazeflow[tui]`, construct the
  renderer, and pass it explicitly as the event consumer

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
