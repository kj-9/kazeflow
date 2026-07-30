## MODIFIED Requirements

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
