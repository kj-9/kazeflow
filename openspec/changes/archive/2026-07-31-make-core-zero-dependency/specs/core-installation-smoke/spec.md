## ADDED Requirements

### Requirement: Core installation smoke demonstrates the public workflow
The project SHALL maintain an installed-wheel smoke test for the base distribution
that uses only public core APIs.  The smoke SHALL verify `import kazeflow`, plan
creation without asset invocation, successful execution, and structured RunResult
retrieval.  It SHALL not import `kazeflow.tui` or depend on a third-party runtime
package.

#### Scenario: Planning does not execute an installed asset
- **WHEN** the installed-wheel smoke creates a flow and obtains its plan before run
- **THEN** its asset side effect has not occurred and the plan describes the selected
  task

#### Scenario: Running returns a structured result
- **WHEN** the installed-wheel smoke runs its planned flow
- **THEN** it receives a successful RunResult with the task's successful terminal
  attempt and expected output

### Requirement: Documentation states the installation boundary
Public README installation and example guidance SHALL distinguish the base core
installation from the optional TUI installation.  It SHALL show the plan-before-run
and result-after-run workflow using core APIs, and SHALL state that Rich rendering
requires the `tui` extra and explicit renderer selection.

#### Scenario: A new core user follows the documented example
- **WHEN** a user follows the README core installation and minimal workflow
- **THEN** they need no third-party runtime package to inspect a plan, execute a
  flow, and inspect its result

#### Scenario: A user wants terminal rendering
- **WHEN** a user follows the README Rich rendering guidance
- **THEN** it instructs them to install `kazeflow[tui]` before importing the optional
  renderer and to pass that renderer explicitly
