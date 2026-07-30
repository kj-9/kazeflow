## MODIFIED Requirements

### Requirement: Core installation smoke demonstrates the public workflow
The project SHALL maintain an installed-wheel smoke test for the base distribution
that uses only public core APIs.  The smoke SHALL verify `import kazeflow`, plan
creation without asset invocation, successful execution, and structured RunResult
retrieval.  It SHALL not import `kazeflow.tui` or depend on a third-party runtime
package.  Public release documentation SHALL provide a command that builds the
wheel and invokes this core-only smoke outside the source checkout.

#### Scenario: Planning does not execute an installed asset
- **WHEN** the installed-wheel smoke creates a flow and obtains its plan before run
- **THEN** its asset side effect has not occurred and the plan describes the selected
  task

#### Scenario: Running returns a structured result
- **WHEN** the installed-wheel smoke runs its planned flow
- **THEN** it receives a successful RunResult with the task's successful terminal
  attempt and expected output

#### Scenario: A release reviewer invokes the core-only smoke
- **WHEN** a release reviewer follows the documented core-only smoke command after
  building a wheel
- **THEN** it exercises the installed wheel's public plan/run/result workflow
  without selecting the TUI extra
