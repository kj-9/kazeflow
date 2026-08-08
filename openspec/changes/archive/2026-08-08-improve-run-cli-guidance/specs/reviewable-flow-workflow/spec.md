## ADDED Requirements

### Requirement: Documentation provides a CLI quick-start and partition-aware review loop
Public documentation SHALL provide a concise CLI quick-start from installation to
script definition, plan review, deliberate run, and terminal result inspection. It
SHALL include a partition-aware variation that reviews selection before execution
and reiterates the trusted-Python loading boundary.

#### Scenario: A script-first user adopts the CLI workflow
- **WHEN** a new user follows the documented quick-start
- **THEN** they can create a module-level flow, inspect it before asset invocation,
  execute it deliberately, and understand where to inspect its terminal outcome
