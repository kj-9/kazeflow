## Purpose

Define the documented, caller-controlled workflow for reviewing a human- or
AI-authored flow before execution and assessing its structured outcome afterward.

## Requirements

### Requirement: Documentation presents a pre-execution review workflow
Public documentation SHALL present a core-only workflow in this order: define or
select a flow, obtain `FlowPlan`, review it, run the selected targets only after
review, and inspect the returned `RunResult`.  It SHALL include a minimal runnable
example and a review-oriented example suitable for a human to inspect code written
by either a person or an AI.  Both examples SHALL use public core APIs and SHALL NOT
require `kazeflow.tui`.

#### Scenario: A minimal user follows the workflow
- **WHEN** a user follows the minimal documented example
- **THEN** they can define a flow, obtain a plan before invoking an asset, run it,
  and inspect a successful structured result using a core-only installation

#### Scenario: A reviewer assesses an authored flow before execution
- **WHEN** a reviewer receives a human- or AI-authored flow and follows the
  review-oriented example
- **THEN** the example directs them to inspect selected targets, dependency-first
  tasks, partition selection, and relevant run configuration from `FlowPlan` before
  choosing whether to run

### Requirement: Documentation distinguishes plan, result, and logs
Public documentation SHALL state that `FlowPlan` is structured pre-execution
information and does not invoke asset code; `RunResult` is structured terminal
outcome information for one run; and logs are optional time-ordered detail for
progress or diagnosis.  It SHALL state that logs do not replace either plan review
or result inspection.

#### Scenario: A user needs the right source of information
- **WHEN** documentation explains the workflow information available before, during,
  and after execution
- **THEN** it identifies FlowPlan for pre-run review, logs for optional live detail,
  and RunResult for terminal status, timing, outputs, and failure metadata

### Requirement: Reviewability has an explicit safety boundary
Public documentation SHALL state that planning and review help a caller understand
the declared flow structure but do not sandbox asset code, prove safety, prevent
side effects, or automatically approve execution.  The caller SHALL remain
responsible for reviewing code and deciding whether to run it.

#### Scenario: A reviewer encounters untrusted or AI-generated code
- **WHEN** documentation describes reviewing a flow authored by an AI or another
  person
- **THEN** it states that the workflow is review support only and is not a security
  boundary for untrusted Python

### Requirement: Documentation provides a CLI quick-start and partition-aware review loop

Public documentation SHALL provide a concise CLI quick-start from installation to
script definition, plan review, deliberate run, and terminal result inspection. It
SHALL include a partition-aware variation that reviews selection before execution
and reiterates the trusted-Python loading boundary.

#### Scenario: A script-first user adopts the CLI workflow
- **WHEN** a new user follows the documented quick-start
- **THEN** they can create a module-level flow, inspect it before asset invocation,
  execute it deliberately, and understand where to inspect its terminal outcome
