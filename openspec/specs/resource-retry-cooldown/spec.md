# resource-retry-cooldown Specification

## Purpose
Allow explicitly classified transient resource failures to retry within one local flow run while releasing waiting slots and coordinating shared cooldown with concurrency and rolling rate admission.

## Requirements

### Requirement: Bounded explicit failure classification
Resource policies SHALL declare a positive maximum invocation count including the first invocation and an optional synchronous classifier returning fail, explicit circuit opening, or retry with finite non-negative seconds and an explicit shared-cooldown flag. Defaults SHALL be one invocation and no classifier. Only ordinary asset-body exceptions SHALL be classified; cancellation, observer failures, preparation/load/commit errors SHALL NOT be retried. Classifier failure or invalid decision SHALL terminally fail the logical attempt with diagnostic metadata. A retry decision SHALL require remaining budget to schedule another invocation; shared cooldown SHALL apply even when that failing attempt exhausted its budget. No classifier SHALL imply no automatic retry.

#### Scenario: Transient failure recovers
- **WHEN** a classifier requests retry and the second body invocation succeeds within budget
- **THEN** the logical attempt succeeds, its body invocation history preserves the first failure, and downstream receives only the successful output

#### Scenario: Exhausted or permanent failure
- **WHEN** the classifier returns fail or retry without remaining budget
- **THEN** the logical attempt fails once with final failure metadata and dependencies become blocked

#### Scenario: Invalid policy or classification
- **WHEN** a policy is malformed before execution or a classifier returns an invalid decision during execution
- **THEN** the policy fails preflight without events, or the affected logical attempt terminally fails without repeated retries, respectively

### Requirement: Waiting releases slots and reacquires admission
Every body invocation SHALL acquire global and resource concurrency capacity and a fresh rate permit. Failed starts SHALL remain charged to the rolling rate window. Retry backoff SHALL not occupy global or resource capacity. A logical attempt SHALL wait for its own delay, the resource cooldown, and rate/concurrency eligibility; unrelated resources SHALL continue. Shared cooldown SHALL extend to the later deadline when multiple failures request it and SHALL delay newly admitted body invocations, without cancelling existing work. Retry and cooldown state SHALL reset on each run. Cancellation while waiting SHALL propagate without scheduling another invocation or fabricating a terminal result.

#### Scenario: Unrelated progress during backoff
- **WHEN** one logical attempt waits to retry under a global limit of one
- **THEN** unrelated ready work runs before that retry

#### Scenario: Shared cooldown and rolling rate
- **WHEN** a failed invocation requests shared cooldown while another asset uses the same resource
- **THEN** newly admitted bodies wait until both cooldown and rate windows permit them, and every retry consumes a new start

#### Scenario: Concurrent cooldown requests
- **WHEN** overlapping failures request different cooldown durations
- **THEN** the later resource deadline governs new admissions and running bodies are allowed to complete

#### Scenario: Cancellation and fresh run
- **WHEN** a waiting run is cancelled and a new run starts
- **THEN** cancellation produces no synthetic result and the new run inherits no previous retry/cooldown state

### Requirement: Materialization and logical terminal outcomes remain explicit
One logical result and logical start/finish lifecycle SHALL remain associated with each selected partition key. Enabled resource policies SHALL retain numbered invocation outcomes separate from those logical results, and each body SHALL receive its one-based invocation number in context. A materialization hit SHALL have no body invocation history or permits. Retry of a materialization miss SHALL retain the prepared input identity; commit failure SHALL fail without repeating the body. Successful output SHALL become visible only after commit. Falsey keys and deterministic selection order SHALL remain unchanged.

#### Scenario: Materialization hit during cooldown
- **WHEN** a compatible committed output exists while its resource is cooling down
- **THEN** the output is loaded without a body invocation or rate/concurrency permit

#### Scenario: Commit failure after successful body
- **WHEN** a numbered invocation returns output but its materialization commit fails
- **THEN** its history records the failed body-plus-commit operation, the logical attempt fails, and the body is not retried
