## MODIFIED Requirements

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
