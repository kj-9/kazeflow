## Purpose

Stop further invocations against an explicitly classified unusable resource within one run, with attributable outcomes and preserved independent progress.

## ADDED Requirements

### Requirement: Circuit opening is explicit and run-local
A synchronous failure classifier SHALL be able to return `OpenCircuit()` after an ordinary body exception. The source attempt SHALL finish failed with its original failure; its resource SHALL remain open until this run ends. Ordinary Fail, classifier errors, preparation/load/commit failures, cancellation and observer errors SHALL NOT open circuits. The first observed source SHALL remain the source if already-running work also requests opening. Each new run SHALL start closed.

#### Scenario: Fatal resource error opens only its resource
- **WHEN** a body classifier requests circuit opening
- **THEN** that attempt fails, its resource opens, and independent resources continue

#### Scenario: Another run starts fresh
- **WHEN** a flow is run after a previous circuit opening
- **THEN** its resource is initially closed

### Requirement: Pending body work is skipped with causal attribution
Pending same-resource invocations, including delayed retries and prepared misses, SHALL become skipped with `resource_circuit_open` and exactly the source attempt in `blocked_by`. Dependency-blocked attempts SHALL retain dependency reasons. Already-admitted body operations SHALL complete normally and SHALL NOT be cancelled by opening; retries they request SHALL be skipped. Waiting SHALL consume no invocation permit. Selection order, falsey keys and exactly-once logical finish SHALL be preserved.

#### Scenario: Retrying work stops without another body call
- **WHEN** an attempt waits for retry and another attempt opens its resource
- **THEN** the waiting attempt is circuit-skipped retaining its prior invocation history

#### Scenario: Running work completes
- **WHEN** a peer is already admitted when the resource opens
- **THEN** it can finish and commit normally but cannot start another invocation

### Requirement: Materialization hits remain available
After dependencies succeed, enabled materialization preparation and hit loading SHALL remain eligible while a resource is open. Hits SHALL succeed without a body invocation; misses SHALL be circuit-skipped. Preparation or loading errors SHALL retain ordinary failed outcomes. No callback SHALL be classified for these adapter operations.

#### Scenario: Available committed artifact bypasses open circuit
- **WHEN** a pending attempt resolves a valid materialization hit after opening
- **THEN** it succeeds without acquiring a resource body permit
