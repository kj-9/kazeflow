## ADDED Requirements

### Requirement: Plans expose selected resource rate limits
An immutable FlowPlan SHALL expose the start count and duration of each selected resource's optional rate limit. Absence SHALL be explicit in structured metadata. Equivalent definitions SHALL produce deterministic metadata, and planning SHALL not acquire permits, resolve materializations, or invoke bodies.

#### Scenario: Side-effect-free rate inspection
- **WHEN** a caller plans a rate-limited materialized flow
- **THEN** the limit is visible without adapter access or asset execution
