## ADDED Requirements

### Requirement: Body admission respects shared temporal constraints
The executor SHALL defer body execution until both resource rate admission and concurrency capacity are available, while retaining preflight, dependency blocking, ordinary failure continuation, materialization commit-before-downstream, and external cancellation behavior. Waiting for rate admission SHALL not publish output or produce a terminal result prematurely.

#### Scenario: Downstream waits for a committed output
- **WHEN** a materialization miss awaits a rate permit and later executes and commits
- **THEN** matching downstream work becomes eligible only after the successful commit

#### Scenario: An independent failure does not abandon delayed work
- **WHEN** an independent branch fails while a valid body awaits admission
- **THEN** the delayed body still executes once and the completed result includes both outcomes
