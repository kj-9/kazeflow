## ADDED Requirements

### Requirement: Executor resolves committed materializations before invocation
For a normally completed run, each selected partitioned attempt with materialization SHALL produce exactly one terminal result. A reusable committed record SHALL supply its loaded output to the same-key dependent without invoking the asset. Otherwise the executor SHALL invoke the asset once and commit its output before that output becomes available. Independent partitions and branches SHALL retain their current failure-continuation behavior, and result order SHALL remain plan/selection order.

#### Scenario: One key is reusable and another executes
- **WHEN** a partitioned asset has a compatible committed record for one selected key but not another
- **THEN** it loads one key, invokes and commits the other once, and preserves both selected attempt positions

#### Scenario: Uncommitted output does not flow downstream
- **WHEN** commit fails after an asset body returns
- **THEN** the matching downstream attempt is dependency-blocked while independent work may continue
