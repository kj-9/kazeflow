## MODIFIED Requirements

### Requirement: Resource-aware plan documents use a new schema version
Every emitted `kazeflow.plan` document SHALL use schema version 3. Its published schema SHALL include deterministic selected-resource summaries, each task's resource association, and a nullable rate limit with body-start count and seconds. Empty resource summaries and null task associations SHALL be explicit. Version 1 and 2 schemas and golden documents SHALL remain published and unchanged for historical consumers; the CLI need not emit older versions. Release documentation SHALL explain migration to version 3. Other CLI document types SHALL retain their current versions.

#### Scenario: Resource-aware plan
- **WHEN** an automation requests a JSON plan for two assets sharing a resource
- **THEN** it receives a version 3 plan with the shared concurrency limit, optional rate limit, and task associations

#### Scenario: No resources
- **WHEN** an automation requests a JSON plan for a flow without resources
- **THEN** it receives a version 3 document with an empty resource summary and null task associations

#### Scenario: No rate limit
- **WHEN** a selected resource has only a concurrency bound
- **THEN** its version 3 projection contains an explicit null rate limit
