## MODIFIED Requirements

### Requirement: Resource-aware plan documents use a new schema version
Every emitted `kazeflow.plan` document SHALL use schema version 4. Its published schema SHALL include deterministic selected-resource summaries, each task's resource association, and a nullable rate limit with body-start count and seconds. Empty resource summaries and null task associations SHALL be explicit. Version 1, 2, and 3 schemas and golden documents SHALL remain published and unchanged for historical consumers; the CLI need not emit older versions. Release documentation SHALL explain migration to version 4. Selected summaries SHALL also include maximum invocations and classifier-enabled metadata. Run-result, runs-list, runs-show and runs-compare documents SHALL use version 2, with run-result portable record version 2 and stored-record versions 1 or 2. Other document types SHALL retain version 1; SQLite database version SHALL remain 1. Historical schemas/goldens SHALL remain unchanged.

#### Scenario: Resource-aware plan
- **WHEN** an automation requests a JSON plan for two assets sharing a resource
- **THEN** it receives a version 4 plan with the shared concurrency limit, optional rate limit, and task associations

#### Scenario: No resources
- **WHEN** an automation requests a JSON plan for a flow without resources
- **THEN** it receives a version 4 document with an empty resource summary and null task associations

#### Scenario: No rate limit
- **WHEN** a selected resource has only a concurrency bound
- **THEN** its version 4 projection contains an explicit null rate limit
