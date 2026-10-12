## ADDED Requirements

### Requirement: Resource-aware plan documents use a new schema version
After this change, every emitted `kazeflow.plan` document SHALL use schema version 2. Its published schema SHALL include deterministic selected-resource summaries and each task's resource association, with explicit empty or null values when no resources are used. The version 1 schema and golden documents SHALL remain published and unchanged for existing consumers, but the CLI need not continue emitting version 1. The release documentation SHALL explain the migration from version 1 to version 2. Other CLI document types SHALL retain their current versions.

#### Scenario: Resource-aware plan
- **WHEN** an automation requests a JSON plan for two assets sharing a resource
- **THEN** it receives a `kazeflow.plan` version 2 document that identifies one shared limit and both task associations

#### Scenario: No resources
- **WHEN** an automation requests a JSON plan for a flow with no resource declarations
- **THEN** it receives a version 2 document with an explicit empty resource summary and null task associations
