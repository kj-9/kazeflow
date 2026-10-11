## MODIFIED Requirements

### Requirement: Resource-aware plan documents use a new schema version
Every emitted `kazeflow.plan` document SHALL use schema version 4 with selected-resource summaries, task associations, nullable rate limit, maximum invocations and classifier-enabled metadata. Empty resources and null associations SHALL be explicit. Run-result, runs-list, runs-show and runs-compare documents SHALL use version 4 with run-result portable record version 4 and stored-record versions 1–4. Version 4 SHALL preserve circuit reasons and invocation history and expose successful materialization provenance. Other document types SHALL retain version 1; SQLite database version SHALL remain 1. Published schema collection v6 SHALL describe the current matrix. Historical schema collections v1–v5 and goldens SHALL remain unchanged. Release documentation SHALL explain migration.

#### Scenario: Resource-aware plan
- **WHEN** an automation requests a JSON plan for shared-resource assets
- **THEN** it receives a version 4 plan with concurrency, nullable rate limit and associations

#### Scenario: No resources
- **WHEN** a JSON plan has no selected resources
- **THEN** it has empty resource summaries and null associations

#### Scenario: No rate limit
- **WHEN** a selected resource has only a concurrency bound
- **THEN** its plan contains an explicit null rate limit

#### Scenario: Provenance in run and stored history
- **WHEN** JSON run succeeds using committed or reused materialization
- **THEN** version 4 documents carry record version 4 metadata and history can also show unchanged older records
