## ADDED Requirements

### Requirement: Selected retry policies are inspectable without execution
Selected resource summaries SHALL include the maximum invocation count and whether a classifier is enabled, without executing or serializing the classifier. Malformed declarations SHALL fail planning before any events or body execution. Policies not selected SHALL remain absent from selected summaries.

#### Scenario: Inspect retry budget
- **WHEN** a selected resource declares a classifier and a maximum of three invocations
- **THEN** its immutable plan reports three and classifier-enabled true without calling the classifier
