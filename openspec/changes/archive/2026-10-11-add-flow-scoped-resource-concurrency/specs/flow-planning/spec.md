## ADDED Requirements

### Requirement: Plans expose resolved resource constraints
An immutable `FlowPlan` SHALL expose the names and positive concurrency limits of resources used by its selected dependency closure, and each `TaskPlan` SHALL expose its selected asset's resource name or absence. Equivalent flow definitions SHALL produce equivalent, deterministically ordered resource planning data. Planning SHALL not acquire capacity, invoke asset bodies, or create execution state.

#### Scenario: Inspect a shared resource before execution
- **WHEN** two selected tasks declare the same resource and a caller requests a plan
- **THEN** the caller can inspect one resource limit and both task associations without either task running

#### Scenario: Unused resource
- **WHEN** a flow declares a resource not used by its selected closure
- **THEN** the plan omits it from the selected-work resource summary

#### Scenario: No resources
- **WHEN** a flow and its selected assets declare no resources
- **THEN** the plan retains the existing target, task, partition, and global-concurrency meaning with an empty resource summary
