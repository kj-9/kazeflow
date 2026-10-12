## ADDED Requirements

### Requirement: Plans expose materialization participation without storage lookup
Each selected task in `FlowPlan` SHALL expose whether its asset has an explicit materialization declaration. Planning SHALL validate materialization configuration and dependency eligibility without calling resolve, load, commit, or an asset body. The plan SHALL not claim a cache hit or predict a reusable record.

#### Scenario: Side-effect-free participation inspection
- **WHEN** a caller plans a flow containing a materialized partitioned asset
- **THEN** the task plan identifies its participation and no adapter storage operation occurs
