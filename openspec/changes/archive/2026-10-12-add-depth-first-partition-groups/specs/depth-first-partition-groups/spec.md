## Purpose

Let applications explicitly bound the number of selected partition keys progressing through an asset chain while preserving inspectable dependency plans and exactly-once logical terminal outcomes.

## ADDED Requirements

### Requirement: Groups are explicit immutable validated metadata
PartitionGroup SHALL contain an ordered tuple of at least two distinct non-empty asset names and a positive non-boolean max_active_keys integer (default1). Flow and build_flow_plan SHALL accept an optional sequence of groups, freeze that sequence and expose selected groups in FlowPlan.partition_groups. Groups SHALL be disjoint; their assets SHALL exist and be partitioned in the same selected domain, and each member after the first SHALL directly depend on its predecessor. A dependency path leaving a group and returning into it SHALL be rejected to avoid barriers that depend on unreleased keys. A wholly unselected group SHALL be omitted; a partially selected group SHALL fail preflight. Invalid groups SHALL fail before events, bodies or adapter operations. Selected groups SHALL appear deterministically by their first member's plan position.

#### Scenario: Inspect without execution
- **WHEN** a valid grouped flow is planned
- **THEN** its immutable selected groups are inspectable with no execution or adapter I/O

#### Scenario: Reject an unsafe or partial declaration
- **WHEN** groups overlap, are not partitioned direct chains, are partially selected or leave and re-enter through a dependency
- **THEN** planning rejects the declaration before any event or work

### Requirement: Active keys progress depth first with separate capacity
For each selected group, at most max_active_keys selected key positions SHALL be admitted concurrently. Admission SHALL follow selected key order. A key SHALL retain its group slot through preparation, body, commit, retry/cooldown and waiting, and release it only after every group member for that key reaches a terminal result, including dependency or circuit skips. Within admitted keys, earlier keys and their next unfinished member SHALL be preferred over starting later keys when eligible. Blocked or resource-waiting work SHALL not prevent eligible work on another admitted key or outside the group. Global/resource invocation limits and rate/cooldown policies SHALL still apply independently. Materialization hits SHALL count toward group progression without consuming body resource permits. Existing flows without groups SHALL retain their admission behavior.

#### Scenario: One active key traverses a chain
- **WHEN** a group has one active key slot and two keys selected
- **THEN** the first key reaches a terminal result for every member before the second key is admitted

#### Scenario: Retry retains key capacity and releases invocation capacity
- **WHEN** an admitted key waits to retry under a group cap of one
- **THEN** a later group key is not admitted while unrelated eligible work can use released invocation capacity

#### Scenario: Failure completes the key before replacement
- **WHEN** a grouped upstream partition fails
- **THEN** its matching grouped downstream attempts terminate as blocked, the slot is released and another selected key can progress

### Requirement: Grouping preserves result and observation contracts
Groups SHALL preserve per-key dependency matching, unpartitioned prerequisite/task-aggregation barriers, task ordering in FlowPlan/RunResult, attempt ordering by selected keys and exactly one logical terminal outcome per selected task/key. External cancellation and observer failures SHALL propagate without synthetic completion. Group state SHALL reset for every run. Empty selection SHALL yield existing no-work outcomes and no deadlock. Group limits SHALL constrain admission rather than promise memory eviction or cross-run exactly-once behavior.

#### Scenario: Empty grouped work
- **WHEN** a group receives an explicit empty selection
- **THEN** member tasks have no_partition_keys outcomes and the run completes without bodies

#### Scenario: Result order despite completion order
- **WHEN** concurrently admitted keys finish in a different chronology
- **THEN** results retain plan task order and selection attempt order and events retain valid causal sequence
