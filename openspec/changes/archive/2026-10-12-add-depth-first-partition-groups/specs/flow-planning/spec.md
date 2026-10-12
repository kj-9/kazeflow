## MODIFIED Requirements

### Requirement: Flow plans are immutable, structured planning data
The `kazeflow.plan` module SHALL expose `PlanConfig`, `TaskPlan`, `FlowPlan`,
and `build_flow_plan()`.  `PlanConfig` SHALL contain `max_concurrency` and the
optional selected `partition_keys`; `TaskPlan` SHALL contain a task name, its
direct dependency names, and its partition selection; and `FlowPlan` SHALL
contain the canonical selected targets, the ordered task plans, and the
normalized plan configuration, optional resource summaries and a tuple of selected PartitionGroup values as partition_groups (default empty).  All three model values SHALL be immutable
after construction, and every sequence exposed by them SHALL be a tuple.
`FlowPlan.targets` SHALL be the lexicographically sorted tuple of directly
selected target names, not the transitive closure and not the caller's input
order.  `None` as a task's partition selection SHALL identify its one
unpartitioned attempt; an empty tuple SHALL identify a partitioned task with
explicitly no selected keys.

#### Scenario: A plan has no mutable execution state
- **WHEN** a caller builds a plan for an unpartitioned target
- **THEN** the returned `FlowPlan`, its `PlanConfig`, and every `TaskPlan` are
  immutable structured values and the task's partition selection is `None`

#### Scenario: An explicitly empty partition selection is retained
- **WHEN** a caller builds a plan containing a partitioned task with an empty
  partition-key sequence
- **THEN** that task has an empty tuple selection rather than being represented
  as an unpartitioned task

#### Scenario: Equivalent target selections produce equal plans
- **WHEN** callers select the same two targets in different input orders with
  equivalent registry metadata and configuration
- **THEN** both `FlowPlan.targets` values are the same lexicographically sorted
  tuple and the two returned plans compare equal
