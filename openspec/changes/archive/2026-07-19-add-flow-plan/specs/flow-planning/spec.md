## ADDED Requirements

### Requirement: Flow plans are immutable, structured planning data
The `kazeflow.plan` module SHALL expose `PlanConfig`, `TaskPlan`, `FlowPlan`,
and `build_flow_plan()`.  `PlanConfig` SHALL contain `max_concurrency` and the
optional selected `partition_keys`; `TaskPlan` SHALL contain a task name, its
direct dependency names, and its partition selection; and `FlowPlan` SHALL
contain the canonical selected targets, the ordered task plans, and the
normalized plan configuration.  All three model values SHALL be immutable
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

### Requirement: The plan builder resolves selected work without execution
`build_flow_plan(targets, *, config=None, registry=default_registry)` SHALL
accept `targets` only as a `list` or `tuple` of one or more non-empty strings.
`str`, `bytes`, `bytearray`, `None`, sets, iterators, and a list or tuple
containing a non-string value SHALL raise `TypeError`; the builder SHALL NOT
interpret a lone string as a one-element target selection.  An empty list or
tuple, or a list or tuple containing a duplicate name, SHALL raise `ValueError`.
The builder SHALL normalize a valid selection to its lexicographically sorted
target tuple before resolving the transitive dependency closure from the given
asset registry.

The builder SHALL return only metadata required to inspect that work.  It SHALL
not invoke an asset function, create an asyncio task, print output, configure
logging, write files, or retain a task output.  The returned task plans SHALL
include every target and every transitive dependency exactly once.

#### Scenario: Planning a target does not run its asset
- **WHEN** an asset function would produce an observable side effect if called
  and a caller builds a plan targeting that asset
- **THEN** the plan contains the asset and the side effect has not occurred

#### Scenario: A target includes its transitive dependencies
- **WHEN** a selected target depends on an intermediate task which depends on a
  root task
- **THEN** the plan contains the root, intermediate, and target exactly once

#### Scenario: A lone string is not a target sequence
- **WHEN** a caller passes `"target"` instead of a non-text sequence containing
  `"target"`
- **THEN** `build_flow_plan()` raises `TypeError` and invokes no asset function

#### Scenario: A valid target sequence is canonicalized
- **WHEN** a caller passes the sequence `["zeta", "alpha"]`
- **THEN** the returned `FlowPlan.targets` is `("alpha", "zeta")`

### Requirement: Plan ordering is deterministic and dependency-first
The `FlowPlan.tasks` sequence SHALL be a deterministic topological order in
which every direct dependency precedes its dependent task.  When more than one
task is otherwise ready, the builder SHALL order task names lexicographically.
Direct dependency names in each `TaskPlan` SHALL also be lexicographically
ordered.  Equivalent registry metadata and equivalent targets/configuration
SHALL therefore produce equal ordered plan values regardless of the registry's
internal set iteration order.

#### Scenario: Independent ready tasks have a repeatable order
- **WHEN** two dependency-free tasks are both required by one selected target
- **THEN** their positions in the plan are lexicographic by name and both
  precede the selected target

#### Scenario: Dependency metadata is unordered internally
- **WHEN** the registry supplies a task's dependencies through an unordered
  collection
- **THEN** repeated planning produces the same dependency tuple and task order

### Requirement: Invalid definitions and target selections fail during planning
The builder SHALL raise `ValueError` and return no plan when a validated target
name is unknown to the registry, a transitive dependency is missing from the
registry, or the selected dependency closure contains a cycle.  The error
SHALL be raised before any asset function is invoked.  An empty target string
is an invalid target value and SHALL raise `ValueError` before registry lookup.

#### Scenario: A dependency is missing
- **WHEN** a selected target names a dependency that is not registered
- **THEN** `build_flow_plan()` raises `ValueError` and invokes no asset function

#### Scenario: A target name is unknown
- **WHEN** a valid non-empty target name is not registered
- **THEN** `build_flow_plan()` raises `ValueError` and invokes no asset function

#### Scenario: Empty or duplicate target values are supplied
- **WHEN** a caller supplies an empty target sequence, an empty target name, or
  a target name more than once
- **THEN** `build_flow_plan()` raises `ValueError` and invokes no asset function

#### Scenario: The selected closure has a cycle
- **WHEN** selected assets have a direct or indirect dependency cycle
- **THEN** `build_flow_plan()` raises `ValueError` and invokes no asset function

### Requirement: Plan configuration validates concurrency and partition selection
`PlanConfig.max_concurrency` SHALL be either `None` or a positive integer;
booleans and all non-positive or non-integer values SHALL be rejected with
`ValueError` by the builder.  A selected flow containing any partitioned task
SHALL require an explicitly supplied partition-key sequence.  Each supplied
partition key SHALL be hashable and non-`None`; `0`, `False`, and `""` SHALL be
preserved as actual partition keys.  The builder SHALL reject duplicate keys
under Python equality, including `0` and `False`, with `ValueError`.  An empty
explicit sequence SHALL be valid and SHALL be preserved as the selection of
every selected partitioned task.

#### Scenario: Falsey keys remain partition keys
- **WHEN** a partitioned target is planned with a non-duplicate sequence
  containing `0`, `False`, or `""`
- **THEN** each supplied key appears in the task's partition selection and none
  is represented as an unpartitioned attempt

#### Scenario: Partition selection is omitted
- **WHEN** a selected flow contains a partitioned task and `partition_keys` is
  not explicitly supplied
- **THEN** `build_flow_plan()` raises `ValueError`

#### Scenario: Invalid partition keys are rejected
- **WHEN** the supplied sequence contains `None`, an unhashable value, or two
  keys equal under Python equality
- **THEN** `build_flow_plan()` raises `ValueError`
