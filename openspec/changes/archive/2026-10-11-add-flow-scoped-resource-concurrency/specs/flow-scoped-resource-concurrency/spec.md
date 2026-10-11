## Purpose

Allow a local flow to share an inspectable concurrency bound across asset invocations that use the same named resource, while independent work continues normally.

## ADDED Requirements

### Requirement: A flow can declare named concurrency resources
A `Flow` SHALL accept an optional mapping from non-empty resource names to policies with a positive integer `max_concurrency`. An asset SHALL be able to declare at most one resource name. Planning SHALL reject invalid policy values or an asset reference to a resource not declared by that flow before invoking any asset or emitting a run event. A flow with no resource declarations SHALL retain its existing behavior.

#### Scenario: Shared declaration
- **WHEN** two selected assets declare the same named resource in one flow
- **THEN** planning succeeds and both are associated with the same flow-scoped concurrency policy

#### Scenario: Missing resource
- **WHEN** a selected asset names a resource absent from its flow
- **THEN** planning fails before any asset invocation or run event

#### Scenario: Invalid limit
- **WHEN** a policy uses zero, a negative number, a boolean, or a non-integer limit
- **THEN** planning fails before any asset invocation or run event

### Requirement: Resource limits apply across all selected attempts in one run
The executor SHALL run no more than a resource's configured number of asset invocations at a time across all assets and partition keys using that resource. It SHALL also obey the existing flow-wide `max_concurrency` bound. Waiting for one resource SHALL NOT consume a global execution slot or prevent otherwise eligible work using a different resource or no resource from starting. Each selected attempt SHALL still reach one terminal outcome on normal completion and retain plan order in the result.

#### Scenario: Shared resource across assets and partitions
- **WHEN** two assets using the same resource have multiple ready partition attempts and that resource has limit one
- **THEN** no more than one of their invocations runs at a time, even if the global limit is higher

#### Scenario: Unrelated work remains concurrent
- **WHEN** one resource is saturated and an independent attempt does not use it
- **THEN** the independent attempt can start if a global slot is available

#### Scenario: Failure releases capacity
- **WHEN** an invocation using a resource fails and other attempts remain eligible
- **THEN** its capacity is released, the failed attempt is recorded once, and eligible attempts continue

### Requirement: Resource policy scope is explicit
Resource capacity SHALL be scoped to one `Flow.run_async()` execution, including all assets and partitions in that execution. The policy SHALL count asset invocations rather than arbitrary calls made inside an asset. It SHALL NOT promise coordination with another run, process, machine, or external API quota.

#### Scenario: Separate runs do not coordinate
- **WHEN** two flows run concurrently in different executions using the same resource name
- **THEN** each execution applies its own limit without claiming a shared cross-run limit
