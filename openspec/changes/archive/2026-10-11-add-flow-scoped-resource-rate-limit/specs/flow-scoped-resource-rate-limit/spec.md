## Purpose

Bound asset body starts over time for a shared resource within one local flow run, while preserving independent work and explicit inspection before execution.

## ADDED Requirements

### Requirement: Resource rate declarations are explicit and validated
A flow-owned resource policy SHALL accept an optional rate limit with a positive non-boolean integer start count and a positive finite non-boolean numeric duration in seconds. Invalid declarations SHALL fail before asset execution or run events. Existing policies without a rate limit SHALL retain their behavior.

#### Scenario: Invalid declaration
- **WHEN** a rate limit has a zero count, boolean count, non-finite duration, or non-positive duration
- **THEN** definition or planning rejects it before any run event or asset call

### Requirement: Rate limits bound starts in every rolling window
For each resource, no more than its configured count of asset body starts SHALL occur within any interval `(t-duration, t]` in one run, across all selected assets and keys. Failed body invocations SHALL count as starts. The limit SHALL count body invocations rather than arbitrary application API calls and SHALL not coordinate separate runs.

#### Scenario: Shared limit across assets and keys
- **WHEN** multiple ready assets and partitions share a rate-limited resource
- **THEN** their combined starts obey the rolling-window bound as well as existing global and resource concurrency bounds

#### Scenario: Failed invocation
- **WHEN** a body fails after starting
- **THEN** its start remains counted until it leaves the time window

#### Scenario: Separate runs
- **WHEN** two runs use the same resource name
- **THEN** each applies independent run-local rate state

### Requirement: Rate waiting preserves progress and cancellation
An attempt waiting only for a rate permit SHALL occupy neither a global invocation slot nor a resource invocation slot. Eligible unrelated work SHALL continue. Normally completed runs SHALL retain one terminal result per selected attempt and plan/selection result order. External cancellation SHALL interrupt permit waiting and propagate without a synthetic completed result.

#### Scenario: No running body remains
- **WHEN** all running work finishes but a pending body awaits the next permitted start
- **THEN** the executor waits until admission is possible and eventually completes the pending attempt once

#### Scenario: Independent work
- **WHEN** a resource is waiting for its next permit and unrelated work is ready
- **THEN** the unrelated work can use available global slots

#### Scenario: Cancel during rate wait
- **WHEN** the caller cancels a run while an attempt waits for a permit
- **THEN** that body does not start and cancellation propagates without a fabricated RunResult

### Requirement: Materialization hits do not consume start permits
Resolution and loading of a compatible committed materialization SHALL consume no body-start permit. A materialization miss SHALL receive admission before its body starts. Resolve/load failure SHALL retain the existing failure behavior without an implicit body invocation.

#### Scenario: Hit and miss
- **WHEN** one selected key is reusable and another requires execution
- **THEN** only the executed body's start is counted against the resource rate limit
