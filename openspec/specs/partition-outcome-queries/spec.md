# partition-outcome-queries Specification

## Purpose
Expose ordered terminal partition outcomes from live run results so users can inspect failed or circuit-skipped keys and explicitly choose a subsequent run selection.

## Requirements

### Requirement: Failed partition queries select terminal logical failures
`RunResult.failed_partition_keys(task_name)` SHALL return a tuple containing the original partition keys of exactly the named task's terminal failed attempts. It SHALL exclude successful, skipped and cancelled attempts, including recovered retry successes and circuit-skipped attempts with earlier failed invocations. Keys SHALL retain selected attempt order and object identity, without sorting, hashing, normalization, deduplication or truthiness filtering. No matching outcomes and explicit empty partition selections SHALL return an empty tuple.

#### Scenario: Falsey and opaque keys remain intact
- **WHEN** failed logical attempts contain falsey or non-hashable partition keys
- **THEN** their original key objects appear in attempt order in the returned tuple

#### Scenario: A recovered invocation is not a failed logical outcome
- **WHEN** an invocation failed but its logical attempt ultimately succeeded or was circuit-skipped
- **THEN** the failed-key query excludes that attempt

### Requirement: Circuit skipped queries preserve the specific stop cause
`RunResult.circuit_skipped_partition_keys(task_name)` SHALL return a tuple of the original keys for exactly terminal skipped attempts whose reason is `resource_circuit_open`. It SHALL preserve the same order and identity rules as the failed-key query. Dependency-blocked skips, cancellations and ordinary failures SHALL be excluded even when a circuit is open elsewhere in the run. Filtering SHALL use individual attempt outcomes rather than aggregate task status/reason.

#### Scenario: Mixed partition task is queried by individual outcomes
- **WHEN** a task contains failed, successful, dependency-blocked and circuit-skipped attempts
- **THEN** the circuit-key query returns only circuit-skipped keys, regardless of its failed aggregate status

### Requirement: Task lookup failures are diagnosed consistently
Both query methods SHALL accept a string task name and reject other input types with `TypeError`. Missing task names, including an empty string, SHALL raise `KeyError` carrying the supplied name. A present unpartitioned task SHALL raise `ValueError` even when it has no failed or circuit-skipped outcome. Lookup SHALL not silently select another task, fall back to a target or conflate empty work with an invalid query.

#### Scenario: Missing task is identified
- **WHEN** either query names a task absent from the result
- **THEN** it raises KeyError with that exact name

#### Scenario: Unpartitioned work is rejected
- **WHEN** either query names an unpartitioned task
- **THEN** it raises ValueError explaining that the task is not partitioned

### Requirement: Queries are side-effect free and retain the portable privacy boundary
Queries SHALL inspect only the live in-memory result and SHALL NOT invoke assets, classifiers, adapters or new execution. They SHALL NOT mutate snapshots or add raw keys to any existing record/event/CLI/store projection. Stored portable records SHALL NOT fabricate raw keys or expose these methods as a reconstructed RunResult. Passing returned keys to another flow run SHALL remain an explicit application choice; no automatic resume is introduced.

#### Scenario: A query leaves projections unchanged
- **WHEN** a live result is queried
- **THEN** its existing portable record is unchanged and contains no new raw-key fields
