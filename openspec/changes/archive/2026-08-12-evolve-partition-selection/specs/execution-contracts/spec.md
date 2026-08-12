## MODIFIED Requirements

### Requirement: Partition selection preserves falsey keys and handles no-work explicitly
For a selected partitioned task, omitted `partition_keys` and `partition_range` SHALL raise `ValueError` before execution. An explicitly supplied empty key sequence SHALL produce zero partition attempts for that task, with the task aggregate terminally `skipped` and reason `no_partition_keys`; that no-work skip SHALL be distinct from a dependency-blocking skip. A non-partitioned downstream dependency of a `no_partition_keys` task SHALL receive an empty mapping and remain runnable. Each supplied key SHALL be normalized by the selected partition definition before generic validation. Each normalized non-`None` key, including `0`, `""`, and `False` when accepted by a custom definition, SHALL be treated as an actual partition key and SHALL NOT be truth-tested as an unpartitioned attempt. `None`, unhashable normalized values, and keys duplicated after normalization, including duplicates under Python equality such as `0` and `False`, SHALL be rejected before execution. Unpartitioned tasks SHALL use absence of a partition key rather than a falsey value to identify their single attempt. Explicit keys, an explicit bounded range, and explicit empty selection SHALL be distinct selection forms and SHALL NOT be combined in one run configuration.

#### Scenario: Falsey keys are executed as partitions
- **WHEN** a custom partition definition accepts a non-duplicate sequence containing `0` or `""`
- **THEN** the result contains a partition attempt for each normalized key and none is recorded as unpartitioned

#### Scenario: Partition keys are omitted for a selected partitioned task
- **WHEN** a selected flow includes a partitioned task and both explicit keys and range are omitted
- **THEN** the call raises `ValueError` before any asset function runs

#### Scenario: An explicitly empty partition selection produces no work
- **WHEN** a selected flow includes a partitioned task and `partition_keys` is an empty sequence
- **THEN** the partitioned task is aggregate `skipped` with reason `no_partition_keys`, distinct from dependency blocking, and its non-partitioned downstream receives an empty mapping and executes

#### Scenario: Invalid partition keys are supplied
- **WHEN** normalization rejects a key, returns `None` or an unhashable value, or produces two keys equal under Python equality
- **THEN** the call raises `ValueError` before any asset function runs

#### Scenario: Selection forms conflict
- **WHEN** a caller combines explicit partition keys with a partition range
- **THEN** the call raises `ValueError` before any asset function runs

### Requirement: Partition-aware dependencies propagate outcomes at the matching granularity
When both an upstream task and a downstream task are partitioned, their definitions SHALL belong to the same partition domain and a downstream partition attempt SHALL depend only on the upstream attempt with the same normalized partition key. A failed, cancelled, or dependency-blocked skipped upstream partition SHALL make only the matching downstream partition dependency-blocked `skipped`, with `blocked_by`; unaffected keys SHALL remain independently runnable. A runnable downstream partition SHALL receive a mapping containing only successfully completed values from each partitioned upstream dependency; it SHALL never receive failed, cancelled, or skipped values.

For a non-partitioned downstream task that depends on a partitioned upstream task, the downstream task SHALL execute only when the upstream aggregate is `success`, or when it is `skipped` solely with reason `no_partition_keys`. In the latter case it SHALL receive `{}`. A non-partitioned downstream SHALL be dependency-blocked `skipped` when the upstream aggregate is `failed`, `cancelled`, or `skipped` for dependency blocking; it SHALL not execute with a partial mapping of successful upstream partitions. A partitioned downstream with an explicitly empty shared selection has no attempts and independently aggregates to `skipped(no_partition_keys)`.

#### Scenario: A failed upstream key blocks only its matching downstream key
- **WHEN** normalized upstream key `a` fails, normalized upstream key `b` succeeds, and a compatible partitioned downstream has keys `a` and `b`
- **THEN** downstream key `a` is dependency-blocked `skipped` with `blocked_by`, downstream key `b` can execute, and downstream key `b` receives only successful upstream values

#### Scenario: A non-partitioned downstream does not use partial partition output
- **WHEN** a partitioned upstream has mixed successful and failed or dependency-blocked partition outcomes
- **THEN** its non-partitioned downstream is dependency-blocked `skipped` and is not invoked with a partial output mapping

#### Scenario: An empty partition selection permits a non-partitioned reducer
- **WHEN** a partitioned upstream has aggregate `skipped(no_partition_keys)` and its non-partitioned downstream is otherwise runnable
- **THEN** the downstream executes with `{}` as that upstream argument
