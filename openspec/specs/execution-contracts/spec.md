## Purpose

Define stable, externally observable execution semantics for kazeflow flows, tasks,
partition attempts, results, presentation boundaries, and sync/async entry points.

## Requirements

### Requirement: Execution lifecycle has explicit terminal statuses
The core SHALL represent every flow, selected task, and selected partition attempt with an explicit lifecycle status. Task and partition attempts SHALL use `pending`, `running`, `success`, `failed`, `skipped`, or `cancelled`; the terminal statuses are `success`, `failed`, `skipped`, and `cancelled`. Flows SHALL use `pending`, `running`, `success`, `failed`, or `cancelled`. Every selected attempt SHALL reach exactly one terminal status before the returned result is finalized. A flow SHALL be `success` only when every selected attempt is `success` or `skipped`; it SHALL be `failed` when any attempt failed; and it SHALL be `cancelled` only when explicit cancellation prevents completion and no attempt failed.

#### Scenario: A successful dependency chain completes
- **WHEN** every selected task and partition attempt completes without an error
- **THEN** each attempt and the flow have status `success`

#### Scenario: A dependency is not runnable after an upstream failure
- **WHEN** a task depends directly or transitively on a failed task
- **THEN** the dependent task is terminally `skipped`, records `blocked_by` identifying the failed dependency chain, and its asset function is not invoked

#### Scenario: Explicit cancellation prevents scheduled work
- **WHEN** explicit cancellation is requested before all selected attempts complete and no attempt fails
- **THEN** uncompleted attempts are terminally `cancelled` or dependency-blocked `skipped` and the flow is `cancelled`

### Requirement: Partitioned task status aggregates partition outcomes
A partitioned task SHALL expose both each selected partition attempt and one aggregate task status. The aggregate SHALL be `success` only when every selected partition attempt is `success`; it SHALL be `failed` when any partition attempt is `failed`; it SHALL be `cancelled` when no partition failed and any partition is `cancelled`; and it SHALL be `skipped` when no partition failed or was cancelled and one or more partitions are `skipped`. An explicitly empty selection SHALL produce an aggregate `skipped` task with reason `no_partition_keys`. Aggregate results SHALL preserve the individual partition outcomes and reasons, including mixed success/skipped or success/failed outcomes, rather than collapsing them into a synthetic output.

#### Scenario: Partition outcomes are mixed by a failure
- **WHEN** one selected partition succeeds and another selected partition fails
- **THEN** both partition outcomes are retained and the aggregate task status is `failed`

#### Scenario: Partition outcomes are mixed by dependency blocking
- **WHEN** one selected partition succeeds and another is dependency-blocked and skipped
- **THEN** both partition outcomes are retained and the aggregate task status is `skipped`

#### Scenario: No partitions are selected explicitly
- **WHEN** a partitioned task receives an explicitly empty partition sequence
- **THEN** it has no partition attempts and has aggregate status `skipped` with reason `no_partition_keys`

### Requirement: Failures are isolated and reported as results
The executor SHALL allow ready independent branches and attempts already running to continue after an attempt fails. It SHALL not schedule a task whose required dependency has failed, been cancelled, or been dependency-blocked `skipped`; `skipped(no_partition_keys)` is an explicit exception governed by the partition-selection contract. `run()` and `run_async()` SHALL return a `RunResult` for a completed failed flow rather than raise a task failure. Invalid definitions and invalid run configuration SHALL raise before any asset function is scheduled. This change SHALL NOT introduce a `raise_on_failure` or equivalent task-failure raising option.

#### Scenario: An independent branch outlives a failure
- **WHEN** one ready branch fails while another ready branch is independent
- **THEN** the independent branch is allowed to finish and the returned result records both terminal outcomes

#### Scenario: A task failure returns a result
- **WHEN** an asset function raises
- **THEN** the call returns a failed `RunResult` containing serializable failure metadata

#### Scenario: Invalid configuration is rejected before execution
- **WHEN** a caller supplies invalid concurrency or partition configuration
- **THEN** the call raises a validation error and invokes no asset function

### Requirement: Result timestamps and durations have defined clocks
Each flow and attempt result SHALL expose timezone-aware UTC wall-clock start and end timestamps when the corresponding lifecycle boundaries have occurred. Each completed flow and attempt SHALL expose a non-negative duration measured as a difference of monotonic clock readings. Implementations SHALL NOT derive duration by subtracting wall-clock timestamps.

#### Scenario: A completed attempt records time consistently
- **WHEN** an attempt starts and later reaches a terminal status
- **THEN** its result contains aware UTC start and end timestamps and a non-negative monotonic duration

#### Scenario: The system clock changes during a run
- **WHEN** wall-clock time changes while an attempt is running
- **THEN** the recorded duration remains based on the monotonic elapsed interval

### Requirement: Partition selection preserves falsey keys and handles no-work explicitly
For a selected partitioned task, omitted `partition_keys` SHALL raise `ValueError` before execution. An explicitly supplied empty sequence SHALL produce zero partition attempts for that task, with the task aggregate terminally `skipped` and reason `no_partition_keys`; that no-work skip SHALL be distinct from a dependency-blocking skip. A non-partitioned downstream dependency of a `no_partition_keys` task SHALL receive an empty mapping and remain runnable. Each supplied non-`None` key, including `0`, `""`, and `False`, SHALL be treated as an actual partition key and SHALL NOT be truth-tested as an unpartitioned attempt. `None` and duplicate keys, including duplicates under Python equality such as `0` and `False`, SHALL be rejected before execution. Unpartitioned tasks SHALL use absence of a partition key rather than a falsey value to identify their single attempt.

#### Scenario: Falsey keys are executed as partitions
- **WHEN** a partitioned task is selected with a non-duplicate sequence containing `0` or `""`
- **THEN** the result contains a partition attempt for each supplied key and none is recorded as unpartitioned

#### Scenario: Partition keys are omitted for a selected partitioned task
- **WHEN** a selected flow includes a partitioned task and `partition_keys` is omitted
- **THEN** the call raises `ValueError` before any asset function runs

#### Scenario: An explicitly empty partition selection produces no work
- **WHEN** a selected flow includes a partitioned task and `partition_keys` is an empty sequence
- **THEN** the partitioned task is aggregate `skipped` with reason `no_partition_keys`, distinct from dependency blocking, and its non-partitioned downstream receives an empty mapping and executes

#### Scenario: Invalid partition keys are supplied
- **WHEN** `partition_keys` includes `None` or two keys equal under Python equality
- **THEN** the call raises `ValueError` before any asset function runs

### Requirement: Partition-aware dependencies propagate outcomes at the matching granularity
When both an upstream task and a downstream task are partitioned, a downstream partition attempt SHALL depend only on the upstream attempt with the same partition key. A failed, cancelled, or dependency-blocked skipped upstream partition SHALL make only the matching downstream partition dependency-blocked `skipped`, with `blocked_by`; unaffected keys SHALL remain independently runnable. A runnable downstream partition SHALL receive a mapping containing only successfully completed values from each partitioned upstream dependency; it SHALL never receive failed, cancelled, or skipped values.

For a non-partitioned downstream task that depends on a partitioned upstream task, the downstream task SHALL execute only when the upstream aggregate is `success`, or when it is `skipped` solely with reason `no_partition_keys`. In the latter case it SHALL receive `{}`. A non-partitioned downstream SHALL be dependency-blocked `skipped` when the upstream aggregate is `failed`, `cancelled`, or `skipped` for dependency blocking; it SHALL not execute with a partial mapping of successful upstream partitions. A partitioned downstream with an explicitly empty shared selection has no attempts and independently aggregates to `skipped(no_partition_keys)`.

#### Scenario: A failed upstream key blocks only its matching downstream key
- **WHEN** partitioned upstream key `a` fails, upstream key `b` succeeds, and a partitioned downstream has keys `a` and `b`
- **THEN** downstream key `a` is dependency-blocked `skipped` with `blocked_by`, downstream key `b` can execute, and downstream key `b` receives only successful upstream values

#### Scenario: A non-partitioned downstream does not use partial partition output
- **WHEN** a partitioned upstream has mixed successful and failed or dependency-blocked partition outcomes
- **THEN** its non-partitioned downstream is dependency-blocked `skipped` and is not invoked with a partial output mapping

#### Scenario: An empty partition selection permits a non-partitioned reducer
- **WHEN** a partitioned upstream has aggregate `skipped(no_partition_keys)` and its non-partitioned downstream is otherwise runnable
- **THEN** the downstream executes with `{}` as that upstream argument

### Requirement: Runtime values are separate from serializable run records
Each successful task or partition result SHALL make its in-memory Python output available through the returned `RunResult` for the duration of the caller's process. Raw exception objects, if retained for in-process inspection, SHALL likewise remain in-memory-only. The core SHALL not require arbitrary output objects, partition keys, or exception objects to be JSON-serializable. A serializable run-record projection SHALL exclude raw outputs and exception objects and include stable run, lifecycle, timestamp, duration, identity, partition-presence, and failure metadata. Failure metadata SHALL include an exception type name, message, and formatted traceback text when available.

#### Scenario: A task returns a non-serializable object
- **WHEN** a successful asset returns an arbitrary Python object that cannot be JSON encoded
- **THEN** the `RunResult` exposes that object in memory and its serializable record remains producible without the object

#### Scenario: A task fails with an exception object
- **WHEN** an asset function raises an exception
- **THEN** the task result can retain the exception in memory and exposes serializable failure metadata for portable records

### Requirement: Core execution has no implicit presentation or persistence
Core planning and execution SHALL use a no-op observer by default and SHALL not print output, configure global logging, create files, or create a database unless a caller explicitly selects an observer, renderer, logger, or persistence adapter. Core execution data SHALL be available independently of optional presentation and persistence consumers. The core runtime SHALL require only the Python standard library.

#### Scenario: Core execution runs without a renderer
- **WHEN** a caller executes a flow without selecting a presentation consumer
- **THEN** the flow returns its result without terminal UI output or a third-party presentation import

#### Scenario: A persistence adapter is absent
- **WHEN** a caller does not select a persistence adapter
- **THEN** execution creates no run-history database or record file

### Requirement: The synchronous entry point is not callable inside a running event loop
`run()` SHALL detect a running event loop in its calling thread before it schedules work. In that context it SHALL raise a clear `RuntimeError` directing the caller to await `run_async()`; it SHALL not create a nested event loop or invoke any asset function. Outside a running event loop, `run()` SHALL run the same execution semantics as `run_async()` and return the same kind of `RunResult`.

#### Scenario: A caller invokes run from async code
- **WHEN** `run()` is called from a thread with an active event loop
- **THEN** it raises `RuntimeError` before asset execution and identifies `run_async()` as the supported entry point

#### Scenario: A synchronous caller executes a flow
- **WHEN** `run()` is called from ordinary synchronous Python code
- **THEN** it returns a `RunResult` governed by the same status and failure semantics as `run_async()`

### Requirement: Compatibility changes are explicit and narrowly scoped
The migration from the current entry points SHALL preserve existing asset decorator, dependency-inference, and direct-function-call use cases. Callers that ignore the current `None` return value SHALL remain source-compatible when entry points begin returning `RunResult`. The automatic Rich terminal display SHALL be removed from core execution; migration guidance SHALL direct callers to an explicit optional renderer. `Flow.asset_outputs` SHALL not remain the authoritative execution result, and repeat runs SHALL not read outputs retained from a previous run.

#### Scenario: Existing code ignores a run return value
- **WHEN** existing synchronous or asynchronous caller code invokes an entry point and ignores its return value
- **THEN** the caller remains source-compatible after the entry point returns `RunResult`

#### Scenario: Existing code relies on automatic terminal UI
- **WHEN** a caller requires Rich terminal rendering that was previously automatic
- **THEN** migration guidance directs it to select the optional renderer explicitly

#### Scenario: A flow is run more than once
- **WHEN** the same flow definition is executed for a second time
- **THEN** the second run does not use task output retained from the first run
