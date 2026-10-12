# resource-execution-statistics Specification

## Purpose
Expose precise run-local resource invocation, retry and observed admission-wait summaries from live results so callers can inspect resource use without adding telemetry infrastructure or changing portable records.

## Requirements

### Requirement: Statistics are immutable validated live result values
ResourceStatistics SHALL contain name, invocations, succeeded, failed, retries, wait_count, wait_duration and invocation_duration. Counts SHALL default to zero, be non-negative non-boolean integers, and obey succeeded+failed=invocations and retries<=invocations. Durations SHALL default to zero and be non-negative timedeltas; zero invocation/wait counts SHALL require the corresponding duration to be zero. Name SHALL be a non-empty string. RunResult SHALL append an optional resources tuple (default empty), reject duplicate names or non-statistics entries, and expose resource_statistics(name) returning the exact matching snapshot. Non-string names SHALL raise TypeError and missing/empty names SHALL raise KeyError carrying the supplied name. Existing positional construction and all portable result versions1–4 SHALL preserve their previous shapes and omit these statistics.

#### Scenario: Lookup a selected resource
- **WHEN** a live result includes a selected resource summary
- **THEN** exact lookup returns its immutable snapshot without mutation

#### Scenario: Historical result has no statistics
- **WHEN** a manually constructed or historical portable result has no live summary
- **THEN** lookup does not invent counters and existing portable shapes remain unchanged

### Requirement: Invocation and retry statistics reflect actual operations
Normally completed runs SHALL include summaries for exactly selected named resources in plan resource order. Actual starts of body-plus-commit operations SHALL increment invocations; completed operation success/failure SHALL increment succeeded/failed and add monotonic elapsed duration. Retry count SHALL increment only when a subsequent body invocation for the same logical attempt actually starts. Body success followed by commit failure SHALL count as a failed operation. Resolve/load preparation, materialization hits, dependency/circuit skips and retry decisions whose retry never starts SHALL NOT count as body invocations or retries. Selected resources with no body starts SHALL retain zero invocation statistics. Each run SHALL have fresh counters and no process-global totals.

#### Scenario: Retry recovers
- **WHEN** a body fails and its second invocation succeeds
- **THEN** the resource reports two invocations, one failure, one success and one retry while the logical result is success

#### Scenario: Commit fails or materialized work is reused
- **WHEN** a body returns but commit fails, or a compatible output is reused without a body
- **THEN** commit failure counts one failed invocation and reuse counts zero invocations

### Requirement: Admission waits use disclosed observed intervals
When the executor examines an otherwise dependency-ready body operation and denies resource concurrency, rolling rate, retry-delay or shared-cooldown admission, it SHALL start one observed wait episode if none is open for that operation. wait_count SHALL count these episodes once despite repeated checks. Wait duration SHALL accumulate monotonic elapsed time from first observed denial to actual body start or terminal skip. Preparation and dependency waits SHALL NOT start episodes. A global-only queue delay before resource admission is examined SHALL NOT be measured, but elapsed time within an open observed episode MAY include subsequent global/scheduler delay. Durations SHALL be sums across operations, may overlap and SHALL NOT claim exclusive per-cause or flow wall-clock time. Waiting/skipped work SHALL release invocation capacity according to existing policies, and statistics SHALL not alter scheduling, statuses, event counts, cancellation or observer failure behavior.

#### Scenario: Shared resource wait
- **WHEN** resource concurrency blocks a ready operation until another operation finishes
- **THEN** its wait is counted once and timed until actual start

#### Scenario: Waiting work is circuit skipped
- **WHEN** a ready operation has an open wait episode and a resource circuit stops it before body start
- **THEN** the episode duration closes at terminal skip without adding invocation or retry counts
