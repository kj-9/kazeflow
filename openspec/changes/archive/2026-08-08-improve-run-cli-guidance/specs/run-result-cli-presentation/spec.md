## ADDED Requirements

### Requirement: Text runs present an ordered terminal task summary
For `kazeflow run --format text`, a completed result SHALL include the run ID,
overall status, total duration, and one deterministic line per task in result
order.  Each task line SHALL identify its terminal status and duration.  It SHALL
identify a partitioned task and its attempt count without exposing a raw partition
key, raw asset output, raw exception object, or traceback.

#### Scenario: Summarize a successful partitioned run
- **WHEN** an approved text-mode run completes with an unpartitioned task and a
  partitioned task
- **THEN** the output lists both tasks in result order with their statuses,
  durations, and only safe partition presence/count information

### Requirement: Text runs explain non-success task outcomes safely
Text-mode task summaries SHALL include a skip reason for skipped tasks and a
portable exception type and message for failed tasks.  A failed task summary SHALL
include a documented next command that can inspect the saved run when `--store` was
selected, without assuming that persistence was selected.

#### Scenario: Explain a failed task
- **WHEN** an approved text-mode run reaches a failed asset result
- **THEN** the failed task line contains its portable failure type and message and
  the output directs a stored run caller to `kazeflow runs show RUN_ID`

### Requirement: Verbose text enables safe attempt detail
`kazeflow run` SHALL accept `--verbose` only with `--format text`.  Verbose text
SHALL append deterministic attempt-level outcomes in task and attempt order,
including attempt status, duration, safe partition presence, skip reason, and
portable failure metadata when applicable.  Combining `--verbose` with JSON SHALL
be a usage error before entry loading.

#### Scenario: Request verbose attempt detail
- **WHEN** a caller invokes an approved text-mode run with `--verbose`
- **THEN** the final output includes ordered safe attempt detail and does not
  expose raw partition keys or outputs

#### Scenario: Reject verbose JSON
- **WHEN** a caller combines `run --verbose --format json`
- **THEN** the CLI exits `2` without loading the entry or writing stdout
