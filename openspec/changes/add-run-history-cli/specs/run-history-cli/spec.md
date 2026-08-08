## ADDED Requirements

### Requirement: History commands read an existing project-local SQLite store
`kazeflow runs list`, `kazeflow runs show RUN_ID`, and `kazeflow runs compare
LEFT_RUN_ID RIGHT_RUN_ID` SHALL resolve their store path to `--store PATH` when
provided, otherwise to `./.kazeflow/runs.sqlite3` relative to the current
working directory. Before opening the SQLite adapter, each command SHALL verify
that the resolved path names an existing regular file. The commands SHALL be
read-only: they SHALL NOT create, initialize, migrate, or otherwise modify a
store. They SHALL remain independent of core flow execution and SHALL NOT load a
flow entry, invoke an asset body, or initialize an optional TUI. This default
SHALL NOT make ordinary `kazeflow run` commands persist without their existing
explicit persistence option.

#### Scenario: Read the project-local default store
- **WHEN** a caller invokes a history command without `--store` from a directory
  containing an existing `.kazeflow/runs.sqlite3` regular file
- **THEN** the command reads that file as its store

#### Scenario: Refuse a missing resolved store without creating it
- **WHEN** a caller invokes any history command whose explicit or default store
  path does not exist, or is not a regular file
- **THEN** the command emits a diagnostic to stderr, does not create or modify a
  database at that path, emits no successful document to stdout, and exits with
  status `4`

#### Scenario: Read an existing store without execution side effects
- **WHEN** a caller invokes a history command with an existing valid SQLite store
- **THEN** the command reads only the stored portable records and does not load a
  Python flow, execute assets, initialize a TUI, or modify the store

### Requirement: List stored run summaries deterministically
`kazeflow runs list` SHALL list stored run summaries in ascending
saved-time order, breaking equal saved times by ascending run ID.  `--limit N`,
when supplied, SHALL require a non-negative integer and SHALL return the first
`N` summaries in that deterministic order.  Each list summary SHALL expose only
the stored run ID, terminal status, saved time, and schema version.

#### Scenario: List ordered summaries with a limit
- **WHEN** a store contains records with different or equal saved times and a
  caller invokes `runs list --limit 2`
- **THEN** the command returns exactly the first two summaries ordered by saved
  time ascending and then run ID ascending

#### Scenario: Emit a single JSON list document
- **WHEN** a caller invokes `runs list` with `--format json`
- **THEN** stdout contains exactly one JSON document representing the ordered
  summaries and all diagnostics remain on stderr

### Requirement: Show the stored portable record envelope
`kazeflow runs show RUN_ID --store PATH` SHALL return the stored record for the
selected run without rebuilding a `RunResult`.  Its JSON representation SHALL be
one document containing the record's stored envelope, including its run ID,
schema version, terminal status, saved time, and canonical portable record.  Text
output SHALL identify the same envelope and its portable record.  Neither output
format SHALL disclose raw task outputs, exception objects, or raw partition-key
values.

#### Scenario: Show a portable stored record
- **WHEN** a caller selects an existing stored run that contains partitioned
  attempts and failure metadata
- **THEN** the command shows its stored envelope and portable record, preserving
  task and attempt order and partition presence while omitting raw output,
  exception-object, and partition-key values

#### Scenario: Emit a single JSON show document
- **WHEN** a caller invokes `runs show RUN_ID` with `--format json`
- **THEN** stdout contains exactly one JSON document for that stored envelope and
  stderr contains no successful record content

### Requirement: Compare portable aggregate run data in caller order
`kazeflow runs compare LEFT_RUN_ID RIGHT_RUN_ID --store PATH` SHALL load both
selected stored records and produce a comparison based only on portable aggregate
data.  The comparison SHALL preserve the caller-provided left and right IDs and
their respective values; it SHALL NOT reorder them by saved time or run ID.
Comparison output SHALL cover the available run, task, and attempt terminal
statuses, reasons, partitioned state or partition-presence counts, and failure
presence and exception type.  It SHALL NOT claim to match individual partition
attempts or recover raw task outputs, exception objects, or raw partition-key
values.

#### Scenario: Preserve left and right comparison identity
- **WHEN** a caller compares two existing runs in an order that differs from their
  saved-time order
- **THEN** the output labels and values retain the supplied left run and supplied
  right run in that order and reports only portable aggregate differences

#### Scenario: Emit a single JSON comparison document
- **WHEN** a caller invokes `runs compare LEFT_RUN_ID RIGHT_RUN_ID` with
  `--format json`
- **THEN** stdout contains exactly one JSON document for the left/right portable
  aggregate comparison and all diagnostics remain on stderr

### Requirement: Classify run selection and store-read failures
History commands SHALL exit with status `2` when a requested run ID is unknown or
when a history-specific argument, including `--limit`, is invalid.  They SHALL
exit with status `4` when opening, validating, decoding, or reading the selected
store fails, including schema-version, malformed-record, SQLite, and filesystem
failures.  On either failure, the command SHALL write a diagnostic only to stderr
and SHALL not emit a successful text result or JSON document on stdout.

#### Scenario: Report an unknown selected run
- **WHEN** a caller invokes `runs show` for an absent run ID, or `runs compare`
  with either absent run ID
- **THEN** the command emits a diagnostic to stderr, emits no successful output to
  stdout, and exits with status `2`

#### Scenario: Report a malformed or incompatible existing store
- **WHEN** a caller selects an existing store whose schema, SQLite content, or
  stored portable record cannot be read or validated
- **THEN** the command emits a diagnostic to stderr, emits no successful output to
  stdout, and exits with status `4`
