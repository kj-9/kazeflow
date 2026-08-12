# Run history CLI

## Purpose

Specify read-only local inspection of portable SQLite run records.
## Requirements
### Requirement: Project-local history store

`runs list`, `runs show`, and `runs compare` SHALL use `--store PATH` when given,
otherwise `./.kazeflow/runs.sqlite3` relative to the current directory. They SHALL
verify an existing regular file before opening it and never create, migrate, or
modify it. This SHALL not make ordinary `run` persistent.

#### Scenario: Read the default store
- **WHEN** a project-local default store exists and `--store` is omitted
- **THEN** the history command reads that existing file

### Requirement: Deterministic run listing
`runs list` SHALL list summaries by saved time and then run ID. `--limit` accepts
only non-negative integers. Summaries expose only ID, status, saved time, and the
stored portable-record schema version; JSON is one typed stdout document whose CLI
document version is distinct from each stored portable-record schema version.

#### Scenario: Limit ordered summaries
- **WHEN** a caller supplies a non-negative limit
- **THEN** the first summaries in stored ordering are returned in the typed list
  document

### Requirement: Portable record display
`runs show` SHALL display the stored portable envelope without rebuilding a
`RunResult`, preserving portable task/attempt order and partition presence while
excluding raw outputs, exception objects, and raw partition keys. Its typed JSON
document SHALL name the independently versioned CLI document, portable record, and
SQLite store schema contracts; it SHALL not promise redaction of exception messages
or tracebacks.

#### Scenario: Show a portable record
- **WHEN** a caller selects a stored run
- **THEN** its typed stored-run document identifies its envelope, portable-record,
  and store versions

### Requirement: Aggregate comparison
`runs compare` SHALL preserve caller left/right order and compare only portable
run/task aggregates: statuses, reasons, partitioned attempt counts, attempt-status
counts, and failure presence/type. It SHALL not claim to match individual partitions
or recover omitted values. JSON is one typed comparison document whose two embedded
stored records preserve their caller-supplied order and version identities.

#### Scenario: Compare two stored records
- **WHEN** a caller supplies distinct left and right run IDs
- **THEN** aggregate portable differences and embedded records preserve that order

### Requirement: History errors
Unknown run IDs and invalid history arguments SHALL exit `2`. Store opening,
schema, decoding, SQLite, and filesystem failures SHALL exit `4`. Error paths write
diagnostics only to stderr and no successful typed stdout document.

#### Scenario: Reject an unknown run
- **WHEN** a requested stored run is absent
- **THEN** the command exits `2` with a stderr diagnostic and no JSON document
