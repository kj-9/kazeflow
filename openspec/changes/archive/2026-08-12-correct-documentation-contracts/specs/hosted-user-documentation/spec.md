## ADDED Requirements

### Requirement: Truthful current Partition guidance
The hosted documentation SHALL state that current planning requires an explicit
partition-key selection whenever the selected dependency closure contains a
partitioned task. It MUST distinguish `DatePartitionDef.range()` as a Python helper
from CLI key parsing or validation and MUST NOT claim that a Partition definition
implicitly chooses work, validates CLI strings, or manufactures a current date.

#### Scenario: User omits a required Partition selection
- **WHEN** a user follows the hosted guidance for a partitioned flow without passing
  a Partition selection
- **THEN** the documentation predicts a configuration error before any asset body
  runs and shows how to provide an explicit key

#### Scenario: User supplies a textual CLI key
- **WHEN** a user supplies `--partition-key VALUE`
- **THEN** the documentation describes the current CLI value as an explicitly
  selected string and does not imply definition-owned parsing or validation

#### Scenario: User needs a date range in Python
- **WHEN** a user needs several date keys through the Python API
- **THEN** the documentation demonstrates `DatePartitionDef.range()` separately and
  passes its returned keys explicitly in run configuration

### Requirement: Precise portable-record sensitivity boundary
The hosted documentation SHALL distinguish omission of dedicated raw output,
exception-object, and structural partition-key fields from confidentiality of the
remaining portable failure metadata. Every user journey that emits or persists a
portable record MUST state or directly link to the fact that exception messages and
tracebacks can contain application-controlled and sensitive values.

#### Scenario: An exception repeats a Partition key
- **WHEN** a partitioned asset includes its raw key in an exception message or
  traceback and a portable record is produced
- **THEN** the documentation predicts that the structural key field is absent but
  the repeated value can remain in failure metadata

#### Scenario: User persists a record
- **WHEN** a user chooses JSON output or SQLite storage
- **THEN** adjacent guidance tells them to handle the record as potentially
  sensitive rather than presenting the portable projection as redacted or secret-safe

### Requirement: Accurate external cancellation guidance
The hosted documentation SHALL distinguish cancellation statuses accepted by result
and storage models from the current executor control flow. It SHALL state that
externally cancelling an awaited `Flow.run_async()` re-raises
`asyncio.CancelledError` and produces no synthetic terminal `RunResult` or complete
terminal event sequence.

#### Scenario: Caller cancels an asynchronous run
- **WHEN** a caller externally cancels the task awaiting `Flow.run_async()`
- **THEN** the documentation predicts propagated cancellation without a returned
  terminal result and does not direct the caller to inspect a synthetic cancelled run

#### Scenario: User reads the status model reference
- **WHEN** the API or concepts reference lists a representable `cancelled` status
- **THEN** it does not imply that external executor cancellation currently returns
  that status through a public cancellation-result API

### Requirement: Verifiable first-run environment guidance
The hosted getting-started journey SHALL identify supported Python versions, show a
virtual-environment installation path, distinguish core and quoted TUI-extra
installation, and provide commands that let a user verify the installed CLI before
loading a Python entry.

#### Scenario: New user prepares an isolated environment
- **WHEN** a new user follows the first installation path
- **THEN** they can create and activate a virtual environment, install the
  zero-required-dependency core, and verify `kazeflow --help`

#### Scenario: zsh user installs the optional TUI
- **WHEN** a zsh user chooses the Rich progress display
- **THEN** the documented extra is quoted so shell glob expansion does not reject
  the command
