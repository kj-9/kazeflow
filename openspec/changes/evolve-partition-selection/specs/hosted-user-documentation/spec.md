## MODIFIED Requirements

### Requirement: Truthful current Partition guidance
The hosted documentation SHALL state that planning requires an explicit selection
whenever the selected dependency closure contains a partitioned task. It SHALL
explain that the partition definition normalizes or rejects selected keys before
asset execution; `DatePartitionDef` accepts strict ISO `YYYY-MM-DD` CLI keys and
provides an explicit inclusive bounded range. It SHALL demonstrate repeated keys,
bounded range selection, and deliberate empty selection separately and MUST NOT
claim that a definition implicitly chooses today, all history, or unbounded work.

#### Scenario: User omits a required Partition selection
- **WHEN** a user follows the hosted guidance for a partitioned flow without passing a Partition selection
- **THEN** the documentation predicts a configuration error before any asset body runs and shows how to choose an explicit selection form

#### Scenario: User supplies a textual date key
- **WHEN** a user supplies `--partition-key 2026-08-11` to a date-partitioned flow
- **THEN** the documentation predicts strict validation and normalization to an in-memory date before execution

#### Scenario: User needs a bounded date range
- **WHEN** a user needs several consecutive date keys
- **THEN** the documentation demonstrates an explicit inclusive range in both CLI and Python without implying an unbounded catalog

#### Scenario: User deliberately selects no partition work
- **WHEN** a user wants to review or execute an empty selection
- **THEN** the documentation distinguishes the explicit empty selector and its skipped no-work result from omitted configuration

