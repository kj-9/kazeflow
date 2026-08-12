## Purpose

Define the user-facing explanation of partition selection in the script-first CLI
workflow.
## Requirements
### Requirement: User documentation explains partition selection by example

Public documentation SHALL explain that a partitioned asset represents selectable
independent work slices and that an unpartitioned asset runs once. It SHALL include
a script-first date example with a module-level `flow`, non-executing definition
inspection, plan-first commands using repeatable `--partition-key` and a bounded
`--partition-range`, and a deliberate `run --yes` command using the reviewed selection.

#### Scenario: A new user follows the partition example
- **WHEN** a user follows the documented partition example
- **THEN** they can inspect the definition and normalized selection count before execution and run only the explicitly selected slices

### Requirement: User documentation distinguishes selection states

Public documentation SHALL state the observable distinction between omitting a
selector, passing one or more keys, passing one bounded range, and selecting empty
work explicitly. It SHALL state that partition definitions own normalization and
validation, that falsey values accepted by a custom definition remain present
selections, and that portable projections omit structural raw keys without sanitizing
application-controlled failure metadata.

#### Scenario: A user needs to rerun one slice
- **WHEN** documentation describes rerunning a single partition
- **THEN** it directs the user to pass that key, inspect the normalized plan, and then explicitly approve the matching run

#### Scenario: A user enters an invalid date
- **WHEN** documentation describes a malformed date key or reversed range
- **THEN** it predicts a preflight configuration error before asset execution

#### Scenario: A completed attempt records time consistently
- **WHEN** an attempt starts and later reaches a terminal status
- **THEN** its result contains aware UTC start and end timestamps and a non-negative monotonic duration

#### Scenario: The system clock changes during a run
- **WHEN** wall-clock time changes while an attempt is running
- **THEN** the recorded duration remains based on the monotonic elapsed interval
