## Purpose

Define the user-facing explanation of partition selection in the script-first CLI
workflow.

## Requirements

### Requirement: User documentation explains partition selection by example

Public documentation SHALL explain that a partitioned asset represents selectable
independent work slices and that an unpartitioned asset runs once. It SHALL include
a script-first example with a module-level `flow`, a plan-first command using
repeatable `--partition-key`, and a deliberate `run --yes` command using the same
selection.

#### Scenario: A new user follows the partition example
- **WHEN** a user follows the documented partition example
- **THEN** they can inspect selected partition count before execution and run only
the explicitly selected slices

### Requirement: User documentation distinguishes selection states

Public documentation SHALL state the observable distinction between omitting
`--partition-key`, passing one or more keys, and an explicit empty selection when
the underlying flow supports it. It SHALL state that string values are passed to
the flow's partition definition and that falsey values remain present selections.

#### Scenario: A user needs to rerun one slice
- **WHEN** documentation describes rerunning a single partition
- **THEN** it directs the user to pass that key, inspect the resulting plan, and
then explicitly approve the matching run
