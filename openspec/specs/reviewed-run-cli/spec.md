# Reviewed run CLI

## Purpose

Specify deliberate execution through the `kazeflow` CLI.

## Requirements

### Requirement: Review before execution

`kazeflow run` SHALL resolve its entry and selections, build a pre-execution plan,
and write its human-readable summary to stderr before any asset body runs.

#### Scenario: Review before run
- **WHEN** a caller requests a valid run
- **THEN** stderr receives a preflight before asset execution

### Requirement: Explicit execution decision

When stdin and stderr are TTYs, only `y` or `yes` at the stderr prompt starts a
run. Otherwise `--yes` is required. Declining is exit `0` and SHALL not invoke
assets, initialize adapters, or create a result.

#### Scenario: Decline execution
- **WHEN** an interactive caller declines the prompt
- **THEN** the command exits successfully without side effects

### Requirement: Portable completed results

Text results SHALL be deterministic. JSON output SHALL be exactly one portable,
lossy `RunResult` document on stdout; review interaction and diagnostics remain on
stderr. Completed asset failure exits `1`.

#### Scenario: Emit a failed result
- **WHEN** an approved run has an asset failure
- **THEN** it emits a portable result and exits `1`

### Requirement: Explicit adapters

The default run path SHALL not initialize Rich or SQLite. `--tui` is lazy and
fails with `4` when unavailable. `--store PATH` creates and saves only after a
terminal result; an adapter failure exits `4` and suppresses final success output.

#### Scenario: Select unavailable TUI
- **WHEN** an approved caller selects an unavailable TUI
- **THEN** the command exits `4` before asset invocation

### Requirement: Inherited failures

Runs SHALL preserve the shared `0`/`1`/`2`/`3`/`4` classification, including
ambiguous discovered targets as `2` and entry failures as `3`.

#### Scenario: Require noninteractive approval
- **WHEN** either input stream is not a TTY and `--yes` is absent
- **THEN** the command exits `2` without running assets
