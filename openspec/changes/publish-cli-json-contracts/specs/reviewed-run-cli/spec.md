## MODIFIED Requirements

### Requirement: Explicit execution decision
When stdin and stderr are TTYs, only `y` or `yes` at the stderr prompt starts a
run. Otherwise `--yes` is required. Declining is exit `0` and SHALL not invoke
assets, initialize adapters, or create a result. A declined JSON run SHALL emit one
typed declined-run document on stdout; a declined text run SHALL retain its
human-facing stderr review behavior without a result document.

#### Scenario: Decline execution
- **WHEN** an interactive caller declines the prompt
- **THEN** the command exits successfully without asset or adapter side effects

#### Scenario: Decline a JSON run
- **WHEN** an interactive caller declines `kazeflow run --format json`
- **THEN** stdout contains one typed declined-run document and no `RunResult` is
  created

### Requirement: Portable completed results
Text results SHALL be deterministic. JSON output for a completed run SHALL be
exactly one typed, lossy run-result document on stdout, with its portable
run-record contract version distinguished from the CLI document version; review
interaction, diagnostics, progress, and user-Python stdout remain on stderr.
Completed asset failure exits `1`.

#### Scenario: Emit a failed result
- **WHEN** an approved run has an asset failure
- **THEN** it emits a typed portable result document and exits `1`

### Requirement: Inherited failures
Runs SHALL preserve the shared `0`/`1`/`2`/`3`/`4` classification, including
ambiguous discovered targets as `2` and entry failures as `3`. JSON usage,
resolution, infrastructure, or selected-adapter failure SHALL emit no successful
document; only completed results and interactive declines receive typed JSON
documents.

#### Scenario: Require noninteractive approval
- **WHEN** either input stream is not a TTY and `--yes` is absent
- **THEN** the command exits `2` without running assets or emitting a successful
  JSON document
