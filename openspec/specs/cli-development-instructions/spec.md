# cli-development-instructions Specification

## Purpose
Let a developer or coding agent retrieve concise, version-matched guidance from the installed kazeflow CLI before defining or inspecting a flow, without loading application code.

## Requirements

### Requirement: Entry-free offline instructions command

The public CLI SHALL provide `kazeflow instructions`. Without an entry or optional feature, it SHALL emit one human-readable Markdown document to stdout and exit `0`. The command SHALL work from a core-only installed wheel without network access, persistent state changes, application import, factory invocation, or asset body execution.

#### Scenario: Read instructions in an empty project
- **WHEN** a user invokes `kazeflow instructions` from a directory without a flow entry
- **THEN** stdout contains the Markdown guide, stderr is empty, and the process exits `0`

#### Scenario: Read from an installed core-only wheel
- **WHEN** a user installs the core-only wheel and invokes `kazeflow instructions` offline
- **THEN** the guide is available with no additional runtime dependency

### Requirement: Accurate, version-matched guidance

The guide SHALL describe discoverable top-level asset definitions, module-level `flow` and explicit zero-argument factories, the distinction between dynamic registration and CLI discovery, import-time effects and dependency requirements, explicit partition selection, and inspection through the commands and options available in that installed version. It SHALL state that planning does not invoke asset bodies but does not sandbox imported Python. It SHALL link to the authoritative hosted documentation for detail.

#### Scenario: Agent prepares a partitioned flow
- **WHEN** an agent reads the guide before writing a partitioned script
- **THEN** it can identify how to expose a CLI-discoverable flow, inspect assets and partitions, and explicitly select partitions when planning

#### Scenario: User evaluates an unfamiliar script
- **WHEN** a user reads the guide before planning an unfamiliar entry
- **THEN** the guide warns that entry loading may run Python and that `plan` is not a safety proof

### Requirement: Discoverability and compatibility

The command SHALL be discoverable from top-level CLI help and the public documentation. It SHALL be an additive text command; existing command outputs, JSON schemas, Python APIs, and exit classifications SHALL remain unchanged. Unsupported arguments SHALL use the existing usage-error classification.

#### Scenario: Discover from CLI help
- **WHEN** a user invokes `kazeflow --help`
- **THEN** the help lists `instructions` and its purpose

#### Scenario: Reject an entry argument
- **WHEN** a user invokes `kazeflow instructions some.py`
- **THEN** the CLI exits `2` with a diagnostic on stderr and no guide on stdout
