## ADDED Requirements

### Requirement: CLI-first command surface
The public command-line executable SHALL be named `kazeflow`; the project SHALL NOT publish `kz` as a CLI command. `kazeflow assets`, `kazeflow plan`, and `kazeflow run` SHALL be the basic commands for inspecting, reviewing, and executing a script-defined flow. The CLI SHALL supplement, rather than remove or deprecate, the existing Python APIs.

#### Scenario: Invoke a basic command
- **WHEN** a caller invokes `kazeflow assets`, `kazeflow plan`, or `kazeflow run` with a valid entry and required options
- **THEN** the CLI performs the corresponding inspection, review, or execution operation

#### Scenario: Preserve Python API use
- **WHEN** an existing caller uses the Python API without invoking the CLI
- **THEN** the availability of the CLI does not require that caller to change to CLI-based planning or execution

### Requirement: Script-first entry resolution
The CLI SHALL accept a bare Python file path as a first-class entry form, in addition to `module:attribute` and `path/to/file.py:attribute`. For an explicit attribute entry, the resolved attribute SHALL be a `Flow` or a zero-argument callable that returns a `Flow`; the callable SHALL be invoked at most once for one CLI invocation. The CLI SHALL permit factory resolution only for an explicitly named attribute and SHALL NOT implicitly discover or invoke factories. A missing or unloadable module or file, missing attribute, factory exception, or value that is not a `Flow` SHALL be reported as an entry resolution failure.

#### Scenario: Resolve a bare script with a declared flow
- **WHEN** a caller supplies a bare Python file path whose loaded module defines `flow` as a `Flow`
- **THEN** the CLI uses that `flow` as the script's declared flow

#### Scenario: Resolve a Flow attribute
- **WHEN** a caller supplies an importable module entry whose named attribute is a `Flow`
- **THEN** the CLI resolves that `Flow` for the requested command

#### Scenario: Resolve an explicitly selected Flow factory
- **WHEN** a caller supplies an entry whose explicitly named zero-argument callable returns a `Flow`
- **THEN** the CLI invokes the callable once and resolves its returned `Flow`

#### Scenario: Do not infer a factory from a bare script
- **WHEN** a caller supplies a bare Python file path that has no module-level `flow` and contains a callable capable of returning a `Flow`
- **THEN** the CLI does not invoke that callable unless the caller supplies its attribute explicitly

#### Scenario: Reject an invalid entry value
- **WHEN** a caller supplies an entry whose named attribute is neither a `Flow` nor a callable returning a `Flow`
- **THEN** the CLI reports an entry resolution failure and exits with status `3`

### Requirement: Asset discovery and default target selection
After loading a bare Python file, the CLI SHALL enumerate the assets registered while loading that file, and `assets` SHALL display those discovered assets whether or not the module declares `flow`. When no module-level `flow` is declared, `plan` and `run` SHALL derive suggested targets from the discovered assets that have no discovered downstream dependent asset, unless the caller selects target(s) explicitly. A bare script with neither a module-level `flow` nor discovered assets SHALL be an entry resolution failure.

#### Scenario: List assets from an undeclared script
- **WHEN** a caller runs `kazeflow assets path/to/file.py` for a loadable script with no module-level `flow` and registered assets
- **THEN** the CLI lists the assets discovered from that script

#### Scenario: Plan the suggested terminal assets
- **WHEN** a caller runs `kazeflow plan path/to/file.py` for an undeclared script with registered assets and supplies no target
- **THEN** the CLI displays the terminal asset target or targets it derived as suggestions and the corresponding plan

#### Scenario: Require an execution target when suggestions are ambiguous
- **WHEN** a caller runs `kazeflow run path/to/file.py` for an undeclared script with more than one suggested terminal target and supplies no target
- **THEN** the CLI reports a supplied-configuration failure, does not invoke an asset body, and exits with status `2`

#### Scenario: Run a selected discovered target
- **WHEN** a caller supplies a valid explicit target for an undeclared script with discovered assets
- **THEN** the CLI plans and runs the selected target using the existing flow semantics

### Requirement: Honest review safety boundary
The CLI SHALL state that loading an entry imports or executes user Python and can cause top-level side effects; an explicitly selected factory is also arbitrary user code that can have side effects. For any resolved flow or discovered-asset plan, the CLI `plan` operation SHALL obtain and display its plan without invoking an asset body. The CLI SHALL NOT represent either guarantee as a sandbox, safety proof, or automatic approval to run the flow.

#### Scenario: Plan a resolved flow
- **WHEN** a caller invokes the CLI `plan` operation for a resolved flow
- **THEN** the CLI creates and displays a plan without invoking an asset body

#### Scenario: Load side-effectful entry code
- **WHEN** a caller invokes a CLI command for an entry whose module import, file execution, or selected factory has a side effect
- **THEN** the CLI documentation and diagnostics preserve the distinction between entry-loading code and asset-body execution

### Requirement: Review and execution use existing flow semantics
The CLI `plan` operation SHALL describe selected targets and normalized run configuration using the resolved flow's or discovered assets' existing planning semantics. A CLI `run` operation SHALL use the same resolved entry and selected options within its own process, and SHALL return the existing structured run-outcome semantics. The CLI SHALL NOT promise that a plan printed in a different invocation is the same in-memory plan later executed.

#### Scenario: Review selected work
- **WHEN** a caller supplies valid target and run-configuration selections to the CLI `plan` operation
- **THEN** the displayed plan uses the same target closure, ordering, partition, and validation semantics as the Python API

#### Scenario: Run reports an asset failure
- **WHEN** a resolved flow or discovered-asset run completes with one or more failed asset attempts
- **THEN** the CLI treats the outcome as a completed structured run and exits with status `1`

### Requirement: Separate human and machine output
The CLI SHALL support human-oriented text output and a JSON output mode. In JSON mode, stdout SHALL contain exactly one JSON document for a successful asset listing, plan, or completed run, and diagnostics SHALL be written to stderr. Run JSON SHALL exclude raw asset outputs, exception objects, and raw partition-key values. Plan JSON SHALL use an explicitly documented lossy representation rather than serializing arbitrary Python partition-key objects.

#### Scenario: Emit JSON for a completed run
- **WHEN** a caller requests JSON output for a completed run containing raw output, an exception, or a partition key
- **THEN** stdout contains one JSON document without those raw values and diagnostics do not mix into stdout

#### Scenario: Report a command error in JSON mode
- **WHEN** a caller requests JSON output but supplies invalid CLI arguments or run configuration
- **THEN** the CLI writes diagnostics to stderr, does not emit a successful assets, plan, or run document, and exits with status `2`

### Requirement: Stable failure classification
The CLI SHALL use status `0` for a successful completed operation, `1` for a completed run with asset failure, `2` for command-line usage or supplied configuration failure, `3` for entry resolution failure, and `4` for execution infrastructure or explicitly selected adapter failure. A selected persistence or event consumer failure after a terminal run result SHALL take precedence over an asset-failure status.

#### Scenario: Fail to save an otherwise failed run
- **WHEN** a run has an asset failure and an explicitly selected persistence adapter then fails
- **THEN** the CLI exits with status `4` and reports the adapter failure

#### Scenario: Reject an invalid entry argument
- **WHEN** a caller supplies an invalid CLI entry syntax
- **THEN** the CLI exits with status `2` without attempting to load an entry

### Requirement: Explicit optional feature selection
The default CLI invocation SHALL NOT import or initialize Rich, create a database, or persist a run record. Rich presentation SHALL require an explicit selection and an installed TUI extra. SQLite persistence SHALL require an explicit store path and SHALL be attempted only after a terminal run result is available.

#### Scenario: Use the core-only CLI path
- **WHEN** a caller runs a CLI plan or run command without a TUI or store option
- **THEN** the command does not initialize Rich or SQLite persistence

#### Scenario: Select unavailable Rich presentation
- **WHEN** a caller explicitly selects Rich presentation without the TUI extra installed
- **THEN** the CLI reports an infrastructure or selected-adapter failure and exits with status `4`
