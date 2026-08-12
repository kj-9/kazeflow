## ADDED Requirements

### Requirement: Typed machine-readable CLI envelope
Every completed `--format json` CLI outcome SHALL write one JSON object with exactly
the top-level fields `document_type`, `schema_version`, and `data`. `document_type`
SHALL identify one of `kazeflow.assets`, `kazeflow.partitions`, `kazeflow.plan`,
`kazeflow.run-result`, `kazeflow.run-declined`, `kazeflow.runs-list`,
`kazeflow.runs-show`, or `kazeflow.runs-compare`; `schema_version` SHALL be a
positive integer whose interpretation is scoped to that document type. `data` SHALL
conform to the schema for that type and version.

#### Scenario: Identify a plan without command context
- **WHEN** an automation reads a successful JSON plan document from stdout
- **THEN** it can identify `kazeflow.plan` and its schema version from the document
  itself before interpreting `data`

#### Scenario: Identify a declined JSON run
- **WHEN** an interactive caller declines `kazeflow run --format json`
- **THEN** stdout contains a `kazeflow.run-declined` document rather than an empty
  successful stream

### Requirement: Normative schemas and golden documents
The repository SHALL publish Draft 2020-12 JSON Schema for every supported
`document_type` and `schema_version`, plus representative checked-in golden JSON
documents for assets, partitions, plan, completed run results, declined runs, run
listing, stored-run display, and run comparison. Each schema SHALL specify required
and optional fields, value types, allowed status and selection values, nesting,
array item shape, and whether additional properties are accepted. The schemas and
goldens SHALL describe the emitted envelope rather than requiring an automation to
infer a command-specific payload shape.

#### Scenario: Validate a representative document
- **WHEN** a maintainer validates a checked-in golden plan document against its
  published schema
- **THEN** validation succeeds without relying on prose-only field descriptions

#### Scenario: Reject an incomplete envelope
- **WHEN** a JSON value lacks `document_type`, `schema_version`, or `data`
- **THEN** it does not validate as a completed kazeflow CLI document

### Requirement: Portable run-record boundary inside CLI documents
A completed-run document and any stored-run document SHALL name the nested portable
run-record contract and its own version independently of the enclosing CLI document
version and the SQLite store schema version. Their published schemas SHALL preserve
the existing portable record boundary: arbitrary task outputs, raw exception
objects, and dedicated raw partition-key fields are omitted. Exception type,
message, and traceback remain portable failure metadata and MUST be documented as
potentially application-controlled or sensitive values rather than redacted data.

#### Scenario: Inspect separate version domains
- **WHEN** an automation receives a stored-run JSON document
- **THEN** it can distinguish the CLI document version, portable run-record version,
  and SQLite store schema version without treating those values as interchangeable

#### Scenario: A failure repeats a partition value
- **WHEN** application code puts a partition value in an exception message
- **THEN** a run-result or stored-run document omits the dedicated raw key field but
  does not promise to remove the repeated value from failure metadata

### Requirement: JSON stdout integrity
While a JSON-format command is loading an entry, invoking an explicit factory, or
executing an approved asset, text written by that user Python to standard output
SHALL be redirected to standard error. CLI prompts, preflight, progress, and
diagnostics SHALL also use standard error. A completed JSON outcome SHALL therefore
write exactly one document and no non-JSON bytes to standard output. This guarantee
does not make loaded or executed Python safe, suppress its side effects, or
serialize arbitrary application output.

#### Scenario: User code prints during a JSON run
- **WHEN** an approved asset calls `print()` during `kazeflow run --format json`
- **THEN** the printed text is written to stderr and stdout remains one valid
  run-result document

#### Scenario: Entry loading prints during JSON inspection
- **WHEN** a loaded entry writes to stdout during `kazeflow plan --format json`
- **THEN** the entry text is written to stderr and stdout remains one valid plan
  document

### Requirement: Deterministic document arrays
The published schemas and golden documents SHALL define the order of every emitted
array. Assets SHALL use deterministic asset-name order; plan tasks and dependencies
SHALL use resolved dependency-first plan order; partition attempts SHALL use
normalized selection order; run-result tasks SHALL use plan order; history listings
SHALL use saved-time then run-ID order; and comparison documents SHALL preserve the
caller-supplied left/right order. Object member order is not a semantic compatibility
guarantee.

#### Scenario: Compare the same two runs in reverse
- **WHEN** a caller invokes `runs compare` with the same IDs in opposite argument
  order
- **THEN** each output preserves the supplied left and right identities and does not
  reorder them by saved time or identifier

### Requirement: Alpha JSON compatibility policy
For a published `document_type` and `schema_version`, kazeflow SHALL preserve the
schema-defined fields, types, ordering, and meanings. A change that removes or
renames a field, changes its type or meaning, changes array ordering, or otherwise
invalidates a document schema SHALL use a new schema version for that document type
and include migration guidance in release documentation. During alpha, kazeflow
SHALL emit the currently documented version and MUST NOT promise indefinite support
for parsing or emitting obsolete versions; any removal of a previously documented
version SHALL be announced in release documentation.

#### Scenario: Change a plan field incompatibly
- **WHEN** a future release changes the type or meaning of a published plan field
- **THEN** it publishes a new `kazeflow.plan` schema version and migration guidance
  instead of silently changing the existing version

### Requirement: JSON exit and document matrix
For JSON-format commands, a successful inspection, history operation, or successful
run SHALL exit `0` and emit its typed completed document. A declined interactive
JSON run SHALL exit `0` and emit `kazeflow.run-declined`. A completed run with an
asset failure SHALL exit `1` and emit `kazeflow.run-result`. Usage or configuration
failures SHALL exit `2`, entry-resolution failures SHALL exit `3`, and infrastructure
or selected-adapter failures SHALL exit `4`; those `2`/`3`/`4` paths SHALL emit no
successful JSON document to stdout and SHALL write their diagnostics to stderr.

#### Scenario: Completed asset failure remains inspectable
- **WHEN** an approved JSON run reaches a terminal asset failure
- **THEN** it exits `1` with one typed run-result document on stdout

#### Scenario: Adapter failure suppresses a completed document
- **WHEN** a selected persistence or presentation adapter fails
- **THEN** the command exits `4`, writes diagnostics to stderr, and emits no
  successful JSON document to stdout
