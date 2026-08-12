## ADDED Requirements

### Requirement: Machine-readable CLI contract reference
The hosted documentation SHALL provide a command-indexed machine-readable CLI
reference for `assets`, `partitions`, `plan`, `run`, `runs list`, `runs show`, and
`runs compare`. It SHALL describe the common JSON envelope, stable document types,
schema-version interpretation, complete field reference, normative Draft 2020-12
schemas, and representative golden documents. It SHALL state that human text is for
review and that automation MUST use JSON rather than parse text layout.

#### Scenario: Automation user selects a command
- **WHEN** a user needs to consume a plan or stored-run result programmatically
- **THEN** the hosted reference lets them find that command's document type, schema,
  fields, and representative document

### Requirement: JSON stream, exit, and privacy guidance
The hosted documentation SHALL explain that a completed JSON command writes one
typed document to stdout and routes diagnostics, prompts, progress, and stdout from
loaded/factory/asset user Python to stderr. It SHALL publish the `0`/`1`/`2`/`3`/`4`
exit/document matrix, including the typed document emitted for an interactive
declined JSON run. It SHALL distinguish structural omission of raw outputs,
exception objects, and raw partition keys from redaction: exception messages and
tracebacks can still contain application-controlled sensitive values.

#### Scenario: User designs a pipe-safe run
- **WHEN** a user reads the JSON run reference before piping stdout to another tool
- **THEN** they can identify the one-document stdout guarantee, stderr behavior,
  completed-failure exit `1`, and declined-run document

### Requirement: Alpha schema evolution guidance
The hosted documentation SHALL state that `schema_version` is scoped by
`document_type`, that the portable run-record and SQLite store versions are separate
contracts, and that incompatible schema changes receive a new version with release
migration guidance. It SHALL state the alpha policy: current documented versions are
emitted, obsolete versions are not promised indefinitely, and removal is announced
in release documentation.

#### Scenario: Consumer encounters a future version
- **WHEN** an automation receives an unsupported version for a known document type
- **THEN** the documentation directs it to reject or explicitly handle that version
  rather than assuming a field-compatible payload
