## MODIFIED Requirements

### Requirement: Output and status classification
Completed JSON outcomes SHALL be exactly one typed stdout document with
`document_type`, document-scoped `schema_version`, and schema-defined `data`;
diagnostics SHALL use stderr. JSON-mode entry loading, explicit factories, and
approved asset execution SHALL redirect user-Python stdout to stderr so it cannot
corrupt the completed document. Portable JSON SHALL exclude arbitrary outputs, raw
exception objects, and raw partition keys, but exception messages and tracebacks
remain potentially sensitive application-controlled metadata. Statuses SHALL be
`0` success or declined JSON run, `1` completed asset failure, `2` usage or
configuration failure, `3` entry resolution failure, and `4` infrastructure or
selected-adapter failure. Text output SHALL be a human-facing review projection,
not a byte-for-byte automation contract; graph and detail format selection SHALL
follow the public CLI compatibility policy.

#### Scenario: Emit portable JSON
- **WHEN** a completed command selects JSON
- **THEN** stdout contains one lossy typed JSON document and diagnostics plus
  user-Python print output use stderr

#### Scenario: Reject an invalid output selection
- **WHEN** a caller combines incompatible documented output options
- **THEN** the command exits `2` and writes its diagnostic only to stderr without a
  successful document on stdout

### Requirement: Run detail option preserves output boundaries
The public `run` command SHALL support a text-only `--verbose` option for terminal
result detail. JSON output SHALL remain exactly one typed portable run-result
document on stdout, and an incompatible verbose/JSON selection SHALL be classified
as usage error before entry loading.

#### Scenario: Preserve JSON automation output
- **WHEN** a caller requests a successful JSON run without verbose detail
- **THEN** stdout contains exactly one typed portable result document and no text
  summary
