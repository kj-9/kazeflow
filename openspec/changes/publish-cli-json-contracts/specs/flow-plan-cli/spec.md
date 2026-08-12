## MODIFIED Requirements

### Requirement: Output and core boundary
Text and one-document typed JSON plans and partition-definition inspection SHALL
separate stdout from diagnostics. In JSON mode, user-Python stdout emitted while
loading the entry or factory SHALL be written to stderr so stdout contains only the
completed typed document. Their documented lossy projections SHALL expose selection
kind, stable domain, definition metadata, and counts while omitting arbitrary raw
partition keys. Default inspection SHALL not require Rich, SQLite, persistence,
execution, or a mandatory third-party runtime dependency.

#### Scenario: Use core-only inspection
- **WHEN** an installation has no optional extras
- **THEN** `assets`, `partitions`, and `plan` remain available

#### Scenario: Preserve a JSON inspection document
- **WHEN** loaded entry code prints while a caller requests JSON assets, partitions,
  or plan output
- **THEN** stdout remains one typed inspection document and the entry text is sent
  to stderr
