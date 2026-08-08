## MODIFIED Requirements

### Requirement: Output and status classification

JSON success output SHALL be exactly one stdout document; diagnostics SHALL use
stderr. Portable JSON SHALL exclude raw outputs, exception objects, and raw
partition keys. Statuses SHALL be `0` success, `1` completed asset failure, `2`
usage/configuration failure, `3` entry resolution failure, and `4` infrastructure
or selected-adapter failure. Text output SHALL be a human-facing review projection,
not a byte-for-byte automation contract; graph and detail format selection SHALL
follow the public CLI compatibility policy.

#### Scenario: Emit portable JSON
- **WHEN** a successful command selects JSON
- **THEN** stdout contains one lossy JSON document and diagnostics use stderr

#### Scenario: Reject an invalid output selection
- **WHEN** a caller combines incompatible documented output options
- **THEN** the command exits `2` and writes its diagnostic only to stderr
