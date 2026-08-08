## ADDED Requirements

### Requirement: Run detail option preserves output boundaries
The public `run` command SHALL support a text-only `--verbose` option for terminal
result detail.  JSON output SHALL remain exactly one portable `RunResult` document
on stdout, and an incompatible verbose/JSON selection SHALL be classified as usage
error before entry loading.

#### Scenario: Preserve JSON automation output
- **WHEN** a caller requests a successful JSON run without verbose detail
- **THEN** stdout contains exactly the portable result document and no text summary
