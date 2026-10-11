## ADDED Requirements

### Requirement: Text plan output identifies selected resource limits
The CLI text plan SHALL show each selected named resource and its concurrency limit, plus the resource association of each selected asset. It SHALL not imply a cross-run rate limit or API-call quota. Flows without resources SHALL remain readable without an empty resource table.

#### Scenario: Review a shared resource
- **WHEN** a CLI plan includes two assets using one resource
- **THEN** text output shows the shared resource limit and both task associations before any asset body executes
