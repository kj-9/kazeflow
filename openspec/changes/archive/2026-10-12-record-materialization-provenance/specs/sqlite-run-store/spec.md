## MODIFIED Requirements

### Requirement: Portable record versions are independent of database layout
The store SHALL retain database schema version 1 while saving new records as portable record version 4 in each row's record-version field. Version 1, 2, 3 and 4 records SHALL remain readable without modifying historical rows or fabricating absent histories or provenance. Unknown record versions SHALL be rejected. History listing SHALL expose each row's record version independently of database version. The store SHALL preserve ordered invocation history and the privacy boundary. It SHALL reject provenance fields in versions 1–3 and missing or invalid provenance fields in version 4. Version 4 metadata SHALL obey successful provenance validation, including materialized requiring a non-empty ID and non-success requiring nulls.

#### Scenario: Read legacy and current records together
- **WHEN** a version 1 database contains historical records and a newly saved result
- **THEN** load/list preserve versions 1–4 and old rows remain unchanged

#### Scenario: Reject malformed provenance
- **WHEN** a saved v4 record claims materialized without an ID or successful provenance on failure
- **THEN** load rejects the record
