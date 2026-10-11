## MODIFIED Requirements

### Requirement: Portable record versions are independent of database layout
The store SHALL retain database schema version 1 while saving new records as portable record version 3 in each row's record-version field. Version 1, 2 and 3 records SHALL both be readable without modifying historical rows or fabricating absent invocation histories. Unknown record versions SHALL be rejected. History listing SHALL expose each row's record version independently of the database version. The store SHALL save the explicit version 3 result projection and preserve ordered invocation history and the existing privacy boundary.

#### Scenario: Read legacy and current records together
- **WHEN** a version 1 database contains an old portable record and a newly saved retry result
- **THEN** load/list preserve record versions 1 and 2 and the old row remains unchanged
