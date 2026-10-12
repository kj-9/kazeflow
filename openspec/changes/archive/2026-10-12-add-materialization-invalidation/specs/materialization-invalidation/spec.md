## Purpose

Preserve explicit application revocation of exact materialization identities and allow diagnostic inspection without executing assets, decoding outputs or silently reusing invalidated generations.

## ADDED Requirements

### Requirement: Invalidation is an optional explicit operation
An optional MaterializationInvalidation protocol SHALL provide invalidate(context, *, reason="explicit") returning None synchronously or asynchronously. Existing Materialization adapters SHALL NOT be required to implement it, and execution SHALL NOT invoke it automatically. FileMaterialization SHALL implement invalidation for the full exact identity (asset, version, domain, typed key/presence and ordered direct-input IDs). A non-empty string reason SHALL be required; invalid arguments SHALL fail before writes. Other identities SHALL remain unaffected, including falsey keys of distinct types. Each successful invalidation SHALL establish a fresh revocation epoch even when no output exists or a prior invalidation was recorded.

#### Scenario: Existing adapter remains usable
- **WHEN** an adapter provides only resolve/load/commit
- **THEN** existing planning and execution remain valid

#### Scenario: Exact identity isolation
- **WHEN** an application invalidates one version/input/key identity
- **THEN** that identity cannot reuse its prior generations and other variants remain reusable

### Requirement: Revocation survives stale artifacts while allowing deliberate rebuild
Successful file invalidation SHALL atomically persist revocation metadata without deleting outputs. Resolve SHALL return a miss for generations from prior epochs and load SHALL reject previously issued records from prior epochs. A later explicit commit SHALL publish in the current epoch and become reusable. Ordinary replacement commits without invalidation SHALL retain old record loadability. A failure before revocation publication SHALL preserve the prior state. Corrupt revocation metadata SHALL fail explicitly, never act as absent revocation. Copying the full updated root SHALL preserve revocation; restoring only old refs/objects while retaining revocation metadata SHALL NOT resurrect them. Restoring/deleting revocation metadata or rolling back the entire root SHALL be outside this guarantee. Callers SHALL quiesce users of the identity during invalidation; no cross-process locking or concurrent invalidation transaction is promised.

#### Scenario: Copied old artifacts do not revive
- **WHEN** old reference and generation files are restored into a copied invalidated root with its current revocation metadata intact
- **THEN** resolve returns a miss and loading the old record is rejected

#### Scenario: Explicit rebuild
- **WHEN** a new run explicitly selects an invalidated partition and successfully commits a new output
- **THEN** it executes the body, receives a new materialization ID and later runs reuse the new generation

#### Scenario: Failed invalidation
- **WHEN** atomic publication of revocation metadata fails
- **THEN** the old generation remains reusable and the error propagates

### Requirement: Inspection diagnoses storage without mutation
FileMaterialization.inspect(context) SHALL return an immutable FileMaterializationInspection with status available, missing, invalidated, incompatible or corrupt, nullable record and nullable reason. Only available SHALL have a committed record; missing SHALL have no reason; other statuses SHALL explain the diagnosis. Missing output without revocation SHALL be missing; stale or absent output with revocation SHALL be invalidated. Incompatible schema or codec SHALL be incompatible, malformed/integrity/unreadable storage SHALL be corrupt. Invalid context arguments SHALL still raise. Inspection SHALL perform no writes, body invocation or codec encode/decode calls. Resolve/load SHALL preserve explicit corruption and incompatibility errors rather than falling back to execution.

#### Scenario: Inspect missing or invalidated output
- **WHEN** a caller inspects absent storage or an explicitly invalidated identity
- **THEN** it receives missing or invalidated with no filesystem mutation

#### Scenario: Inspect incompatible or corrupt storage
- **WHEN** a referenced generation has an unsupported schema/codec or damaged payload
- **THEN** inspection reports incompatible or corrupt respectively without decoding output
