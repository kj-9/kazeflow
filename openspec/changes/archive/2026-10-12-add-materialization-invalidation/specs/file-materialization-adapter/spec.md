## MODIFIED Requirements

### Requirement: Portable committed identity
Committed storage SHALL include a versioned self-describing manifest identifying the asset, explicit version, partition domain and typed key, ordered input materialization IDs, codec, record ID, and payload integrity metadata. Reuse SHALL require matching identity and codec. An absent reference SHALL be a miss; incompatible manifest versions and codecs, malformed references/manifests, missing referenced payloads, and integrity failures SHALL raise an adapter error. Records SHALL remain loadable after another commit for the same identity unless explicitly invalidated. New commits SHALL write reference and manifest schema version 2 carrying a nullable revocation epoch; legacy version 1 SHALL remain readable only when no revocation metadata exists for that identity. Unsupported versions SHALL fail explicitly. Revocation metadata SHALL use its own strict versioned format and stay relative to the storage root. Copying the storage root SHALL preserve reuse without depending on the original absolute location.

#### Scenario: Fresh process and copied root
- **WHEN** another process opens a copied store with the same codec and identity
- **THEN** it resolves and loads the committed output without executing the producer

#### Scenario: Identity change
- **WHEN** the asset version, domain, key, or input materialization IDs change
- **THEN** the previous identity is not reused

#### Scenario: Corruption or incompatible encoding
- **WHEN** a referenced artifact is corrupt, incomplete, or uses an incompatible schema or codec
- **THEN** resolve or load fails explicitly and never returns corrupted output

#### Scenario: Older record survives replacement
- **WHEN** another output is committed for an existing identity
- **THEN** the old record remains loadable if no invalidation occurred and subsequent resolve selects a complete committed generation
