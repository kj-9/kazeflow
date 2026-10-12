# file-materialization-adapter Specification

## Purpose
Provide explicitly configured local file persistence for partitioned asset outputs, allowing verified committed outputs to be reused across Python processes without changing execution or portable result contracts.

## Requirements

### Requirement: Explicit storage and encoding
The adapter SHALL require a storage root and an explicit codec with a non-empty stable identifier and byte encoding/decoding operations. Construction and planning SHALL perform no filesystem writes or codec calls. The provided JSON codec SHALL accept only JSON data without lossy conversion of Python types or non-finite numbers. Supported partition keys SHALL be exact builtin strings, booleans, integers, finite floats, plain dates, or absent values; unsupported keys SHALL fail before publication. Typed keys SHALL distinguish falsey values and equal values of different types.

#### Scenario: Explicit activation
- **WHEN** an adapter is constructed and included in a plan
- **THEN** no storage directory or artifact is created and no output is encoded

#### Scenario: Typed keys
- **WHEN** records are committed for keys 0, false, and an empty string
- **THEN** each identity resolves its own record without aliasing

#### Scenario: Unsupported value
- **WHEN** an unsupported partition key or JSON output is committed
- **THEN** the operation fails and does not publish a reusable record

#### Scenario: Date partition compatibility
- **WHEN** the built-in date partition definition normalizes a selected ISO date into a plain date value
- **THEN** the adapter commits and reuses that date without aliasing an ISO string key or accepting datetime values

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

### Requirement: Atomic publication
The adapter SHALL publish a reference only after a complete manifest and encoded payload have been written. Readers SHALL observe complete old or new generations during concurrent publication. A commit that fails before reference publication SHALL leave the previous reference reusable, or leave a miss if none existed. Encoding, decoding, and storage errors SHALL propagate as attempt failures under the existing executor contract. Atomic visibility SHALL not imply exactly-once external side effects or power-loss durability.

#### Scenario: Failure before publication
- **WHEN** writing a new generation or replacing its reference fails
- **THEN** no partial new generation is reusable and any previously committed record is preserved

#### Scenario: Concurrent commits
- **WHEN** writers concurrently commit the same or different identities
- **THEN** every returned committed record loads its own complete output and subsequent resolve observes a complete generation

#### Scenario: Downstream failure and restart
- **WHEN** an upstream output was committed and a later task fails
- **THEN** a fresh run reuses the upstream output and retries the failed downstream work
