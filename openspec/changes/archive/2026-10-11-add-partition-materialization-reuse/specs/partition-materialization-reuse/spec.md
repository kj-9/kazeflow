## Purpose

Allow selected partitioned asset outputs to be reused across local runs only through an explicit adapter and a committed, compatible identity, while preserving ordinary Python execution for other assets.

## ADDED Requirements

### Requirement: Materialization is explicit per partitioned asset
A partitioned asset SHALL be able to declare a materialization adapter and a non-empty explicit asset version. The adapter SHALL provide resolve, load, and commit operations; no adapter SHALL be selected implicitly. Planning SHALL reject an incomplete declaration or a materialized asset with a selected dependency that does not also provide a committed materialization identity. Flows without declarations SHALL retain their existing behavior.

#### Scenario: No adapter
- **WHEN** a selected asset has no materialization declaration
- **THEN** it follows the existing execution path without storage access

#### Scenario: Incomplete declaration
- **WHEN** a selected asset declares materialization without a usable version or has a dependency without materialization
- **THEN** planning fails before asset invocation or run events

### Requirement: Only compatible committed output is reused
For each selected partition key, the executor SHALL ask the adapter whether a committed record matches the asset name, explicit version, normalized partition domain and key, and committed direct-input materialization identities. Only a compatible committed record SHALL be loaded and passed to matching downstream work without invoking the asset body. Missing, incompatible, invalid, or uncommitted state SHALL NOT be reused. The adapter SHALL decide how keys and outputs are serialized; the core SHALL NOT pickle arbitrary outputs or infer versions from Python source.

#### Scenario: Reuse after downstream failure
- **WHEN** a committed upstream partition exists and a later run repeats that key after a downstream failure
- **THEN** the upstream body is not invoked and its loaded output is supplied to the matching downstream attempt

#### Scenario: Version or input changes
- **WHEN** the asset version or one committed direct-input identity differs from the stored record
- **THEN** the old output is not reused and the asset body executes

#### Scenario: Falsey key
- **WHEN** a selected normalized partition key is `0`, `False`, or `""`
- **THEN** it is treated as a present key and is never confused with an unpartitioned attempt

### Requirement: Commit is the downstream visibility boundary
After an asset body succeeds, the executor SHALL await adapter commit and accept only a committed record before treating that attempt as successful input to downstream work. A resolve, load, or commit failure SHALL fail that attempt rather than silently fall back to an unverified output. A failed commit SHALL not expose the uncommitted output to downstream attempts. External cancellation SHALL retain the current no-synthetic-result behavior; only a completed atomic adapter commit MAY be reusable on a later run.

#### Scenario: Commit failure
- **WHEN** an asset returns an output but its adapter commit fails
- **THEN** the attempt fails and its downstream matching-key attempt does not run

#### Scenario: Load failure
- **WHEN** resolve reports reusable state but loading it fails
- **THEN** the attempt fails without invoking the asset as an implicit fallback

### Requirement: Storage and cross-run safety remain adapter responsibilities
The adapter SHALL be explicitly selected application code responsible for atomic commits, validation, storage lifecycle, and any encoding or persistence of raw partition keys. The core SHALL not require a database, filesystem store, or third-party runtime package. This capability SHALL NOT promise cross-process locking, external sink transactions, or automatic interpretation of human edits and deletions.

#### Scenario: Two runs race on the same key
- **WHEN** independent runs process the same partition concurrently
- **THEN** the core makes no cross-run exactly-once claim and relies on the selected adapter's commit semantics
