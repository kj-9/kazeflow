## Purpose

Let applications deliberately collect and aggregate committed materializations from an explicit identity selection after partial failure, while preserving failure diagnostics, revocation and application-owned publication boundaries.

## ADDED Requirements

### Requirement: Collection is optional and explicitly scoped
An optional MaterializationCollection protocol SHALL expose available(contexts), returning an ordered tuple of committed MaterializationRecord values synchronously or asynchronously. Existing Materialization adapters SHALL NOT be required to implement collection. FileMaterialization SHALL accept a non-text sequence of MaterializationContext values and snapshot it. Every context SHALL obey existing identity/key validation, and duplicate exact identities SHALL be rejected before storage access. Collection SHALL query only the explicit contexts, preserving their order among available results, with typed falsey keys distinct. Empty input SHALL return an empty tuple without storage access. It SHALL NOT scan the root, infer keys from stored history, or automatically include other partitions, versions, input IDs or prior runs.

#### Scenario: Exact scope includes deliberately selected prior outputs
- **WHEN** contexts explicitly select compatible outputs from a prior run and current run
- **THEN** only their currently published committed records are returned in supplied context order

#### Scenario: Invalid or duplicate selection
- **WHEN** input is not a non-text context sequence or two contexts have the same exact identity
- **THEN** collection rejects it before storage access

### Requirement: Availability respects revocation and integrity
File collection SHALL use the same resolve semantics as single identity lookup. Missing and invalidated identities SHALL be omitted. Corruption, unsupported schemas, incompatible codecs and unreadable storage SHALL fail explicitly; they SHALL NOT silently remove an artifact from aggregation. Collection SHALL not decode output, write storage, invoke assets or change RunResult status. Returned records SHALL remain subject to existing load-time integrity/revocation checks. Collection and later load SHALL NOT imply a multi-record transaction or locking; callers needing a stable set SHALL coordinate storage users.

#### Scenario: Partial availability after failure
- **WHEN** some selected identities have committed outputs, others are missing or invalidated and the producer run failed
- **THEN** available records can be inspected and explicitly loaded while the producer result remains failed

#### Scenario: Revocation after collection
- **WHEN** an artifact is invalidated after its record was collected
- **THEN** loading that record rejects it according to the invalidation contract

#### Scenario: Corrupt candidate
- **WHEN** one selected currently published candidate is damaged or incompatible
- **THEN** collection raises rather than publishing a silently incomplete set

### Requirement: Aggregation and publication remain application choices
Documentation and a runnable example SHALL show ordinary Python explicitly loading available outputs, aggregating them after a completed partially failed run, and publishing one complete local snapshot with receipt metadata. The example SHALL atomically replace one file containing rows and its receipt (stable publication identity and included materialization IDs), so failure before replacement preserves the old complete publication. Repeating the same record/output selection SHALL produce the same publication identity. The example SHALL report producer failure without relabeling it as success. Core collection SHALL not aggregate, publish, resume or promise external-sink transactions/exactly-once effects. Documentation SHALL distinguish diagnostic run history, materialization storage and sink receipts, including plaintext application-data privacy.

#### Scenario: Deliberate atomic local publication
- **WHEN** the application chooses available records after partial failure
- **THEN** the example publishes their complete aggregate and receipt in a single replacement while retaining failed producer diagnostics

#### Scenario: Publication failure preserves the prior snapshot
- **WHEN** replacing the snapshot fails
- **THEN** the old complete snapshot remains and no new receipt is reported as committed
