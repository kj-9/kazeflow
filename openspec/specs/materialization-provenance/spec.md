# materialization-provenance Specification

## Purpose
Expose successful execution and reuse metadata across live events and portable diagnostics while preserving historical records and application-owned materialization boundaries.

## Requirements

### Requirement: Successful provenance is explicit and truthful
Successful logical attempts SHALL report nullable `resolution` (`executed` or `materialized`) and nullable non-empty string `materialization_id`. Executed ordinary work SHALL have no ID; successfully committed executions SHALL retain their committed ID; reuse SHALL report materialized with an ID. Failed, skipped and cancelled outcomes SHALL have both fields null. Hand-constructed successful results MAY retain unknown resolution as null without an ID. Metadata SHALL reflect terminal logical outcomes rather than failed retry invocations.

#### Scenario: Commit followed by reuse
- **WHEN** a partition commits successfully and a later run reuses it
- **THEN** the first success reports executed with its ID and the second reports materialized with the same ID

#### Scenario: Failed commit
- **WHEN** commit fails after the body returns
- **THEN** terminal failure has no successful provenance and downstream work stays blocked

### Requirement: Success events expose terminal provenance
Execution events SHALL append optional resolution and materialization ID fields valid only on successful attempt-finished events. Their values SHALL match the corresponding result. Event projection version 4 SHALL add both nullable fields and preserve invocation metadata; versions 1–3 SHALL preserve their prior shapes and restrictions. Zero-argument event projection SHALL use version 4. Unknown and boolean versions SHALL be rejected.

#### Scenario: Retry and reuse observation
- **WHEN** execution recovers after a failed invocation or reuses a committed output
- **THEN** its single successful attempt finish reports the final provenance without attributing it to other lifecycle events

### Requirement: Provenance retains adapter trust boundaries
All projections SHALL continue to omit raw partition keys, outputs, exceptions and adapter code. Materialization IDs SHALL be treated as opaque application-defined diagnostic strings without interpreting them as keys or loading artifacts. Documentation SHALL explain that IDs can contain application information and are not automatically redacted. CLI verbose attempt text SHALL display known resolution and optional ID; history show SHALL retain the saved metadata. Inspection SHALL not resolve or load artifacts.

#### Scenario: Inspect saved reuse
- **WHEN** saved history contains a materialized success
- **THEN** inspection reports resolution and ID without accessing the materialization adapter
