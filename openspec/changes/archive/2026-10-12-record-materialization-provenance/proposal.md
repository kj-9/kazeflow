## Why

Issue #2 follow-up 6 in ROADMAP requires auditable executed/materialized provenance across live results, events and stored portable history. Existing in-memory metadata is omitted by all portable projections.

## What Changes

- Add portable result and event version 4 with nullable resolution and materialization ID.
- Emit attempt-success provenance, save record v4 in SQLite database version 1, and read historical v1–v3 unchanged.
- Advance CLI run/history documents to version 4, publish schema collection v6 and compatible evidence; plan stays v4 and other documents v1.
- Document metadata trust/privacy and demonstrate execution versus reuse.
- Scope: observability and storage only. Non-goals: plan-time adapter lookups, invalidation, automatic resume, raw output/key persistence, scheduling and statistics.

## Capabilities

### New Capabilities
- `materialization-provenance`: validated success metadata in portable results/events and CLI presentation.

### Modified Capabilities
- `run-results`: add explicit v4 projection and clarify committed execution IDs while retaining older versions.
- `execution-events`: advance default projection to v4 with success metadata.
- `sqlite-run-store`: save/read v4 with validated provenance and unchanged database layout.
- `cli-json-contracts`: version 4 run/history documents and published schema compatibility.

## Impact

Additive event fields and explicit result projection; new CLI emitted version requires consumer migration. Core stays stdlib-only. Historical record and schema shapes remain unchanged. Adapter-defined IDs become portable diagnostic metadata and may contain application information; no redaction or raw-key reconstruction is promised.
