## Why

Issue #2 ROADMAP follow-up 7 requires explicit revocation and diagnostic inspection of materializations. Deleting only a reference can let copied old artifacts reappear without preserving the application's invalidation decision.

## What Changes

- Add an optional MaterializationInvalidation protocol, leaving the existing resolve/load/commit protocol and executor unchanged.
- Add FileMaterialization.invalidate(context, reason=...) with persistent exact-identity revocation and inspect(context) with structured available/missing/invalidated/incompatible/corrupt status.
- Version file references/manifests for revocation epochs; read legacy storage while preventing stale ref/object restoration when revocation metadata is retained.
- Document explicit rebuild, copy/restore boundaries, privacy and required quiescence during invalidation.
- Scope is caller-driven exact identity invalidation. Non-goals: cascading invalidation, artifact deletion/garbage collection, automatic resume, human-intent inference, cross-process locking, merging old full backups, CLI or portable record changes.

## Capabilities

### New Capabilities
- `materialization-invalidation`: explicit persistent revocation, safe rebuilding and diagnostic inspection.

### Modified Capabilities
- `file-materialization-adapter`: versioned epochs and record loadability subject to explicit invalidation.

## Impact

Additive stdlib-only protocol/API. Existing adapters remain compatible. New file schema version 2 is readable by updated code; older adapters reject it. Portable result/event/CLI/SQLite versions stay unchanged. Roots and revocation reasons remain trusted application data.
