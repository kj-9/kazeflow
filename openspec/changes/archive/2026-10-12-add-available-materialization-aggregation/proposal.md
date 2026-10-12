## Why

Issue #2 ROADMAP follow-up9 needs a deliberate way to aggregate committed outputs when some partitions failed, including intentionally selected prior-run outputs. Normal dependency failure behavior must remain intact, and artifact availability must stay separate from run success and sink publication.

## What Changes

- Add optional MaterializationCollection protocol and FileMaterialization.available(contexts), returning ordered committed records for explicitly supplied exact identities.
- Skip missing/invalidated identities, preserve explicit corruption/incompatibility errors and typed key distinctions, reject duplicate exact identities.
- Add a plain-Python partial aggregation example after a completed failed run, with one atomic local snapshot containing publication receipt metadata.
- Document prior-run selection, revocation and collection/load consistency, and external sink responsibility.
- Scope: caller-driven collection and application aggregation/publication. Non-goals: directory scanning, automatic past-run inclusion, generalized reducer tasks, changing dependency skip/status semantics, database/history data reconstruction, scheduler, external sink transactions or exactly-once side effects.

## Capabilities

### New Capabilities
- `available-materialization-aggregation`: explicit bounded collection of committed records and deliberate partial publication guidance.

### Modified Capabilities
None. Existing executor, file formats, record/event/CLI/SQLite versions remain unchanged.

## Impact

Additive stdlib-only optional protocol/method; existing adapters need not implement collection. New example demonstrates application-controlled idempotent local snapshot replacement with its receipt in the same file. No automatic execution or sink writes occur in core collection.
