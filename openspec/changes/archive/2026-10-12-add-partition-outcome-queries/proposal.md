## Why

Issue #1's final planned result-query follow-up needs a reliable way to select failed and circuit-skipped partition keys without manual traversal. Terminal logical results already contain the necessary in-memory keys and outcomes.

## What Changes

- Add `RunResult.failed_partition_keys(task_name)` and `RunResult.circuit_skipped_partition_keys(task_name)` returning ordered tuples of original keys.
- Define falsey keys, empty selections, terminal status filtering and explicit unknown/unpartitioned task errors.
- Document how selected keys can be passed explicitly to a new run, with no automatic restart.
- Scope: in-memory result helpers, evidence, examples and Docs. Non-goals: generic status queries, resource statistics, persisted key reconstruction, CLI commands, execution changes and automatic resume.

## Capabilities

### New Capabilities
- `partition-outcome-queries`: ordered in-memory key queries by terminal partition outcome with diagnosed task lookup.

### Modified Capabilities
None. Existing record/event/CLI/storage shapes and versions remain unchanged.

## Impact

Additive methods on the existing stdlib-only RunResult. No runtime dependencies, data fields or executor changes. Raw keys are returned only from the live Python result; lossy portable records cannot supply these queries.
