## Why

Issue #2 identifies an expensive recovery gap: after one partitioned asset succeeds and a downstream step fails, a later run must repeat the completed work or infer its state from application files. Run history records outcomes, not reusable asset outputs. This change establishes an explicit, optional reuse boundary for one asset and partition at a time.

This is post-M17 issue-driven work, separate from documentation milestone M18 and from Issue #1's resource policy. The current uncommitted resource-concurrency change remains independent and active.

## What Changes

- Let a partitioned asset opt into a user-supplied materialization adapter and explicit asset version.
- Before invocation, ask the adapter whether a committed result is reusable; load it if valid, otherwise invoke the ordinary Python asset and commit its output before making that output available downstream.
- Give the adapter normalized partition identity and upstream committed materialization identities. Do not reuse when a dependency lacks a stable committed identity; do not infer versions by hashing Python functions or silently pickle arbitrary outputs.
- Surface `executed` versus `materialized` in the in-memory attempt result and expose materialization participation in the structured plan. Retain existing portable run-record, CLI JSON, and SQLite schemas in this first slice; their versioned extensions belong to a follow-up change.
- Leave flows without materialization on their current execution path, with no required third-party runtime dependency or storage service.

This slice does not add depth-first partition grouping or partial aggregation, the other two contracts identified in Issue #2's follow-up. It does not provide a built-in filesystem/object-store adapter, external sink receipts, implicit resume, or safety guarantees for untrusted Python. Those require separate changes and review.

## Capabilities

### New Capabilities

- `partition-materialization-reuse`: Explicit adapter resolution, committed-output reuse, and commit-before-downstream semantics for selected partitioned attempts.

### Modified Capabilities

- `flow-planning`: Expose whether a selected task participates in materialization without inspecting storage during planning.
- `core-executor-integration`: Resolve or execute an eligible partitioned attempt while preserving dependency, failure, and cancellation semantics.
- `run-results`: Distinguish in-memory executed and materialized successful attempts without changing the current portable record schema.

## Impact

The public Python API gains an optional asset materialization declaration and adapter protocol. Planning, executor, and in-memory attempt values change; existing result record fields and CLI document versions do not. Storage, serialization, partition-key encoding, integrity checks, and atomic commits are responsibilities of the explicitly selected adapter, documented as a trust boundary. Core remains standard-library-only and infrastructure-free; no implicit persistence or global cache is introduced.
