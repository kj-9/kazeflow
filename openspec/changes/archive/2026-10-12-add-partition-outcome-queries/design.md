## Context

Issue #1 suggests result.failed_partition_keys("prepare_item"). Live AttemptReference values already retain raw keys; record projections intentionally omit them. See proposal for the prioritized scope.

## Goals / Non-Goals

Add two small synchronous inspection methods to RunResult. Keep execution, fields and record/event versions unchanged. Do not add statistics, generalized status filtering, automatic resume or storage reconstruction.

## Decisions

- Public signatures: `failed_partition_keys(self, task_name: str) -> tuple[object, ...]` and `circuit_skipped_partition_keys(self, task_name: str) -> tuple[object, ...]`.
- Shared private RunResult lookup `_partitioned_task(task_name)` validates str (TypeError otherwise), exact-name lookup (KeyError(name) including empty), then is_partitioned (ValueError with task name). Linear lookup is appropriate for small immutable tuples and preserves existing construction/API.
- Build tuples by filtering task.attempts using enum identities. Failed uses terminal FAILED only. Circuit uses terminal SKIPPED plus RESOURCE_CIRCUIT_OPEN only. Query each attempt even when aggregate status is failed or reason is mixed/None. Do not inspect invocation failures, blocker keys or another task's outcomes.
- Return original attempt.attempt.partition_key objects, preserving attempt order. Never use bool(key), a set, sorting or serialization. Snapshot immutability does not deep-copy arbitrary key objects; returned raw objects remain application-owned.
- No top-level export or new result fields. Existing v1/v2/v3 projections and SQLite rows remain identical. Installed core smoke must execute actual partitioned flow and inspect both methods.
- Root owns results.py, OpenSpec artifacts, roadmap, final review/package checks and commit. Tests owner owns new tests/test_partition_outcome_queries.py. Docs owner owns docs/user/api/plans-results.md, docs/user/partitions.md, docs/release-notes.md, new examples/partition_outcomes.py and scripts/smoke_wheel_install.py. No overlapping edits.

## Risks / Trade-offs

- Users may confuse invocation failure with logical failure → document recovered retry exclusion and test it.
- Portable records lose keys → document live-result-only methods; no guessed key reconstruction.
- A returned tuple is immutable but opaque key objects need not be → preserve original references, document application ownership.

## Migration Plan

Additive Python methods only; no record/event/CLI/schema/DB version change. Keep stdlib-only runtime and existing Python 3.10–3.13 CI support.
