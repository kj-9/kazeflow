## Context

See proposal.md. Logical attempts currently identify one selected key, while ResourcePolicy controls run-local body admission. Storage record-version and database-version fields currently both use 1; they must now be independent.

## Goals / Non-Goals

**Goals:** opt-in bounded retry decisions, shared cooldown, numbered body operations without multiplying logical partition results.
**Non-Goals:** implicit retry, HTTP parsing, circuit actions, jitter/backoff generation, cross-run coordination, portable materialization provenance.

## Decisions

### Frozen public contracts

- resources.py: frozen `Fail()` and `Retry(after_seconds: float = 0.0, cooldown_resource: bool = False)`, `FailureAction = Fail | Retry`, `ResourcePolicy(max_concurrency, rate_limit=None, max_attempts: int = 1, classify_failure: Callable[[Exception], FailureAction] | None = None)`. Positive non-bool max_attempts; finite non-negative non-bool seconds; exact bool cooldown. Default no classifier means Fail regardless of max_attempts. Classifiers are synchronous, fast, explicit application code; invalid action/awaitable/exception terminally fails with diagnostic metadata. Actions and policies are defensively revalidated.
- ResourcePlan appends `max_attempts: int = 1`, `classifier_enabled: bool = False`; no callable/code projection. AssetContext appends `invocation_number: int = 1` preserving positional compatibility.
- results.py: frozen slotted `InvocationResult(number: int, status: AttemptStatus, started_at: datetime, ended_at: datetime, duration: timedelta, failure: FailureInfo | None = None, retry_delay_seconds: float | None = None, cooldown_seconds: float | None = None)`. Status success/failed only; failure required iff failed; positive contiguous one-based numbers; finite nonnegative nullable delays only for failed status. `AttemptResult.invocations: tuple[InvocationResult,...] = ()` appended. Constructors reject noncontiguous history; success cannot precede another invocation; proceeding to another invocation requires failed prior entry with non-null retry_delay_seconds. For nonempty histories, logical success/failed status must match the final invocation and no final retry delay may remain scheduled. The same check applies to complete event streams; manually constructed cancelled outcomes remain allowed. No need to require nonempty history for manually constructed/legacy results. Commit failure can make the last numbered operation failed; classifier failure can make the logical failure differ from history's original body failure.
- InvocationResult.to_record() exact keys `number,status,started_at,ended_at,duration_seconds,failure,retry_delay_seconds,cooldown_seconds`. Existing AttemptResult/TaskResult/RunResult.to_record(*, schema_version: int = 1) retain v1 shapes; explicit v2 adds `invocations` per attempt. No top-level record-version field (CLI/SQLite envelopes carry it). Unknown and boolean versions rejected.
- events.py: EventKind adds INVOCATION_STARTED and INVOCATION_FINISHED. ExecutionEvent appends invocation_number, retry_delay_seconds, cooldown_seconds default None. Invocation events use attempt identity, no skip fields; start running, finish success/failed, failure iff failed; nullable finite delay fields only on failed finish. Existing lifecycle events forbid invocation fields. to_record(*, schema_version=2) adds `schema_version:2` and the 3 invocation fields; explicit v1 preserves old shape for old kinds and rejects new kinds. Complete validator tracks numbered start/finish pairs per logical attempt and causal continuity; old streams still validate. TUI ignores new events, preserving logical progress.
- Retry-enabled means max_attempts > 1 OR classifier is not None. Only enabled resource operations emit numbered events and retain invocation history; default policies/ordinary assets retain previous event-kind sequences and empty history. Materialization hits/preparation failures have no body history. Numbered operations include body and optional commit, as both hold the existing resource slot; preparation is excluded.

### Executor integration (root only after model freeze)

Root owns resources/plan/assets/__init__/flow. Private body-exception wrapper distinguishes asset exceptions from adapter failure. Each enabled body scheduling increments number, updates context, records timing, emits invocation_started outside the task future so observer failures propagate. finish releases resource/global capacity, classifies only wrapped body errors, builds original failure info, records invocation outcome and emits invocation_finished outside future handling. Callback faults retain original body failure in history and report callback error on terminal logical result. Adapter commit errors record a failed numbered operation with no retry decision. Preparation failures remain unnumbered.

A retry decision with available budget sets an attempt ready deadline and retains prepared materialization identity/inputs; no terminal AttemptResult or attempt_finished yet. Exhausted decisions never schedule another body and keep retry_delay_seconds null; cooldown_seconds still records requested shared delay. Resource cooldown extends max(existing_deadline, monotonic_now + requested_seconds), even if the requesting attempt exhausted its own budget. Already admitted/running work is allowed to finish. Pending materialization preparation bypasses body resource wait, allowing cache hits during cooldown. Admission must satisfy own ready deadline plus resource cooldown, rate and concurrency; earliest actionable wait deadline uses their maximum, and across independent candidates uses minimum. Waits occupy no running future, so unrelated work progresses. Cancellation propagates with no synthesized result; run-local state discarded.

### Portable projections and storage

CLI plan uses v4 (fields max_attempts,classifier_enabled per resource); run-result/list/show/compare use v2. Current run-result record_schema_version=2. Stored show/compare accept both record versions and store_schema_version=1. v4 schemas and golden docs coexist with unchanged historical v1/v2/v3 artifacts; v4/schema.json can define all current document types with per-type versions. SQLite CURRENT_SCHEMA_VERSION stays1; add CURRENT_RECORD_SCHEMA_VERSION=2; save uses result.to_record(schema_version=2) and row schema_version2. StoredRunSummary.schema_version remains per-record version as existing CLI expects; decoder receives this envelope version and rejects unknown versions. No DDL migration or rewriting v1 rows.

### Ownership

- Root: resources.py, plan.py, assets.py, __init__.py, flow.py, OpenSpec, roadmap, final review.
- Model/storage agent: results.py, events.py, sqlite_store.py and tests/test_results.py, test_events.py, test_sqlite_store.py, test_sqlite_store_boundaries.py, test_sqlite_store_migrations.py; freeze contracts first and notify root before executor edits.
- Projection/Docs agent: cli.py, Docs schemas/goldens, test_cli.py, test_cli_json_contracts.py, test_documentation_contracts.py, Docs reference/guides/release notes and scripts/smoke_wheel_install.py.
- Retry tests agent: tests/test_resource_retry.py, tests/test_public_api.py, examples/resource_retry.py. No shared executor edits.

## Risks / Trade-offs

- Retried application side effects may repeat → explicit classifier/budget, document idempotency responsibility.
- Synchronous classifiers block event loop if slow → fast callbacks documented; no async classifier in scope.
- Consumer failure must not become retryable asset failure → dispatcher operations outside task-body exception handling.
- Cancellation of a synchronous asset thread cannot stop physical work → retain current cancellation semantics.
- Failure metadata may contain sensitive application values → existing lossy privacy boundary remains; no redaction promise.

## Migration Plan

Existing resource constructors and default result projections stay compatible. Consumers adopt CLI plan4 and run/history2, explicit result record2, and event record2. Old events can be projected as v1; historical schemas remain published. Existing SQLite v1 rows read unchanged; database layout stays1. No commit/push/Issue writes in this implementation task.
