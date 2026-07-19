## 1. Serial executor integration ownership

- [x] 1.1 Assign one executor integration owner exclusive write ownership of `src/kazeflow/flow.py`, `src/kazeflow/assets.py`, any required existing-entry-point export in `src/kazeflow/__init__.py`, `tests/test_execution.py`, and any necessary targeted changes to `tests/test_flow.py`; do not concurrently edit M1 model modules/tests, TUI/logger, package files, or this change's sync/archive state.
- [x] 1.2 Implement `Flow(targets, *, registry=...)` as the owner of direct canonical targets and its exact registry; implement the module `run(..., *, registry=...)` handoff and a checked, deprecated `Flow(graph)` compatibility shim that rejects graph/registry-plan mismatches before events or assets.
- [x] 1.3 Replace the legacy graph-only manager path with a per-invocation execution state that consumes `FlowPlan`, normalizes existing `RunConfig` into `PlanConfig`, exposes side-effect-free `Flow.plan()`, and preflights every invalid definition/configuration before events or asset scheduling.
- [x] 1.4 Make module `run()` and `Flow.run_async()` return terminal `RunResult` only after normal completion; add the active-event-loop guard to module `run()` before planning or execution, and preserve the existing decorator, inferred dependencies, direct asset calls, and ignored-return call sites.

## 2. Deterministic execution and result construction

- [x] 2.1 Implement a deterministic ready-work drain that respects `max_concurrency`, continues until every planned task/attempt is terminal, and constructs task/attempt tuples in `FlowPlan` and selected-key order.
- [x] 2.2 Implement per-run output isolation and result construction using aware UTC lifecycle timestamps plus monotonic durations; convert asset exceptions to `FailureInfo` and failed attempts without raising task failures from a completed run.
- [x] 2.3 Implement dependency eligibility and exact input resolution: raw scalar for an unpartitioned upstream; one-entry `{current_key: output}` maps for each partitioned upstream of a partitioned downstream; ordered full maps only for successful non-partitioned reducers; dependency blocking rather than partial maps; and `{}` after `no_partition_keys`.
- [x] 2.4 Implement presence-based partition handling for falsey keys and explicitly empty selections; ensure reruns never read a prior invocation's outputs and `Flow.asset_outputs` is not authoritative.
- [x] 2.5 Handle external `run_async()` cancellation separately from asset failure: stop pending scheduling, request cancellation of executor-created asyncio tasks, discard outcomes from already-submitted synchronous thread work, re-raise `CancelledError`, and emit no synthetic terminal result/events.

## 3. Passive observability and presentation removal

- [x] 3.1 Add optional synchronous `ExecutionEventConsumer` dispatch with a no-op default; emit and internally validate one complete, consecutively sequenced event stream per completed run, including no-work, blocked, failure, and flow-final events.
- [x] 3.2 Remove automatic flow-tree/Rich progress/renderer/logger coupling from the core execution path and provide a silent standard-library context logger without global logging configuration.

## 4. Executor integration tests

- [x] 4.1 Add or update executor tests proving direct target/registry ownership, checked legacy `Flow(graph)` compatibility, `Flow.plan()` side-effect freedom, `run()`/`run_async()` `RunResult` returns, preflight-before-assets/events behavior, and synchronous active-loop rejection.
- [x] 4.2 Add completion tests for more ready tasks and more partitions than the concurrency bound, proving at-most-bound concurrency, exactly-once invocation, plan/key result order, and terminal coverage.
- [x] 4.3 Add failure and partition tests for branch continuation, dependency-blocked task/attempt blockers, matching-key-only blocking, one-entry matching-key maps, failed sibling isolation, multiple partitioned dependency maps, reducer full-map/partial-output rejection, falsey keys, empty selection reducer input, and fresh rerun state.
- [x] 4.4 Add event tests proving no-op default presentation behavior and a supplied consumer's complete valid lifecycle stream, including failed attempts and terminal flow status.
- [x] 4.5 Add external-cancellation tests proving coroutine cancellation propagation, no pending-attempt start, no synthetic terminal result/event, and discarded output from a cancelled synchronous executor wrapper.

## 5. Serial verification and handoff

- [x] 5.1 Run targeted executor tests and `uv run pytest`; record any intentionally migrated assertions that formerly read `Flow.asset_outputs`.
- [x] 5.2 Run `make ci-check`, `openspec doctor`, and `openspec validate --all --strict`; fix only the executor-owned implementation/test files for failures attributable to this change.
- [x] 5.3 Handoff the completed implementation and verification evidence for serial review; do not sync/archive this change, alter package metadata/dependencies, commit, push, or create a PR as part of M2 execution.
