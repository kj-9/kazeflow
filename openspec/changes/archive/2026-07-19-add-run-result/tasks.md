## 1. Use the frozen M1 model seam

- [x] 1.1 Treat the exact names, enum values, dataclass signatures/defaults,
  constructors, record schemas, and ordering rules in `design.md` and both specs as
  the accepted Wave 1 interface. Do not rename, add fields to, or reinterpret those
  values during implementation. Owner: Result and Event owners. Files: no edits.
- [x] 1.2 Keep M2 as the sole future integration owner: no Wave 1 task edits
  `src/kazeflow/flow.py`, `src/kazeflow/assets.py`, `src/kazeflow/__init__.py`,
  package metadata, or existing flow tests. Owner: M1 model coordinator. Files: no
  runtime files.

## 2. Implement immutable result models

- [x] 2.1 Implement exactly `FlowStatus`, `AttemptStatus`, `SkipReason`,
  `TaskReference`, `AttemptReference`, and `FailureInfo`, including their specified
  enum values, fields, defaults, and validation in `src/kazeflow/results.py`. Owner:
  Result owner. Files: `src/kazeflow/results.py` only; it MUST NOT import events.
- [x] 2.2 Implement exactly `AttemptResult`, `TaskResult`, and `RunResult`, including
  terminal/timing/failure/skip/blocker/partition aggregation invariants and the
  normative `FlowPlan.tasks`/selected-key tuple order preservation. Owner: Result
  owner. Files: `src/kazeflow/results.py` only.
- [x] 2.3 Implement the exact lossy `to_record()` keys, nesting, nulls, omissions,
  and ordered arrays for every result value. Owner: Result owner. Files:
  `src/kazeflow/results.py` only.
- [x] 2.4 Add unit tests for signatures/defaults, immutable envelopes,
  status/timing/failure/blocker invariants, non-serializable outputs, falsey present
  partition keys, no-work aggregation, and record key/order/omission behavior. Owner:
  Result owner. Files: `tests/test_results.py` only.
- [x] 2.5 Run `uv run pytest tests/test_results.py` and the applicable formatter/lint
  checks for the result-owned files. Owner: Result owner. Files: no edits.

## 3. Implement neutral event models

- [x] 3.1 Implement exactly `EventKind`, `ExecutionEvent`,
  `ExecutionEventConsumer.on_event`, and `validate_event_sequence`, including the
  specified fields, defaults, kind payload rules, and complete-stream ordering
  validation. Owner: Event owner. Files: `src/kazeflow/events.py` only.
- [x] 3.2 Enforce the one-way import boundary in `src/kazeflow/events.py`: import
  only `AttemptReference`, `AttemptStatus`, `FailureInfo`, `FlowStatus`,
  `SkipReason`, and `TaskReference` from `results.py`; never import or carry result
  objects, raw output, or raw exceptions. Owner: Event owner. Files:
  `src/kazeflow/events.py` only.
- [x] 3.3 Implement the exact event `to_record()` keys, nesting, nulls, omissions,
  and ordered blocker projection. Owner: Event owner. Files:
  `src/kazeflow/events.py` only.
- [x] 3.4 Add unit tests for signatures/defaults, immutable event values, payload
  exclusion, UTC/sequence validation, `TaskReference` no-work and blocked-task
  events, kind payload rules, record schema, and valid/invalid causal streams. Owner:
  Event owner. Files: `tests/test_events.py` only.
- [x] 3.5 Run `uv run pytest tests/test_events.py` and the applicable formatter/lint
  checks for the event-owned files. Owner: Event owner. Files: no edits.

## 4. Verify the isolated M1 change

- [x] 4.1 Review the completed Wave 1 import graph, public signatures, both fixed
  record projections, and result/event ordering tests; verify `results.py` has no
  events dependency and `events.py` contains no result object payload. Owner: M1
  model coordinator. Files: no edits.
- [x] 4.2 Run `uv run pytest` (or `make test`) and `make ci-check`; distinguish the
  documented pre-existing type-check findings from regressions introduced by these
  four owned files. Owner: M1 model coordinator. Files: no edits.
- [x] 4.3 Run `openspec doctor` and `openspec validate --all --strict`; verify this
  change without syncing or archiving it. Owner: M1 model coordinator. Files: no
  edits.
- [x] 4.4 Confirm no package metadata changed, so wheel build and clean-install checks
  are not applicable to this model-only change; hand the frozen model seam to the
  single M2 executor integration owner. Owner: M1 model coordinator. Files: no edits.
