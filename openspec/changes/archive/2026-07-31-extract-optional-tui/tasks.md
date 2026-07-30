## 1. Freeze the observer boundary

- [x] 1.1 **Core observer owner** — Own `src/kazeflow/events.py` and
  `tests/test_observers.py`; implement the standard-library no-op and any selectable
  plain-text/logging consumer, and test ordered synchronous dispatch plus propagation
  of consumer errors without converting them into asset failures.
- [x] 1.2 **Core observer owner (serial hot-spot)** — Solely own
  `src/kazeflow/flow.py` and `src/kazeflow/assets.py`; remove remaining presentation
  coupling, wire the observer boundary into execution, and preserve all M2 plan,
  result, event, failure, partition, concurrency, and cancellation behavior.

## 2. Implement optional presentation in parallel

- [x] 2.1 **TUI owner, parallel with 1.1** — Own `src/kazeflow/tui.py`,
  `src/kazeflow/logger.py`, and `tests/test_tui.py`; replace legacy executor-coupled
  Rich behavior with an explicitly constructed `ExecutionEventConsumer` and test
  lifecycle rendering from neutral event values only.
- [x] 2.2 **Core-import test owner, parallel with 1.1 and 2.1** — Own only
  `tests/test_core_imports.py` and any isolated smoke-test helper it needs; verify
  renderer-free plan/run/result access and that importing core modules cannot import
  Rich. Do not edit core source, TUI source, `__init__.py`, packaging metadata, or lock
  files.

## 3. Integrate and verify serially

- [x] 3.1 **Core observer owner** — After 1.1, 2.1, and 2.2 are reviewed, reconcile
  the sole `flow.py`/`assets.py` integration point and add equivalence coverage proving
  rendered and renderer-free runs keep identical result semantics.
- [x] 3.2 Run targeted observer, TUI, core-import, execution, partition, and event
  tests with `uv run pytest`; run `make test` and `make ci-check`; report the exact
  commands and any environment limitation for a no-Rich smoke test.
- [x] 3.3 Run `openspec doctor`, `openspec validate extract-optional-tui --strict`,
  and OpenSpec change verification; confirm `pyproject.toml` and `uv.lock` remain
  untouched because M4 owns optional-extra packaging.

## 4. Document and close the change serially

- [x] 4.1 Update user-facing migration guidance for callers that previously expected
  automatic Rich display, without making Rich a core requirement.
- [x] 4.2 Mark completed tasks, sync only approved delta specs to living specs, and
  archive `extract-optional-tui` after implementation and verification are complete.
