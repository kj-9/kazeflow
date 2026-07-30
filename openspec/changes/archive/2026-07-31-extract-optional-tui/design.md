## Context

M2 emits ordered `ExecutionEvent` values to an optional consumer and defaults to no
presentation.  The repository still has the legacy `tui.py` and `logger.py` modules,
which import Rich and were designed around executor-owned progress identifiers and
`AssetResult`.  M3 advances the roadmap by making rendering an independent consumer
while preserving the M2 plan/result/event contract and its execution behavior.

## Goals / Non-Goals

**Goals:**

- Keep all core imports (`flow`, `assets`, `events`, `plan`, `results`, and package
  core exports) limited to the Python standard library.
- Specify a synchronous observer boundary with a no-op default and an opt-in
  standard-library logging consumer.
- Rebuild Rich presentation as an explicit `ExecutionEventConsumer` that can be
  constructed/imported only by callers that select it.
- Make consumer ownership, consumer-error behavior, and parallel file ownership
  unambiguous.

**Non-Goals:**

- Moving Rich into package extras, editing `pyproject.toml` or `uv.lock`, or removing
  `netext`; those are M4 packaging work.
- Persisting event streams, adding async dispatch, backpressure, event filtering,
  plugin discovery, a daemon, scheduler, remote workers, or a web UI.
- Altering plan validation, concurrency, partition, cancellation, failure, event
  sequence, result ordering, or direct asset-function execution semantics.

## Decisions

### The executor owns synchronous dispatch; consumers own presentation lifecycle

Core execution keeps the existing `event_consumer` parameter and invokes
`on_event(event)` in emission order on the calling execution path.  `None` selects an
internal no-op consumer.  The executor neither imports nor constructs a renderer,
manages a display context, or exposes presentation task identifiers.  A caller creates
and enters a renderer around `run()`/`run_async()`, then passes it as the consumer.
This keeps lifecycle control and terminal ownership with the presentation adapter.

Alternative considered: a `renderer=True` or renderer-name argument on `run`.  It is
rejected because it would make core select/import an optional dependency and couples
the public executor API to a display implementation.

### Consumer failures are caller-visible and do not become execution results

If `on_event` raises during normal execution, the executor stops further dispatch and
propagates that exception.  It does not translate the error into an asset failure,
`FailureInfo`, a synthetic `RunResult`, cancellation state, or terminal events.  This
matches an explicitly supplied observer being application code rather than managed
execution work.  Consumers that require best-effort behavior must catch their own
errors.

Alternative considered: swallowing/logging consumer exceptions.  It is rejected
because silent loss of observability makes progress and audit output untrustworthy and
would require global logging policy in core.

### Rich rendering is an optional, event-only adapter

`tui.py` imports Rich and implements the consumer protocol.  It derives all terminal
display state from flow/task/attempt events and their public metadata; it does not read
executor internals, `Flow.asset_outputs`, raw outputs, raw exceptions, or scheduler
state.  It can render no-work and dependency-blocked task finishes.  `logger.py` is
either removed from the core path or retained only within the TUI adapter; core asset
contexts receive a standard-library logger that remains silent absent caller logging
configuration.

Alternative considered: retaining the legacy renderer and adapting it with callbacks.
It is rejected because its progress IDs and `AssetResult` ownership recreate the
presentation-to-executor coupling that M3 removes.

### Parallel ownership has two waves

Wave 1 runs independently after this change is approved:

- **Core observer owner:** `src/kazeflow/events.py`, the sole core hot-spot owner for
  `src/kazeflow/flow.py` and `src/kazeflow/assets.py`, plus
  `tests/test_observers.py`.  This owner implements no-op/stdlib consumers and
  dispatch/error semantics.
- **TUI owner:** `src/kazeflow/tui.py`, `src/kazeflow/logger.py`, and
  `tests/test_tui.py`.  This owner consumes only the frozen M2 event API and does not
  edit core files.
- **Test owner:** `tests/test_core_imports.py` and isolated environment smoke scripts
  or CI test helpers only.  This owner does not edit packaging metadata.

Wave 2 integrates only after Wave 1 interfaces/tests are reviewed: the core observer
owner resolves any `flow.py` integration and runs the full suite.  `__init__.py`,
`pyproject.toml`, and `uv.lock` remain untouched in M3 unless a later explicitly
approved change assigns their single-owner work.

## Risks / Trade-offs

- [A user imports `kazeflow.tui` without Rich installed after M4] → Document that the
  renderer requires the TUI extra and preserve core-only imports; M4 owns dependency
  metadata and clean-install verification.
- [A renderer exception interrupts a run] → Make propagation explicit, test it, and
  advise renderers to handle their own recoverable failures.
- [Concurrent events produce confusing display order] → Render received sequence order
  only; the existing event contract intentionally permits either observed finish order
  for independent concurrent work.
- [Legacy automatic UI users lose behavior] → Provide migration examples showing an
  explicitly entered/passed renderer; no automatic display is reintroduced.

## Migration Plan

1. Land and test observer dispatch plus a Rich event consumer without changing package
   dependency metadata.
2. Migrate legacy automatic UI call paths to:
   `with FlowTUIRenderer(...) as renderer: result = run(..., event_consumer=renderer)`.
3. Verify core imports and renderer-free runs in an environment where Rich cannot be
   imported, and verify renderer behavior in the current TUI-enabled development
   environment.
4. M4 moves Rich to an optional extra and makes the packaging guarantee release-ready.

Rollback is code-only: callers can omit the consumer and retain the presentation-free
M2 executor.  No data migration or persistent state exists.

## Open Questions

- The precise public constructor naming and whether a plain-text consumer belongs in
  `events.py` or a separate core module will be chosen during implementation, provided
  it remains standard-library-only and satisfies the observer specification.
