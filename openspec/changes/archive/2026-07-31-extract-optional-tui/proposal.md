## Why

M2 made execution observable through neutral events, but the repository still ships a
Rich-bound presentation implementation alongside core modules.  M3 must make the
presentation boundary usable in practice so core callers can run and inspect flows
without importing Rich, while callers who want terminal progress can explicitly opt in.

## What Changes

- Add an optional Rich terminal renderer that consumes `ExecutionEvent` values and is
  selected explicitly by the caller.
- Define the core observer/consumer boundary, including no-op behavior, standard-library
  logging support, synchronous dispatch, and the policy for consumer failures.
- Remove Rich-coupled presentation and logging behavior from the core execution path
  without changing plan, scheduling, event, result, partition, or failure semantics.
- Establish core-only and TUI-enabled smoke-test coverage; package metadata changes that
  make Rich an optional extra are deferred to M4.

## Capabilities

### New Capabilities
- `optional-tui-rendering`: Explicit, event-driven Rich rendering that is separate from
  core execution.

### Modified Capabilities
- `execution-events`: Defines event-consumer dispatch and failure behavior at the core
  observer boundary.
- `core-executor-integration`: Defines accepted observer selection and preserves
  presentation-free executor semantics.

## Impact

This advances roadmap M3, **Separate presentation from execution**.  It affects
`src/kazeflow/flow.py`, `assets.py`, `tui.py`, and `logger.py`, with dedicated core and
TUI tests.  The public execution entry points retain their `event_consumer` capability;
the optional renderer adds an explicit presentation API.  No required runtime
dependency is added, and this change must not import Rich from a core module; converting
Rich to a package extra and removing `netext` are M4 work.
