## Context

This M0 change is the ROADMAP specification gate. The current executor builds a graph from registered assets, runs it through `TopologicalSorter`, writes mutable outputs on `Flow`, and drives a Rich TUI directly. It returns `None`, uses monotonic time as an apparent start timestamp, treats falsey partition keys as unpartitioned, and stops scheduling after the first observed failure. The scheduler also drops ready work when ready task/partition count exceeds `max_concurrency`: four independent tasks with a limit of two execute only `a` and `b`. Dependency sets make plan order non-deterministic, and repeated `Flow` runs can read outputs retained from a previous run.

The baseline has 11 passing tests. Existing `ty` output contains four unrelated errors (flow partition typing, `DatePartitionDef.range` override, and TUI `TaskID` typing); future M0/M1/M2 verification must distinguish those baseline errors from new regressions. With the current uv release and checked-in lock metadata, `uv run --locked` reports a pre-release-mode mismatch and requests a lock update. That lock-policy mismatch is outside M0; non-mutating baseline verification uses `uv run --frozen pytest -q` and confirms that `uv.lock` is unchanged.

M1 will independently add plan, result, and event data models; M2 will be the single shared-executor integrator; M3 will extract optional presentation. This change deliberately contains no runtime implementation.

## Goals / Non-Goals

**Goals:**

- Establish testable lifecycle, failure, time, partition, output, presentation, sync/async, and compatibility semantics.
- Preserve script-first direct asset functions while making a completed run inspectable through a returned result.
- Keep the core standard-library-only and separate arbitrary Python runtime values from portable record metadata.
- Give parallel roadmap owners a single contract and non-overlapping integration boundary.

**Non-Goals:**

- Implement `FlowPlan`, `RunResult`, events, scheduler fixes, renderers, persistence, package metadata changes, or public export changes here.
- Add `raise_on_failure`, fail-fast policy, retries, a scheduler/daemon, remote workers, control plane, database-backed history, or sandbox.
- Define a serialization format or SQLite schema; M6 owns adapter schema and migration after M1 result models stabilize.

## Decisions

### Complete terminal accounting and branch-isolated failure

Every selected task and partition attempt ends exactly once. `skipped` denotes dependency-blocked or explicit no-partition work and contains a machine-readable reason; a dependency-blocked skip includes `blocked_by`. `cancelled` is reserved for explicit cancellation. A task failure allows independent ready and already-running work to complete, transitively skips dependents, and makes the flow `failed` even if cancellation is subsequently requested.

This chooses useful local partial results over fail-fast. Fail-fast was rejected because it discards independent work and would add a policy surface without a stated use case. No transactional rollback is promised for user side effects.

### Task failures are result data; invalid preflight is exceptional

`run()` and `run_async()` return a finalized `RunResult` for execution failure. Invalid definitions and run configuration raise before any asset starts. M0 explicitly excludes an opt-in task-failure raise flag; it can be proposed later once callers demonstrate the need.

This avoids losing partial outcomes and separates preflight errors, for which there is no execution result, from attempted work. Always raising was rejected because it hides completed independent outcomes.

### Time has human and elapsed representations

Use aware `datetime.now(timezone.utc)` for result start/end timestamps and monotonic clock readings (`time.perf_counter()` or equivalent) for duration differences. Public wall timestamps remain comparable across systems; duration remains correct across clock adjustments.

### Partition presence is never truthiness

`None` represents no partition and is invalid as a supplied partition key. Values such as `0`, `""`, and `False` are actual keys. Equal duplicate keys, including `0` and `False`, are invalid preflight input. Omitted keys for a selected partitioned task raise `ValueError`; explicitly empty keys model intentional zero work, producing `skipped(no_partition_keys)` and `{}` dependency output.

This rejects accidental omission while allowing a caller to intentionally choose no partition work. It avoids the current deadlock/drop behavior and preserves Python key semantics. Partition keys remain arbitrary hashable runtime values, not core-serializable values.

### Runtime output and exception objects are not portable records

The future `RunResult` exposes raw successful outputs, and may retain raw exceptions, only in memory. Its serializable projection excludes both while retaining stable identity, statuses, time, duration, partition presence, and type/message/formatted-traceback failure metadata.

Automatic JSON encoding was rejected because it restricts ordinary Python assets and conflates core execution with persistence. M6 adapters may offer explicit codecs without changing these core semantics.

### Presentation observes execution and defaults to no-op

The no-op observer is a contract fixed by M0, not a claim that the present Rich-coupled implementation already conforms. M3 owns the explicit observer/renderer boundary and M4 owns removal of required presentation dependencies. Their target core default is no-op: no Rich import, terminal UI, printing, global logging configuration, file, or database creation. Future events feed explicit renderer/logging/persistence consumers. Implicit stdlib logging was rejected because configured output is still a side effect and breaks a silent core default.

### A single event-loop boundary avoids nested-loop ambiguity

`run_async()` is the async entry point. `run()` detects a running loop in its calling thread and raises `RuntimeError` before asset execution, directing the caller to await `run_async()`. It neither nests loops nor bridges via a helper thread, since both change cancellation, thread-affinity, and context semantics.

### M2 owns correctness repair; order and per-run state are contracts now

M2 must consume a deterministic `FlowPlan`, drain ready tasks/partitions until every selected attempt has a terminal result despite the concurrency cap, and build fresh run output state each execution. The contract does not mandate a scheduler algorithm, but it makes the current dropped-ready bug, set-derived non-determinism, and stale `Flow.asset_outputs` behavior incompatible with the completed result model.

## Ownership and Integration Boundaries

This change owns only `openspec/changes/define-execution-contracts/**`. Its capability-spec sync and archive are serialized with other OpenSpec changes.

| Wave | Owner | Exclusive files / responsibility |
| --- | --- | --- |
| M1 | Plan owner | `plan.py`, `tests/test_plan.py`; plans without task execution and deterministic order |
| M1 | Result owner | `results.py`, `tests/test_results.py`; statuses, timing, in-memory/record boundary |
| M1 | Event owner | `events.py`, event contract tests; observer-facing lifecycle events |
| M2 | Executor owner | `flow.py`, `assets.py`, `tests/test_execution.py`; sole integration owner, concurrency and rerun repairs |
| M3 | TUI owner | `tui.py`, `logger.py`, `tests/test_tui.py`; explicit optional Rich consumer |
| M3 | Core observer owner | no-op/plain/log observer implementation and tests |
| M3 | Packaging-test owner | clean-install and optional-extra smoke tests |
| M4 | Packaging owner | `pyproject.toml`, `uv.lock`, CI; sole hotspot owner |

`src/kazeflow/flow.py`, `src/kazeflow/assets.py`, and `src/kazeflow/__init__.py` are hotspots and shall not have concurrent editors. M1 types must be reviewed and fixed before the M2 executor owner integrates them.

## Risks / Trade-offs

- [Independent work can cause side effects after another branch fails] → return complete terminal results and do not claim rollback or transactionality.
- [In-memory arbitrary outputs can retain memory] → exclude them from portable records; add retention/codecs only in separately specified work.
- [Falsey/non-serializable partition keys complicate storage] → preserve runtime semantics and require adapters to choose encoding.
- [Automatic Rich removal surprises callers] → stage migration through M2/M3/M4 with explicit renderer guidance.
- [Cancellation cannot stop arbitrary synchronous user code] → classify terminal outcomes at the executor boundary and do not promise forced interruption.
- [Baseline `ty` errors obscure regressions] → record the four known errors and require new work to add none before baseline repair is separately owned.

## Migration Plan

1. Review and archive this contract without runtime change.
2. Implement M1 neutral models and tests without touching executor hotspots.
3. Assign one M2 executor owner to return `RunResult`, validate partition input before work, use fresh per-run state, drain all ready work under concurrency limits, and add completion tests.
4. In M3, make Rich an explicit optional event consumer while publishing migration guidance for callers that relied on automatic UI or `Flow.asset_outputs`; this is when the no-op observer contract begins to be implemented.
5. In M4 remove required presentation dependencies and validate core-only installation; this is when the zero-dependency packaging target is verified.

Before M4, rollback is an ordinary code-release rollback; M0 creates neither runtime data nor a persistent schema. M6 owns any later record-schema rollback policy.

## Open Questions

- Exact public field names, immutable container shapes, and exception class names for results, failures, skip reasons, and `blocked_by` belong to M1/M2.
- The cancellation API itself is not selected; M2 must meet the statuses contract without promising interruption of arbitrary synchronous user code.
- The exposure shape for results that contain non-serializable or equality-colliding partition keys belongs to M1; it must not silently collapse keys.
