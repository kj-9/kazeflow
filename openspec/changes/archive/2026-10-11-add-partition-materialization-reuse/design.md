## Context

See `proposal.md` and the four delta specs. Today `FlowPlan` fixes selected keys and dependency order, while the executor invokes asset bodies and passes their in-memory outputs downstream. `RunResult` records outcomes but does not identify reusable outputs. The separate Issue #1 resource-concurrency change is active and touches the same executor and public-API hotspots; its edits must not be interleaved with this implementation.

## Goals / Non-Goals

**Goals:** Keep the default execution path unchanged; make reuse a per-asset opt-in; validate identity in core without owning storage; preserve the existing portable run-result and CLI schemas.

**Non-Goals:** An implicit cache, a bundled storage adapter, an exactly-once guarantee across runs, depth-first key scheduling, or partial aggregation.

## Decisions

### 1. Explicit adapter contract and identity

Add a standard-library-only `Materialization` protocol in a dedicated module with `resolve(context)`, `load(record)`, and `commit(context, output)` operations. `@asset` / registry registration accepts an optional adapter plus an explicit, non-empty version. A frozen context carries asset name, version, partition domain, a separate key-presence flag and normalized key, and ordered `(dependency_name, committed_materialization_id)` pairs for direct dependencies. A frozen record carries a non-empty opaque ID, the corresponding identity fields, and an explicit committed flag. `resolve` returns a record or `None`; `commit` returns a committed record. Adapter operations may be synchronous or awaitable, using the existing invocation normalization pattern. No output serializer is prescribed.

Core compares every record identity field to the context before `load` or downstream success. A malformed, mismatched, or uncommitted resolved record is a miss, not a reusable hit; malformed or uncommitted commit results are failures. The adapter must ensure returned committed records refer to durable, atomically published outputs. Core cannot prove durability or prevent a record changing between resolve and load.

Alternative considered: core-owned content hashes, pickle, or a global filesystem manifest. Rejected because arbitrary Python objects and function source are not stable, and storage policy belongs to the application.

### 2. Dependency eligibility and planning

A materialized asset can depend only on materialized partitioned assets selected in the same domain; otherwise planning raises `ValueError` before run events. Root materialized assets have an empty input-ID tuple. `TaskPlan` adds a default-false participation flag. Planning checks declaration completeness and selected dependency shape, but never contacts the adapter. A plan cannot promise a hit.

Alternative considered: treating ordinary dependency outputs as reusable inputs. Rejected because they lack a stable committed identity and would permit stale downstream reuse.

### 3. Executor boundary

For each eligible attempt, obtain committed same-key dependency IDs, call `resolve`, validate a candidate, and call `load` on a hit. On a miss, invoke the ordinary asset body once, then `commit`, validate the returned record, and only then publish output to dependency scheduling. A resolve or load exception fails the attempt; no silent fallback invocation. Failed commit keeps output private and blocks same-key dependents. Existing ready-queue concurrency, independent-branch continuation, terminal-result creation, and plan-order result assembly remain intact. External cancellation follows current semantics: no fabricated terminal result; an adapter's completed atomic commit may be found by a subsequent run.

Alternative considered: retrying body on load failure or pushing commit to background. Rejected because either hides an integrity problem or lets uncommitted output reach downstream work.

### 4. Result and compatibility boundary

`AttemptResult` gains in-memory `resolution` and `materialization_id` fields. A successful ordinary or newly executed materialized attempt has `resolution="executed"`; a loaded attempt has `resolution="materialized"`. Only successful attempts with a validated committed record have an ID. Failures and skips have neither. Keep `to_record()` output, CLI JSON versions, SQLite tables, and event shapes unchanged; portable provenance needs a separate versioned change. The new fields have defaults to preserve existing direct constructors.

Alternative considered: extending current JSON/SQLite records now. Rejected because it creates a schema migration independent of the minimal reuse contract and overlaps Issue #1's active CLI changes.

### 5. Module ownership and implementation sequencing

One owner implements public declaration and exports (`assets.py`, `__init__.py`) and one executor owner handles `flow.py` in a serial integration wave after the active Issue #1 change is isolated. A separate owner can build `materialization.py` and focused tests while another handles documentation and example protocol usage. `plan.py` and `results.py` should be integrated with the executor owner or in separate sequential waves; no two agents edit a hotspot in the same wave. No agent commits or pushes unless requested.

## Risks / Trade-offs

- [Adapter lies about commit or identity] → Core validates record fields, documents the trust boundary, and tests a deliberately invalid adapter; durable atomic publication remains the adapter's obligation.
- [Partition key is not safely serializable] → Pass normalized raw key and presence flag in memory; the adapter chooses encoding and rejects unsupported keys. Never use truthiness to detect a key.
- [Concurrent runs write the same identity] → Document no cross-run exactly-once promise; applications select adapters with suitable compare-and-swap or idempotent commit behavior.
- [Source changes without version bump] → Require explicit version and document that users must bump it when semantics change.
- [Dirty Issue #1 changes obscure review] → Finish or isolate that change before source integration; keep this change's planning artifacts separate.

## Migration Plan

1. Keep this change in proposal state while Issue #1 remains active in the shared working tree.
2. Implement protocol and tests, then declaration/planning, then executor/result integration after the working-tree conflict is resolved.
3. Run focused partition/failure/cancellation tests, full `uv run pytest`, `make ci-check`, package smoke tests for public API changes, and strict OpenSpec validation.
4. Roll back by removing the opt-in declaration path; flows without declarations retain their existing behavior and no portable schema migration is needed.
