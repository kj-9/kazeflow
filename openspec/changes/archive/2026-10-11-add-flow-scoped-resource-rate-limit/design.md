## Context

See proposal.md. The current executor launches eligible attempts into global/resource slots and wraps materialization resolve, body, and commit in one future. Rate waiting inside that wrapper would consume slots. This change therefore separates materialization preparation from body admission. Existing archived changes remain uncommitted in the shared tree and must be preserved.

## Goals / Non-Goals

**Goals:** deterministic shared admission, zero busy polling, side-effect-free plan, compatible materialization reuse, and unchanged portable result/event shapes.

**Non-Goals:** actual HTTP request measurement, burst allowances beyond the count/window contract, retry, circuit state, persistent limits, and cross-process quotas.

## Decisions

### Public declaration and time window

Add frozen `RateLimit(requests: int, per_seconds: float)` and `ResourcePolicy(max_concurrency: int, rate_limit: RateLimit | None = None)`. Validate bool, non-positive values, non-finite durations, and defensive malformed objects during planning. ResourcePlan captures immutable rate data. Use a per-resource deque of monotonic start timestamps and expire entries `<= now - per_seconds`; admission is available while the remaining count is below requests. No wall-clock or persistent timestamps are used.

Choose a strict sliding window because the observable bound can be tested for any interval. Fixed windows allow boundary bursts; token buckets allow burst/refill semantics that require a different contract. Failures do not refund a start. A permit is consumed only immediately before body launch, after capacity is available; prepared/blocked/cancelled work consumes none. Body wrappers record their monotonic launch time before calling Asset.invoke; ordering and launch reservations remain executor-owned to avoid races.

Track pending launch reservations separately from timestamped starts. Availability counts both until a scheduled wrapper enters Asset.invoke and replaces its reservation with the actual monotonic timestamp. A run-local wakeup future notifies the drain loop at this transition so a long running body does not hide the next deadline. This avoids stamping a permit before event-loop scheduling delays. Sync functions are still dispatched through the existing thread invocation path; this is a bound on executor invocation starts, not physical HTTP calls inside threads.

### Executor phases and ownership of capacity

Introduce run-local attempt phases: pending preparation, preparing, waiting body admission, invoking/committing, terminal. Ordinary assets become ready for body admission directly. Materialized assets resolve/load in a preparation future; a hit produces the existing successful outcome, and a miss retains context/inputs for a later admitted body future. Preparation is bounded by the global running-future limit but holds no body resource slot or rate permit. Waiting after a miss has no running future and holds no slot. Body/commit keeps its resource concurrency reservation until completion, a conservative bound compatible with current behavior.

Emit ATTEMPT_STARTED once when an attempt first enters active preparation or body work. Re-entering a phase emits no duplicate lifecycle event. Preserve started timing across waits, terminal result ordering, committed input IDs, and compatibility outputs. Adapter exceptions fail the attempt once. External cancellation cancels active phase futures and the deadline wait; synchronous thread operations retain the existing cancellation caveat.

Extend `_drain` to wait for either running-future completion or the earliest rate deadline. If no future is running, await the earliest deadline directly. If neither progress nor a deadline exists for unfinished valid work, retain the invariant error. Re-evaluate ready work after every completion/deadline and use plan/selection order to break ties. No scheduled sleeper per pending partition and no permanent background task are required.

Alternative: acquiring a limiter inside each running body wrapper would keep global slots occupied and violate the independent-progress requirement. Reserving permits before materialization resolve would charge hits and adapter failures without body execution.

### Testability and compatibility

Keep monotonic/window arithmetic in a small internal helper with injectable time for deterministic boundary tests. Executor integration uses event-controlled tests and small bounded real intervals only where event-loop behavior must be exercised. Test exact deadline expiry, multi-asset sharing, saturated global/resource bounds, independent work, no-running-future waiting, falsey keys, failure, cancellation, and hit/miss materialization.

Emit plan JSON v3 with `rate_limit: null | {requests, per_seconds}` in each resource. Retain v1/v2 schema/goldens, and change no other document or run-record version. Existing Python call sites remain valid; rollback removes only opt-in limits and restores plan v2 emission without stored-data migration.

### Module and file ownership

Root owns public/model decisions and final integration. One core owner handles resources.py, plan.py, flow.py, __init__.py and rate-limit tests in a serial wave. Once model fields are fixed, a separate projection owner handles cli.py, v3 schemas/goldens, CLI tests, and Docs. No concurrent edits to shared hotspots or living spec sync/archive. Changes are not committed or pushed without a user request.

## Risks / Trade-offs

- [A body makes multiple external requests] → label the unit as body starts and document an explicit application limiter for finer granularity.
- [Phase transitions duplicate events or lose output] → test one terminal result, one started/finished lifecycle, and commit-before-downstream.
- [Clock or scheduler jitter] → use monotonic deadlines and never admit early; late starts remain valid.
- [Large pending partition sets] → keep one earliest deadline wait rather than one sleeper per key.
- [Concurrent runs exceed an external quota] → document run-local scope; applications choose external coordination when needed.

## Migration Plan

Implement against the preserved concurrency/materialization baseline, validate focused and full tests, publish plan v3 migration guidance, build core-only wheel, then sync/archive serially. Keep the existing Python 3.10–3.13 CI matrix. No persistent resource state is migrated.
