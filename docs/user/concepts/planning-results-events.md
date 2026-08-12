# Plan, result, and events

kazeflow separates execution information by when and why it is used.

| Information | When | Responsibility |
| --- | --- | --- |
| `FlowPlan` | Before execution | Review selected targets, dependency structure, partitions, and normalized configuration. |
| `RunResult` | After normal completion | Review terminal statuses, timings, in-memory outputs, and failure metadata. |
| Execution events | During execution | Feed optional progress consumers without coupling presentation to the executor. |
| Logs | During or after execution | Carry application-configured diagnostic detail. |

## Planning is metadata-only after loading

`Flow.plan()` validates the selected dependency closure and configuration without
invoking asset bodies. The CLI must first import a Python entry to obtain the flow;
that loading boundary is separate from planning.

## Partition selection is normalized configuration

For a partitioned closure, a `FlowPlan` records one explicit selection form: keys,
an inclusive bounded range, or deliberate empty work. The involved definitions must
share a stable domain, and each definition normalizes the supplied input before
attempts exist. An omitted selection, invalid date, reversed range, incompatible
domain, duplicate normalized key, or selector on unpartitioned work fails during
preflight.

The plan exposes selection kind, domain, and safe counts in portable presentation;
it does not expose arbitrary selected key values. See [Select partitions
deliberately](../partitions.md) for the command forms and the distinction between
empty work and missing configuration.

## Results are structured values

Asset failures normally appear in a terminal `RunResult` rather than being raised to
the caller. Task and partition-attempt results remain in deterministic plan and
selection order.

`RunResult.to_record()` creates a new JSON-friendly projection. It omits arbitrary
outputs, raw exception objects, and the dedicated raw partition-key field. It does
not write a file or promise durable history. Its portable exception messages and
tracebacks can still contain application-controlled values; see the
[portable-record trust boundary](trust-boundary.md#portable-record-boundary).

External cancellation is different from a normally completed result. Cancelling the
task awaiting `Flow.run_async()` propagates `asyncio.CancelledError` without a
synthetic terminal `RunResult` or complete terminal event sequence.

## Events do not own execution

An event consumer observes lifecycle values. It cannot authorize a run or replace
the terminal result. A consumer failure propagates as infrastructure failure rather
than being reported as an asset failure.
