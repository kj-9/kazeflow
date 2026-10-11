# Assets, dependencies, and targets

An asset is an ordinary Python function registered with `@asset`. Its parameter
names declare direct dependencies unless `deps=` is supplied explicitly.

```python
from kazeflow import Flow, asset


@asset
def download() -> bytes:
    return b"input"


@asset
def transform(download: bytes) -> int:
    return len(download)


@asset(deps=["transform"])
def publish() -> None:
    print("published")


flow = Flow(["publish"])
```

## Choose targets

Targets identify the outputs requested for a flow. Planning selects each target and
its transitive dependencies, then returns them in deterministic dependency-first
order.

```console
kazeflow plan pipeline.py
kazeflow plan pipeline.py --target transform
```

`--target` is repeatable. A bare script with no module-level `flow` derives every
discovered terminal asset as a default target. Use an explicit `flow` whenever the
script has one intended entry.

## Keep asset bodies testable

Separate I/O from transformation logic when practical, then unit-test the underlying
functions normally. kazeflow supplies scheduling and result structure; it does not
require an asset body to inherit from a framework class or communicate through a
remote runtime.

## Inspect before execution

Planning detects missing assets, cycles, invalid partition selections, and invalid
concurrency configuration before an asset body runs. It does not validate external
files, credentials, services, or the safety of arbitrary Python code.

## Bound shared resource starts

Associate assets with a named resource to share concurrency and an optional rolling
start limit across those assets and their partition keys:

```python
from kazeflow import Flow, RateLimit, ResourcePolicy, asset


@asset(resource="network")
def fetch_metadata():
    ...


@asset(resource="network")
def fetch_payload(fetch_metadata):
    ...


flow = Flow(
    ["fetch_payload"],
    resources={
        "network": ResourcePolicy(
            max_concurrency=4,
            rate_limit=RateLimit(requests=20, per_seconds=60),
        )
    },
)
```

The count measures asset body invocation starts, rather than HTTP requests issued
inside a body. A compatible materialization hit does not start a body, so it uses
no rate permit. Limits are shared across matching assets during one run; separate
runs maintain independent limits. Use an application-level limiter when you need
to coordinate API requests or multiple runs.

## Retry selected body failures

Retries are opt-in. A resource policy sets the maximum number of body invocations
including the first call and supplies a synchronous classifier. It can return
`Retry` with an explicit delay, or `Fail`:

```python
from kazeflow import Fail, Flow, ResourcePolicy, Retry, asset


def classify_failure(error: Exception) -> Fail | Retry:
    if isinstance(error, TimeoutError):
        return Retry(after_seconds=1, cooldown_resource=True)
    return Fail()


@asset(resource="network")
def fetch():
    ...


flow = Flow(
    ["fetch"],
    resources={
        "network": ResourcePolicy(
            max_concurrency=4,
            max_attempts=3,
            classify_failure=classify_failure,
        )
    },
)
```

The classifier sees exceptions raised by the asset body. Materialization preparation,
loading, commit, observer, and CLI failures do not become retryable body failures.
Cancellation is not classified as a body failure. The classifier runs synchronously
on the event loop, so keep it fast and do not perform I/O in it. If it raises,
returns an unsupported action, or returns an awaitable, the logical attempt fails.
There is no implicit retry, HTTP status parsing, exponential backoff, or jitter.

Every retry obtains a fresh concurrency and rate permit. Backoff and shared resource
cooldown waits release the execution slot, so independent work can continue. A
cooldown affects pending work using the same resource for this run; already running
calls finish. A `Retry` decision can request a resource cooldown after that logical
attempt has used its full invocation budget: the attempt fails without another body
call, while the requested shared cooldown still applies. Its final history item has
no retry delay and records the requested cooldown. A logical task/partition still
has one terminal result, with its numbered invocation history attached.
`AssetContext.invocation_number` identifies the current body call.

Retries may repeat side effects. Make retried operations idempotent or otherwise
safe to repeat; kazeflow's exactly-once terminal result does not guarantee exactly
once external effects.

## Open a resource circuit

A synchronous resource failure classifier can return `OpenCircuit()` when the
resource should stop receiving new body work for the rest of the current run:

```python
from kazeflow import Fail, Flow, OpenCircuit, ResourcePolicy, asset


def classify_failure(error: Exception) -> Fail | OpenCircuit:
    if isinstance(error, PermissionError):
        return OpenCircuit()
    return Fail()


@asset(resource="network")
def fetch():
    ...

flow = Flow(
    ["fetch"],
    resources={
        "network": ResourcePolicy(
            max_concurrency=1,
            classify_failure=classify_failure,
        )
    },
)
```

The classifier runs only for an exception from the asset body. The first validated
`OpenCircuit()` decision in a run identifies the source attempt; pending body work
using that resource is skipped with `resource_circuit_open` and points to that source.
The failure is permanent for that run, and another run starts with a closed circuit.
No threshold, probe, or timer resets the circuit during a run.

Dependency outcomes are checked before circuit state. Work whose dependencies have
not finished waits; work blocked by a failed dependency keeps the dependency-blocked
reason. Materialization preparation and loads may proceed while the circuit is open:
a compatible cache hit completes without calling the resource body, while a miss or
non-materialized body is skipped. Work already admitted, including body calls and
materialization operations, finishes normally and is not cancelled. A pending retry
is skipped immediately after the circuit opens, even if it has a retry delay recorded.

`blocked_by` names the first source attempt in memory. Portable records omit raw
partition keys and preserve only the task name and whether a partition key was
present, so two source partitions can be indistinguishable after serialization.
