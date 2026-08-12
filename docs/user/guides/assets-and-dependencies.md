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
