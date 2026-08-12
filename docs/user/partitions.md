# Select partitions deliberately

A partitioned asset represents independent slices of one task—often dates, regions,
files, or tenants. In the current release, the CLI selects keys explicitly and
passes each value through as a string. A Partition definition marks the asset as
partitioned; it does not parse or validate CLI values.

## Declare a partitioned asset

```python
from kazeflow import DatePartitionDef, Flow, asset


@asset(partition_def=DatePartitionDef())
def publish_daily() -> None:
    print("publish the selected day")


flow = Flow(["publish_daily"])
```

An unpartitioned asset runs once. A partitioned asset creates one attempt per
selected key.

## Plan, then run the selection

```console
kazeflow plan daily.py --target publish_daily --partition-key 2026-08-08
kazeflow run daily.py --target publish_daily --partition-key 2026-08-08 --yes
```

Repeat `--partition-key` (or `--partition`) to select several keys:

```console
kazeflow plan daily.py \
  --partition-key 2026-08-08 \
  --partition-key 2026-08-09
```

CLI values are selected strings. kazeflow does not validate them as dates, guess a
type, call `DatePartitionDef.range()`, or manufacture today's date. Validation inside
an asset body happens only after approval and execution, so inspect the selection in
the plan first.

## Selection semantics

| Input | Meaning |
| --- | --- |
| Omit `--partition-key` | Configuration error when the selected closure contains a partitioned asset; no asset body runs. |
| Pass one or more keys | Request those exact textual values. |
| Python API empty tuple | Explicitly select no partition attempts; the task aggregates as skipped. |
| Python falsey keys | `0`, `""`, and `False` remain present Python values, not omission. CLI values are strings. |

## Generate a date range in Python

`DatePartitionDef.range()` is an explicit Python helper. Pass its returned `date`
objects into the plan and run configuration yourself:

```python
from kazeflow import DatePartitionDef, Flow, run


keys = DatePartitionDef().range("2026-08-08", "2026-08-09")
config = {"partition_keys": keys}

plan = Flow(["publish_daily"]).plan(config)
result = run(["publish_daily"], config)
```

Portable JSON and SQLite records retain whether a partition is present but omit the
dedicated raw-key field. This is not generic redaction: exception messages and
tracebacks can repeat the key or other application values. Treat portable records as
potentially sensitive and use the in-memory `RunResult` when the structural key
value itself matters. [Read the full trust boundary](concepts/trust-boundary.md).

!!! warning

    Planning must load trusted Python before it can inspect the selected flow. It
    does not ask the current Partition definition to validate CLI keys, and it does
    not sandbox the import.
