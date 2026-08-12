# Select partitions deliberately

A partitioned asset represents independent slices of one task—often dates, regions,
files, or tenants. The Python partition definition owns what keys mean; the CLI lets
you select them explicitly.

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

CLI values are strings passed to the script's partition definition. kazeflow does
not guess a type or manufacture today's date.

## Selection semantics

| Input | Meaning |
| --- | --- |
| Omit `--partition-key` | No explicit CLI selection; the partition definition determines work. |
| Pass one or more keys | Request those exact textual values. |
| Python API empty tuple | Explicitly select no partition work when supported. |
| Python falsey keys | `0`, `""`, and `False` remain present values, not omission. |

Portable JSON and SQLite records retain whether a partition is present but omit its
raw key. Use the in-memory `RunResult` when the original value matters.

!!! warning

    Planning must load trusted Python before asking the partition definition which
    keys are valid. It does not sandbox the import.
