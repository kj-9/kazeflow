# Rerun selected dates

Declare a date-partitioned asset, inspect its selection contract, then plan and run
only the reviewed slice.

```python title="daily.py"
from kazeflow import DatePartitionDef, Flow, asset


@asset(partition_def=DatePartitionDef())
def publish_daily() -> None:
    print("published selected date")


flow = Flow(["publish_daily"])
```

```console
kazeflow partitions daily.py
kazeflow plan daily.py --partition-key 2026-08-11
kazeflow run daily.py --partition-key 2026-08-11 --yes
```

`DatePartitionDef` strictly validates canonical `YYYY-MM-DD` input and normalizes it
to a Python `date` before execution. An invalid date, a non-canonical date string,
or omitted selection is rejected during preflight; `publish_daily` does not run.

For consecutive dates, use one explicit inclusive range rather than relying on an
implicit current day or catalog:

```console
kazeflow plan daily.py --partition-range 2026-08-11 2026-08-12
kazeflow run daily.py --partition-range 2026-08-11 2026-08-12 --yes
```

The equivalent Python selection remains bounded and explicit:

```python
keys = DatePartitionDef().range("2026-08-11", "2026-08-12")
plan = flow.plan({"partition_keys": keys})
```

To plan no partition work on purpose, use `--empty-partitions`. This is distinct
from omitting every selector, which is an error for a partitioned flow:

```console
kazeflow plan daily.py --empty-partitions
```

See [Select partitions deliberately](../partitions.md) for repeatable keys, custom
falsey keys, domain compatibility, and portable-record sensitivity.
