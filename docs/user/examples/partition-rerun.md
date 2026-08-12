# Rerun selected dates

Declare a date-partitioned asset, then use one identical selection for review and
execution.

```python title="daily.py"
from kazeflow import DatePartitionDef, Flow, asset


@asset(partition_def=DatePartitionDef())
def publish_daily() -> None:
    print("published selected date")


flow = Flow(["publish_daily"])
```

```console
kazeflow plan daily.py --partition-key 2026-08-11
kazeflow run daily.py --partition-key 2026-08-11 --yes
```

For several slices, repeat the option. The current CLI accepts each value as an
explicit string; `DatePartitionDef` does not validate or coerce it. Omitting every
key is a configuration error before an asset body runs.

Generate `date` objects through the Python API only when that is the key type your
asset expects:

```python
keys = DatePartitionDef().range("2026-08-11", "2026-08-12")
config = {"partition_keys": keys}
plan = flow.plan(config)
```

See [Select partitions deliberately](../partitions.md) for empty selections,
portable-record sensitivity, and the current validation boundary.
