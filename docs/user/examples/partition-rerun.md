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

For several slices, repeat the option. The partition definition decides whether a
textual key is valid; the CLI does not synthesize or coerce dates.
