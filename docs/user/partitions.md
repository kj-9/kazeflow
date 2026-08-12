# Select partitions deliberately

A partitioned asset represents independent slices of one task—often dates, regions,
files, or tenants. An unpartitioned asset runs once. A partitioned asset creates one
attempt for each explicitly selected, normalized key.

The selected dependency closure has one partition domain. The definition on each
partitioned asset owns how its input becomes a canonical key, or whether it is
rejected. This makes a plan useful for review: invalid selection is diagnosed before
an asset body or execution event starts.

## Declare a date-partitioned asset

```python
from kazeflow import DatePartitionDef, Flow, asset


@asset(partition_def=DatePartitionDef())
def publish_daily() -> None:
    print("publish the selected day")


flow = Flow(["publish_daily"])
```

`DatePartitionDef` has the stable `date` domain. It accepts canonical ISO
`YYYY-MM-DD` text from the CLI and normalizes it to an in-memory `datetime.date`
before execution. Invalid dates, non-canonical text, and reversed ranges are
configuration errors; their rejected input is not repeated in the diagnostic.

## Inspect the definition, then plan

Definition inspection does not enumerate a catalog of dates and does not invoke an
asset body. It reports each selected partitioned asset's definition kind, domain,
accepted key form, and whether a bounded range is supported.

```console
kazeflow partitions daily.py --target publish_daily
kazeflow plan daily.py --target publish_daily --partition-key 2026-08-08
kazeflow run daily.py --target publish_daily --partition-key 2026-08-08 --yes
```

Plan first. The plan exposes the normalized selection kind, `date` domain, and safe
counts, but not the selected raw values. The approved run uses the same selection.

## Choose exactly one selection form

For partitioned work, choose one of these forms. Omitting all selectors is an error;
it never means today, all history, or an unbounded catalog. The forms cannot be
combined in one plan or run.

| Form | Command | Meaning |
| --- | --- | --- |
| Keys | `--partition-key 2026-08-08` | Select one normalized key. Repeat `--partition-key` (or its `--partition` alias) for several keys. |
| Bounded range | `--partition-range 2026-08-08 2026-08-10` | Select the inclusive range; a date definition expands this to three normalized dates. |
| Deliberate empty work | `--empty-partitions` | Select zero partition attempts intentionally. A partitioned task is skipped with `no_partition_keys`, rather than being an omitted configuration. |

For example, repeated keys are explicit individual selections:

```console
kazeflow plan daily.py \
  --partition-key 2026-08-08 \
  --partition-key 2026-08-09
```

And a range is concise only when its finite endpoints are known:

```console
kazeflow plan daily.py --partition-range 2026-08-08 2026-08-10
kazeflow run daily.py --partition-range 2026-08-08 2026-08-10 --yes
```

To review the zero-work case explicitly:

```console
kazeflow plan daily.py --empty-partitions
```

If a selected closure contains no partitioned asset, every selector is a usage
error rather than an ignored option. If it does contain one, omitting all selectors
is a configuration error before any asset body runs.

## Use ranges from Python

The Python API makes the same finite, inclusive choice. `DatePartitionDef.range()`
normalizes both endpoints and returns canonical `date` keys:

```python
from kazeflow import DatePartitionDef, Flow, run


date_partitions = DatePartitionDef()
keys = date_partitions.range("2026-08-08", "2026-08-10")
config = {"partition_keys": keys}

plan = Flow(["publish_daily"]).plan(config)
result = run(["publish_daily"], config)
```

Custom `PartitionDef` implementations retain identity normalization by default. If a
custom definition accepts `0`, `""`, or `False`, each is a present key—not omitted
work. A custom definition should publish a stable domain and normalization contract
when it needs stricter validation.

## Keep the portable boundary in mind

The portable **plan** JSON reports selection kind, domain, and safe counts. Portable
run JSON and SQLite records retain only whether an attempt has a partition and omit
the dedicated raw partition-key field. Neither is generic redaction:
application-controlled exception messages and tracebacks can repeat a key or another
sensitive value. Treat portable records as potentially sensitive and use the
in-memory `RunResult` when the structural key value itself matters. [Read the full
trust boundary](concepts/trust-boundary.md).

!!! warning

    Planning and `kazeflow partitions` must load trusted Python before inspecting a
    flow. Definition validation happens before asset bodies run, but it does not
    sandbox entry loading or automatically redact application failure metadata.
