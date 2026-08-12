# `kazeflow partitions`

Inspect partition definitions in a selected dependency closure without executing an
asset body or enumerating a dynamic catalog. Entry loading is still trusted Python;
see the [trust boundary](../concepts/trust-boundary.md) before loading unfamiliar
source.

## Synopsis

```console
kazeflow partitions ENTRY [--target NAME ...] [--format text|json]
```

## What it reports

For every partitioned asset in the selected closure, the command reports:

- asset name;
- definition kind;
- stable partition domain;
- accepted key format; and
- whether the definition supports an explicit bounded range.

For example, a `DatePartitionDef` reports its `DatePartitionDef` definition kind,
the `date` domain, strict ISO `YYYY-MM-DD` key format, and inclusive bounded-range
support. The command does not guess today, list all historical dates, or invoke
asset bodies. An unpartitioned closure succeeds with an explicit empty result.

```console
kazeflow partitions daily.py --target publish_daily
kazeflow partitions daily.py --format json
```

Use this command before choosing one selector for `plan` or `run`:

```console
kazeflow plan daily.py --partition-range 2026-08-08 2026-08-10
```

Text and JSON outputs omit selected raw keys because this command reports definition
metadata rather than work selection. JSON is a deterministic automation projection;
keep diagnostics on standard error separate from successful one-document JSON output.
Its typed envelope, fields, and normative schema are listed in the
[JSON automation contract](json.md#version-1-data-fields).
