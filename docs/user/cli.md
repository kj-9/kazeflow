# CLI reference

The `kazeflow` command is a zero-runtime-dependency review layer over trusted Python
flow scripts. Use task pages for the exact command you need.

| Command | Purpose |
| --- | --- |
| [`assets`](cli/assets.md) | List assets discovered while loading an entry. |
| [`partitions`](cli/partitions.md) | Inspect partition definitions and supported selection forms without asset execution. |
| [`plan`](cli/plan.md) | Review targets, order, partitions, configuration, and graph. |
| [`run`](cli/run.md) | Review a preflight, approve, execute, and print a result. |
| [`runs`](cli/history.md) | List, show, or compare explicitly stored records. |

## Entry forms

```console
kazeflow plan daily.py
kazeflow plan daily.py:flow
kazeflow plan package.module:flow
kazeflow run package.module:make_flow --yes
```

A bare `.py` file prefers its module-level `flow`. Without one, kazeflow uses assets
registered while that file loads and derives all terminal assets as default targets.
An explicit attribute may be a `Flow` or a zero-argument factory returning one.
Factories are never guessed implicitly.

## Output contract

Human-readable text can improve in layout while preserving meaning. JSON is the
stable automation boundary. Successful JSON modes write exactly one document to
standard output; diagnostics, preflight, confirmation, and progress use standard
error.

See [Exit codes and automation](cli/exit-codes.md) for status precedence and
[Trust boundary](concepts/trust-boundary.md) before loading unfamiliar source.
