# `kazeflow run`

Plan a trusted entry, show the preflight, require an explicit decision, execute the
selected asset bodies, and emit the terminal result.

## Synopsis

```console
kazeflow run ENTRY \
  [--target NAME ...] \
  [--partition-key KEY ... | --partition-range START END | --empty-partitions] \
  [--max-concurrency N] [--yes] [--tui] [--store PATH] \
  [--verbose] [--format text|json]
```

## Confirmation

When standard input and standard error are terminals, `run` asks
`Proceed? [y/N]`; only `y` or `yes` proceeds. Declining is a successful no-op and
initializes neither assets, TUI, store, nor `RunResult`.

Non-interactive execution requires `--yes`:

```console
kazeflow run daily.py --yes
```

For a partitioned closure, choose exactly one explicit selection form. `run` prints
the normalized preflight selection kind, domain, and safe counts before confirmation;
keys themselves remain out of portable presentation. Definition validation, including
strict date validation and reversed-range rejection, completes before any asset body
runs.

## Result modes

Text prints the run ID, status, duration, and each task outcome. `--verbose` adds
structurally limited attempt detail. `--format json` writes one portable result
document to standard output while preflight and diagnostics remain on standard
error.

Portable output omits dedicated raw-key fields and arbitrary task outputs, but
failure messages and tracebacks can contain sensitive application values. See the
[portable-record trust boundary](../concepts/trust-boundary.md#portable-record-boundary).
For the typed envelope, separate portable-record version, schema, and exit behavior,
see the [JSON automation contract](json.md).

## Optional adapters

```console
kazeflow run daily.py --tui
kazeflow run daily.py --yes --store .kazeflow/runs.sqlite3
```

The Rich TUI is loaded before execution only when requested. The SQLite store is
opened after a terminal result exists. A requested adapter failure is infrastructure
failure and takes precedence over an asset-failure process status.
