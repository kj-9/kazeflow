# CLI inspection

The standard-library-only `kazeflow` command provides two inspection commands:

```bash
kazeflow assets ENTRY
kazeflow plan ENTRY [--target NAME] [--format json]
```

They supplement the Python API. They do not run a flow, prompt for approval, show
the optional Rich TUI, or save a SQLite run record.

## Define a script entry

A script can register assets in the usual way. Define a module-level `flow` when
the script has an intended default target or targets:

```python
from kazeflow import Flow, asset


@asset
def source() -> str:
    return "input"


@asset
def summarize(source: str) -> int:
    return len(source)


flow = Flow(["summarize"])
```

Pass a bare `.py` path to inspect that script:

```bash
kazeflow assets examples/flow.py
kazeflow plan examples/flow.py
```

An explicit entry can select a flow attribute instead:

```bash
kazeflow assets examples/flow.py:flow
kazeflow plan package.module:flow
```

For a bare script, a module-level `flow` takes precedence as the default flow.
Without one, `assets` lists the assets registered while the script loads, and
`plan` derives every discovered terminal asset (an asset that no other discovered
asset depends on) as its default targets. This makes scripts with several independent
outputs inspectable without choosing only one first. A script with neither a declared
flow nor discovered assets cannot be inspected.

## List assets

Use `assets` to see the deterministic list of assets that a script defines:

```bash
kazeflow assets examples/flow.py
kazeflow assets examples/flow.py --format json
```

`--format json` writes exactly one JSON document to standard output on success. It
is useful for CI and automation; normal text output is intended for review in a
terminal.

## Review a plan

`plan` renders the selected targets, dependency-first task order, partition
selection, and normalized execution configuration without invoking an asset body:

```bash
# Use the declared flow or derived terminal targets.
kazeflow plan examples/flow.py

# Inspect one target's dependency closure.
kazeflow plan examples/flow.py --target summarize

# Consume the intentionally lossy projection from another program.
kazeflow plan examples/flow.py --format json
```

The JSON projection is a review-oriented representation, not a serialization format
for arbitrary Python values. In particular, it does not expose raw partition-key
objects. Successful JSON output is written only to standard output, so diagnostics
remain available on standard error.

## Loading is not sandboxing

`assets` and `plan` load the supplied entry as Python in order to discover its
definitions. Consequently, top-level statements and imports run during loading and
may have side effects. The commands never invoke a decorated asset function while
listing assets or building a plan, but they cannot make loading untrusted Python
safe. Review the script and its environment before supplying it to the CLI.

## Exit status and diagnostics

Successful inspections exit with status `0`. Invalid command-line arguments or
planning configuration exit with status `2`; entries that cannot be loaded or do not
resolve to an inspectable definition exit with status `3`. Diagnostics are written to
standard error. In JSON mode, failures do not produce a successful JSON document on
standard output.

## Current scope

This first CLI surface is inspection only. `kazeflow run`, interactive confirmation,
Rich terminal rendering, and SQLite run-record persistence are not implemented as
CLI commands. The core CLI has no mandatory third-party runtime dependency.
