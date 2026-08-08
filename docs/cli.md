# CLI usage

The standard-library-only `kazeflow` command provides inspection and deliberate
execution commands:

```bash
kazeflow assets ENTRY
kazeflow plan ENTRY [--target NAME ...] [--partition-key KEY ...] \
    [--max-concurrency N] [--verbose] [--format text|json|mermaid|dot]
kazeflow run ENTRY [--target NAME ...] [--partition-key KEY ...] \
    [--max-concurrency N] [--yes] [--tui] [--store PATH] [--format text|json]
kazeflow runs list [--store PATH] [--limit N] [--format text|json]
kazeflow runs show RUN_ID [--store PATH] [--format text|json]
kazeflow runs compare LEFT_RUN_ID RIGHT_RUN_ID [--store PATH] [--format text|json]
```

They supplement the Python API. `assets` and `plan` inspect only; `run` requires a
separate, explicit decision before it invokes asset bodies.

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

An explicit entry can select a flow attribute or a zero-argument factory that returns
a `Flow`:

```bash
kazeflow assets examples/flow.py:flow
kazeflow plan package.module:flow
kazeflow run package.module:make_flow --yes
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

`plan` renders a concise summary and a deterministic dependency graph for the
selected targets without invoking an asset body. The default text projection is for
human review; use JSON rather than parsing its whitespace in automation:

```bash
# Use the declared flow or derived terminal targets.
kazeflow plan examples/flow.py

# Inspect one target's dependency closure.
kazeflow plan examples/flow.py --target summarize

# Consume the intentionally lossy projection from another program.
kazeflow plan examples/flow.py --format json

# Export the same resolved graph for Markdown/GitHub or Graphviz.
kazeflow plan examples/flow.py --format mermaid
kazeflow plan examples/flow.py --format dot > flow.dot

# Add normalized configuration and per-task metadata to text review output.
kazeflow plan examples/flow.py --verbose
```

Use repeatable `--target` to select one or more targets. `--partition-key` (also
available as `--partition`) supplies selected partition keys, and
`--max-concurrency` supplies the execution concurrency value to plan. These options
are also accepted by `run`, so its preflight and execution use the same resolved
entry and selections within one invocation.

`--verbose` is intentionally text-only. Combining it with JSON, Mermaid, or DOT is
a usage error. Mermaid and DOT describe the resolved plan; kazeflow does not invoke
an external renderer or install Graphviz/Mermaid. Paste Mermaid into a compatible
Markdown renderer or render DOT with an external tool when a larger graph needs a
visual layout.

The JSON projection is a review-oriented representation, not a serialization format
for arbitrary Python values. In particular, it does not expose raw partition-key
objects. Successful JSON output is written only to standard output, so diagnostics
remain available on standard error.

## Run after an explicit review

`run` builds a pre-execution plan and writes its summary to standard error before it
asks for confirmation or invokes an asset body. When both standard input and standard
error are TTYs, it prompts `Proceed? [y/N]`; only `y` or `yes`, case-insensitively,
starts the run. Any other response or EOF is a successful no-op: no asset, TUI, or
store is initialized, and no `RunResult` is created.

When either standard input or standard error is not a TTY, `run` does not prompt and
requires `--yes`. This makes execution in CI and pipelines an explicit choice:

```bash
kazeflow run examples/flow.py --target summarize --yes
```

In text mode, a completed run writes a human-readable terminal result summary. With
`--format json`, standard output contains exactly one portable, intentionally lossy
`RunResult` record. The preflight, confirmation prompt, cancellation notice,
progress presentation, and diagnostics use standard error, leaving JSON stdout safe
for automation. Raw outputs, exception objects, and raw partition-key values are not
included in that record.

### Optional adapters

The default execution path does not import Rich, create a database, or persist a run
record. Both optional adapters require explicit selection after execution approval:

```bash
# Present execution events with the optional Rich extra.
kazeflow run examples/flow.py --yes --tui

# Save the terminal result to this explicit SQLite database path.
kazeflow run examples/flow.py --yes --store runs.sqlite3
```

`--tui` lazily loads the optional Rich presentation before execution. It keeps a
single live view of waiting, running, succeeded, skipped, and failed tasks plus
overall completion. Progress is written to standard error, so it remains compatible
with a one-document JSON result on standard output. `--store PATH` constructs the
SQLite store only after a terminal result is available and saves the result before
successful final output is emitted. If a requested adapter fails, the CLI reports
that infrastructure failure and does not emit a successful final result; it takes
precedence over an asset-failure status.

## Inspect local run history

Run history stays local and is deliberately separate from core execution. History
commands read `./.kazeflow/runs.sqlite3`, relative to the directory where the command
is invoked, unless `--store PATH` selects another existing store. They never create,
initialize, migrate, or write a database. A missing default store is therefore an
infrastructure error rather than an empty history.

Save a run explicitly, then inspect it without writing Python:

```bash
mkdir -p .kazeflow
kazeflow run examples/flow.py --yes --store .kazeflow/runs.sqlite3

kazeflow runs list
kazeflow runs show RUN_ID --format json
kazeflow runs compare RUN_A RUN_B
```

`list` is ordered by saved time and then run ID; `--limit N` keeps the first `N`
entries. `show` returns the stored portable envelope. `compare` preserves the
left/right IDs supplied by the caller and compares run and task aggregates only.
It does not attempt to identify individual partitions, because partition keys are
not stored. All successful JSON modes write exactly one document to standard output.
Unknown run IDs and invalid history arguments exit `2`; unreadable, malformed, or
missing stores exit `4` with diagnostics on standard error.

## Loading is not sandboxing

All commands load the supplied entry as Python in order to discover its definitions.
Consequently, top-level statements and imports run during loading and may have side
effects. An explicit factory is also arbitrary user code and runs when selected. The
CLI's inspection path does not itself invoke a decorated asset function; `run` does
so only after its preflight and explicit execution decision. Factory code remains
responsible for any behavior it performs. None of these commands makes loading
untrusted Python safe. Review the script and its environment before supplying it to
the CLI.

## Exit status and diagnostics

| Status | Meaning |
| --- | --- |
| `0` | A successful inspection or terminal run, or a deliberate declined confirmation. |
| `1` | A confirmed run reached a terminal result with an asset failure. |
| `2` | Invalid command syntax or configuration, a missing non-interactive `--yes`, or an ambiguous discovered `run` target. |
| `3` | The entry could not be loaded or resolved to an inspectable flow. |
| `4` | CLI execution infrastructure or a requested TUI/store adapter failed. |

Diagnostics always use standard error. In JSON mode, configuration, entry, and
infrastructure failures produce no successful document on standard output. A selected
adapter failure after a terminal asset failure still exits `4` and suppresses the
final result document.

## Public CLI compatibility

The documented `kazeflow` command names, options, exit statuses, and JSON schemas
are the public CLI interface. JSON is the stable automation boundary; text output is
kept stable in meaning for human review but can receive layout improvements. Before a
future compatible release removes or renames a documented command or option,
kazeflow will publish a deprecation, migration path, and release note.

## Current scope

The CLI does not add a scheduler, daemon, remote execution, cache, sandbox, or
automatic approval. The core CLI has no mandatory third-party runtime dependency;
Rich presentation and SQLite persistence remain explicit optional behavior.
