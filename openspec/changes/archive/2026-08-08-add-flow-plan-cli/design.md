## Context

The approved `define-cli-contracts` change makes `kazeflow assets` and
`kazeflow plan` the first CLI-first workflow. The base distribution has no
required third-party runtime dependency, so M8 uses `argparse` and a thin
adapter around existing planning models rather than Click or Typer.

## Goals / Non-Goals

**Goals:**

- Ship a core-only console script that can load a trusted Python script, inspect
  registered assets, and show a deterministic plan without invoking asset bodies.
- Keep parsing, script loading, target selection, plan projection, and process
  I/O separable and directly testable.
- Preserve the existing Python API and planning semantics.

**Non-Goals:**

- Run an asset, prompt for confirmation, persist results, or render Rich output.
- Sandbox user code or claim that loading a script has no side effects.
- Add a third-party runtime dependency, a manifest language, automatic factory
  invocation, a scheduler, or a database.

## Decisions

### A stdlib adapter owns process concerns

`src/kazeflow/cli.py` exposes `main(argv: Sequence[str] | None = None) -> int`
and uses `argparse`. It delegates entry loading, target selection, and projection
to small internal helpers. The console-script entry point calls `main` through
`SystemExit`; tests call `main` or subprocesses without needing a framework
runner.

This preserves the zero-dependency base wheel. Click and Typer would reduce
parser boilerplate but would change the package's runtime-dependency contract.

### Script loading is explicit and bounded

A bare `.py` entry is loaded as Python once per CLI invocation. The loader uses
the script path as the source of truth and does not run `__main__` application
blocks as a command-line script. It records the assets registered during that
load so unrelated pre-existing registry state cannot become discovery output.

When `flow` is a `Flow`, its targets and registry supply the declared default.
Otherwise terminal discovered assets are derived from the dependency graph.
Multiple candidate targets are valid for `assets` and `plan`; M8 only refuses no
target ambiguity for a future `run` command.

### Projections are deterministic and intentionally lossy

Text presents names, dependencies, targets, partitions, and normalized config in
deterministic order. JSON uses a documented, JSON-serializable envelope and
does not serialize raw partition-key objects. `assets` and `plan` each write one
JSON document to stdout; diagnostics use stderr. Field-level JSON schema details
are implemented and snapshot-tested in this change, then stabilized in M11.

### Errors map at the CLI boundary

The adapter maps argument/configuration failures to status 2, entry-load failures
to status 3, and unexpected CLI infrastructure failures to status 4. Existing
Python exceptions remain available to API callers; the CLI provides concise
diagnostics instead of tracebacks by default.

## Risks / Trade-offs

- [Loading a script runs top-level Python] → Document it in help and errors; do
  not invoke asset bodies for `assets` or `plan`.
- [Global default registry can leak assets across loads] → Capture a load-local
  registry delta and test sequential invocations.
- [Deriving terminal targets surprises multi-flow scripts] → Display all
  candidates clearly; leave execution ambiguity handling to M9.
- [Argparse output is less polished than a framework] → Keep parsing thin and
  defer optional interactive enhancements until there is a demonstrated need.

## Migration Plan

This is a new command with no prior CLI compatibility surface. Add the console
script and core-only wheel smoke coverage together; rollback removes that entry
point without changing the Python API or persistence schema.

## Open Questions

- Confirm the supported behavior for package-relative imports from file entries
  during implementation and document any unsupported layout clearly.
