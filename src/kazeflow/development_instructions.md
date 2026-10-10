# kazeflow Development Instructions

This guide is bundled with your installed kazeflow version. It is a short guide
for authors and coding agents; the [official documentation](https://kj-9.github.io/kazeflow/)
is the detailed reference.

## Define work that the CLI can discover

- Write ordinary Python functions and decorate assets at module level with `@asset`.
  The CLI discovers assets registered while it loads the entry file.
- Prefer `flow = Flow(["target_name"])` at module level to choose a default target.
  Without it, a bare `.py` entry derives terminal registered assets as targets.
  An ambiguous derived `run` needs an explicit `--target`.
- An explicit `file.py:flow` or `package.module:flow` may name a `Flow`.
  An explicitly named, zero-argument factory returning a `Flow` also works.
  Factories are not discovered or called implicitly.
- Assets defined inside a function are registered only when that function runs.
  Put CLI-discoverable definitions at module level or create and return the
  intended `Flow` from an explicit factory.

## Use the project's Python environment

Run `kazeflow` from the environment where the entry script's imports are
available. Installing kazeflow alone does not install dependencies imported by
your own script.

## Inspect before execution

```console
kazeflow assets flow.py
kazeflow partitions flow.py
kazeflow plan flow.py
```

For a partitioned flow, select work explicitly. Inspect its definition with
`partitions`, then use `plan` with `--partition-key KEY` (repeatable),
`--partition-range START END` (bounded, when supported), or
`--empty-partitions`. No partition is selected implicitly.

`plan` does not invoke asset bodies. Loading an entry still imports Python,
and an explicitly selected factory is called; either can run user code and
have side effects. Planning is not a sandbox or a safety proof. Read and trust
unfamiliar source before loading it. Use `run` only when you intend to execute
the reviewed work; it asks for approval unless you pass `--yes`.

For details, read the [CLI reference](https://kj-9.github.io/kazeflow/cli.html),
[partition guide](https://kj-9.github.io/kazeflow/partitions.html), and
[trust boundary](https://kj-9.github.io/kazeflow/concepts/trust-boundary.html).
