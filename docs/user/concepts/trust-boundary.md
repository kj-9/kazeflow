# Trust boundary

kazeflow makes declared work inspectable. It does not make arbitrary Python safe.

## Loading boundary

To discover a flow, the CLI imports a file or module. Python top-level statements,
imports, decorators, and an explicitly selected factory can execute during that
load. `assets` and `plan` do not invoke decorated asset bodies after loading, but
they cannot make the import side-effect-free.

## Execution boundary

An asset body is ordinary Python. It can read or write files, use the network,
launch subprocesses, access credentials, and mutate external systems. A reviewed
dependency graph is neither a security review nor a proof about those effects.

## Portable-record boundary

`RunResult.to_record()`, CLI JSON, and SQLite storage deliberately omit arbitrary
task outputs, raw exception objects, and the dedicated raw partition-key field.
They retain portable failure metadata: exception type, message, and traceback.

Those strings are controlled by the application and Python runtime. An asset can
include a partition key, credential fragment, path, query, or other sensitive value
in an exception, and that value can then appear in JSON or SQLite. Structural field
omission is not sanitization, redaction, or a confidentiality guarantee. Review and
protect portable records as application diagnostic data.

## Responsible workflow

1. Read and trust the entry source and its imports.
2. Inspect targets, dependencies, partitions, and configuration with `plan`.
3. Review the asset bodies and relevant external state.
4. Explicitly decide whether to run.
5. Inspect the `RunResult`; before emitting or storing its portable form, consider
   what its failure messages and tracebacks contain.

For genuinely untrusted code, use an execution environment designed as a sandbox;
that remains outside kazeflow core's scope.
