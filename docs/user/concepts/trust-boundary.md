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

A selected materialization adapter is also trusted application code. Core checks
that its returned record matches the requested asset version, partition, and input
identities, but it cannot prove that the referenced output is durable, atomic, or
unchanged between resolution and load. The adapter decides how raw partition keys
and outputs are encoded, validated, stored, and protected. Use an adapter whose
storage behavior fits the data and concurrency risks of the application.

`FileMaterialization.invalidate` is an explicit application operation. Quiesce users
of the exact identity before calling it; it provides no cross-process locking. Its
non-empty `reason` is stored as plain application metadata in the revocation ledger.
Do not put secrets or personal data in that reason. Copy the full root, including the
ledger, to retain revocation when moving a store. Restoring/deleting the ledger or
rolling back the entire root can remove that decision, so it is outside the stale
artifact restoration guarantee. Invalidation does not delete output generations or
cascade to dependent identities. See the
[partition invalidation guide](../partitions.md#invalidate-one-materialization-explicitly).

`FileMaterialization.available(contexts)` is caller-scoped metadata lookup, not a
snapshot transaction. It does not scan the root or infer prior outputs from run
history. A selected record can be invalidated after collection, and a subsequent
`load` must still reject it. Applications requiring a stable selected set must
coordinate writers and invalidation across collection and loading.

Materialization IDs, aggregate rows, and publication receipts are application data.
The partial aggregation example stores its receipt and rows together in one local
JSON file and uses a deterministic ID for identical rows and included materialization
IDs. JSON and atomic replacement provide neither encryption nor redaction. The
example's local file behavior does not make an external sink transactional or
exactly-once; the application owns that sink's receipt and retry policy.

## Portable-record boundary

`RunResult.to_record()`, CLI JSON, and SQLite storage deliberately omit arbitrary
task outputs, raw exception objects, and the dedicated raw partition-key field.
Record v4 also carries successful resolution and adapter-defined materialization IDs
as diagnostic metadata. These IDs are opaque, may contain application information,
and are not automatically redacted. History inspection does not contact adapters or
load artifacts. Portable records retain failure metadata: exception type, message,
and traceback.

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
