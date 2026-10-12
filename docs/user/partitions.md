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

## Bound progress through a partition chain

When a flow has a linear chain of partitioned assets, declare a `PartitionGroup` to
limit how many selected keys can progress through that chain at once:

```python
from kazeflow import Flow, PartitionGroup

flow = Flow(
    ["publish_daily"],
    partition_groups=[
        PartitionGroup(("extract_daily", "transform_daily", "publish_daily")),
    ],
)
plan = flow.plan({"partition_keys": ["2026-08-08", "2026-08-09"]})
```

The default `max_active_keys=1` lets the first selected key finish every group
member before admitting the next key. Increase it to allow that many selected keys
to progress concurrently. This key limit is separate from `PlanConfig.max_concurrency`
and any resource policy: a key keeps its group slot while it waits for a retry,
cooldown, preparation, or an invocation permit, while those other limits continue
to control invocation starts and resource use.

Groups are explicit, disjoint, ordered linear chains of partitioned assets. Each
member after the first must directly depend on its predecessor. The selected group
must be wholly present in the flow plan; partial selection and dependency paths that
leave a group and re-enter it fail during planning. Grouping changes admission only:
it does not evict outputs from memory or coordinate separate runs. Ungrouped flows
keep their existing scheduling behavior. The
[runnable example](https://github.com/kj-9/kazeflow/blob/main/examples/partition_groups.py)
shows the first key traversing both assets before the second key starts.

## Inspect and explicitly select outcomes

After a run, use the live result to select terminal failures or resource circuit
skips for a partitioned task:

```python
failed = result.failed_partition_keys("publish_daily")
circuit_skipped = result.circuit_skipped_partition_keys("publish_daily")
retry_keys = failed + circuit_skipped

# A subsequent run is an explicit application decision.
retry_result = run(
    ["publish_daily"],
    {"partition_keys": retry_keys},
)
```

The queries return original key objects in attempt order. They include falsey keys,
and select terminal outcomes: a recovered invocation is not reported as failed, and
only skips with reason `resource_circuit_open` appear in the circuit query. A missing
task raises `KeyError`; a present unpartitioned task raises `ValueError`. The returned
tuple is immutable, while any mutable key objects inside it remain application-owned.
These helpers require the live `RunResult`; portable records intentionally omit raw
keys and cannot restore them. Passing keys to a new run is explicit; no automatic
resume occurs.

See the [runnable partition outcome example](https://github.com/kj-9/kazeflow/blob/main/examples/partition_outcomes.py).

Resource statistics are a separate, run-level summary. `RunResult.resources`
aggregates body/commit operations and observed resource-admission waits by selected
resource; it does not report a counter for each partition key. Wait timing begins
only after ready body work is observed being denied by a resource policy, so it
does not include dependency delay or global-only queueing that happens before that
check. An observed interval can include later scheduler delay, and wait durations
may overlap. See the [resource statistics API reference](api/plans-results.md#runresult)
and [example](https://github.com/kj-9/kazeflow/blob/main/examples/resource_statistics.py).

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

## Reuse committed partition outputs

Materialization lets a partitioned asset reuse an output that an explicitly
selected adapter has already committed. It helps when a later step fails and a
subsequent run repeats the same partition. The default asset path remains ordinary
Python execution; reuse is enabled per asset and requires both an adapter and a
non-empty explicit version.

Each materialized asset must be partitioned. Every direct dependency of a
materialized asset must also be materialized with a version, and those assets must
share the same partition domain. The input materialization IDs are part of the
downstream identity, so a changed upstream output does not match the downstream
record from the previous run.

Bump the version whenever the asset's meaning or output contract changes, including
changes to logic that should invalidate prior outputs. kazeflow does not hash Python
source or infer version changes. Planning validates the declaration and selected
dependency shape, and reports participation without contacting the adapter. A plan
does not claim that a reusable output exists.

For each selected key, the executor asks the adapter to resolve a record. A
compatible committed record is loaded and passed to matching downstream work. If
there is no compatible record, kazeflow invokes the asset body and waits for the
adapter to commit the output before making it available downstream. Resolve, load,
or commit failures fail that attempt; a load failure does not silently rerun the
asset body. See the [Materialization API](api/core.md#materialization-protocol) for
the protocol types.

### Reuse a file materialization across processes

The standard-library `FileMaterialization` adapter stores committed outputs under
an explicitly supplied local directory. The
[runnable example](https://github.com/kj-9/kazeflow/blob/main/examples/file_materialization.py)
has three invocations: commit the producer, run and fail a downstream asset, then
start a new process and retry the downstream asset. The producer is loaded from the
file store on the retry. Each materialized asset declares a non-empty version; bump
it when its meaning or output contract changes. The example uses `JsonCodec`, so
its output is restricted to JSON values.

```console
uv run python examples/file_materialization.py seed --root ./materialized --partition-key 2026-08-11
uv run python examples/file_materialization.py fail --root ./materialized --partition-key 2026-08-11
uv run python examples/file_materialization.py retry --root ./materialized --partition-key 2026-08-11
```

The middle command returns a failed run after the producer has already committed.
The final command starts a separate process, reuses that committed producer output,
and executes the downstream asset again.

The [materialization provenance example](https://github.com/kj-9/kazeflow/blob/main/examples/materialization_resume.py)
can also be run twice with the same store root. It prints the successful result and
attempt-finished event in portable v4 form, showing `executed` with the committed ID
first and `materialized` with that ID on the next invocation.

### Invalidate one materialization explicitly

`FileMaterialization.invalidate(context, reason="...")` revokes one exact identity:
asset name and version, partition domain, typed key and its presence, and the ordered
direct input materialization IDs. It does not cascade to downstream assets or delete
the stored generations. Repeating invalidation creates a fresh revocation epoch.
The optional `MaterializationInvalidation` protocol describes this operation; existing
adapters that only implement resolve/load/commit remain usable. Invalidation is never
performed automatically.

Quiesce all users of that identity before invalidating it. The adapter does not lock
across processes or coordinate a concurrent run with invalidation. After invalidation,
explicitly select the partition in a new run to rebuild it. A successful commit in the
new epoch is reusable by later runs. No run is resumed and no partition is selected
automatically.

Use `FileMaterialization.inspect(context)` to diagnose stored state without running an
asset or decoding its output. It reports `available`, `missing`, `invalidated`,
`incompatible`, or `corrupt`. Only `available` includes a committed record. Inspection
does not mutate the store.

The runnable
[invalidation example](https://github.com/kj-9/kazeflow/blob/main/examples/materialization_invalidation.py)
commits an output, invalidates its identity, explicitly runs that partition again, and
then confirms that the new generation is reused.

### Collect available outputs after a partial failure

Normal dependency behavior remains all-or-nothing: a consumer that depends on a
failed partition does not run with an incomplete input. An application can instead
make a separate, deliberate partial publication after the run has finished. The
optional `MaterializationCollection` protocol and
`FileMaterialization.available(contexts)` inspect only the exact contexts supplied
by the caller. Construct them from a known selection, including prior-run identities
only when the application intentionally chooses them; the adapter does not scan its
root or reconstruct keys from run history.

Collection preserves context order and returns committed records that are currently
available. Missing and invalidated identities are skipped. Corrupt or incompatible
selected records raise errors instead of silently shrinking the aggregate. Load the
records explicitly before aggregation; invalidation between collection and load is
still enforced by `load` and can reject a collected record. Collection and later
loads are separate operations with no multi-record snapshot or locking guarantee,
so coordinate writers and invalidation when a stable set matters.

The [partial aggregation example](https://github.com/kj-9/kazeflow/blob/main/examples/available_materializations.py)
keeps the producer's failed `RunResult`, collects selected committed outputs, and
uses ordinary Python to aggregate them. It writes rows and a deterministic receipt
containing the included materialization IDs to one local snapshot file using a
temporary file and atomic replacement. Repeating the same selection produces the
same publication ID. This example covers one local file; an external sink must
define its own transaction, idempotency, and receipt behavior.

### Storage and compatibility boundary

The adapter owns storage policy. `FileMaterialization` uses an explicit codec and a
versioned manifest; the [adapter reference](api/file-materialization.md) documents
its layout and failure behavior. A missing reference is a cache miss. A malformed
or incompatible reference, manifest, or payload raises `FileMaterializationError`
instead of silently rerunning the producer. The adapter verifies payload size and
SHA-256 before decoding. This detects accidental damage, but does not authenticate
a store whose manifest and payload can both be changed.

The supplied `JsonCodec` accepts strict JSON values and rejects tuples,
non-string mapping keys, arbitrary objects, cycles, and non-finite numbers. The
store contains raw application outputs and typed partition keys, including values
that may be private. Use a trusted codec and a trusted local root; a custom codec
is application code and is not sandboxed. JSON encoding is not encryption or
redaction.

Committed data is published by replacing a small reference to an immutable
generation. Atomic rename provides visibility on a local filesystem; power-loss
durability and network filesystem semantics are not guaranteed. Multiple writers
may race, and the last reference replacement wins. Previous generations are kept;
there is no artifact deletion or garbage collection. Explicit invalidation writes a
separate revocation ledger and leaves generations in place.

New references and manifests use schema version 2. Updated readers also accept
version 1 when no revocation ledger exists for that identity. Older package versions
cannot read version 2, so stop older writers and upgrade all processes before writing
new records. Copy the complete store root, including its `invalidations/` directory,
to preserve revocation. Restoring only old references and objects while retaining the
current ledger does not revive them. Deleting or restoring old revocation metadata,
including rolling back the whole root, is outside this guarantee. Synchronous storage
calls run in worker threads, so cancelling a flow cannot stop a call already in
progress; an atomic commit may finish and be available to a later process.

The adapter adds no third-party runtime dependency and is opt-in. It does not
provide cross-run exactly-once execution or an external-sink transaction guarantee.

The in-memory attempt result distinguishes `executed` output from `materialized`
output and carries its materialization ID. Portable result record v4, CLI run and
history documents, and SQLite rows preserve this successful provenance. The default
Python `RunResult.to_record()` projection remains v1; request v4 explicitly to include
it. Older projection versions keep their original shape. See [Results and history](results.md)
and the [trust boundary](concepts/trust-boundary.md) before persisting or sharing
diagnostic records.

The normal plan-first workflow remains: review the selected assets and partition
keys, then explicitly run the same selection. History inspection reports saved
resolution and materialization IDs without contacting the adapter or loading output.
Inspection never resumes a run or reconstructs raw keys or outputs. See the
[materialization resume example](https://github.com/kj-9/kazeflow/blob/main/examples/materialization_resume.py).
