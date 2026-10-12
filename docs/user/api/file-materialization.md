# File materialization adapter

`kazeflow.file_materialization` provides an opt-in, local filesystem adapter for
the core materialization protocol. It uses only the Python standard library and
does not create a store until a commit is requested.

For the optional caller-driven invalidation contract, see the
[materialization protocols](materialization.md) reference.

## Public API

```python
from kazeflow.file_materialization import (
    FileMaterialization,
    FileMaterializationError,
    FileMaterializationInspection,
    JsonCodec,
    MaterializationCodec,
)
from kazeflow import MaterializationCollection, MaterializationInvalidation
```

`FileMaterialization(root: str | Path, *, codec: MaterializationCodec)` implements
the synchronous `resolve(context)`, `load(record)`, and `commit(context, output)`
operations. Supply a codec explicitly; the adapter never chooses a serializer for
you. A codec exposes a stable string `codec_id`, `encode(output) -> bytes`, and
`decode(payload: bytes) -> Any` methods. The executor runs synchronous adapter
operations in worker threads.

The adapter also implements the optional `MaterializationCollection` protocol with
`available(contexts) -> tuple[MaterializationRecord, ...]`. Supply a finite, explicit
non-text sequence of exact `MaterializationContext` identities. The adapter validates
all contexts and duplicate identities before it reads storage, and returns committed
records in selection order. An empty sequence returns immediately. It never scans the
root, reconstructs keys from run history, or adds prior-run outputs unless their
contexts were supplied explicitly.

Missing and invalidated identities are omitted, matching `resolve`. Corrupt,
unsupported, incompatible, or unreadable selected entries raise
`FileMaterializationError` rather than being silently excluded. The returned records
are metadata only; call `load(record)` to decode each output. A later invalidation is
checked again by `load`, and a multi-record collection followed by separate loads is
not one storage snapshot. Applications that need a stable set must coordinate
concurrent writers and invalidation.

`JsonCodec()` has codec ID `kazeflow.json.v1`. It accepts exact JSON built-in
values: `None`, booleans, integers, finite floats, strings, lists, and dictionaries
with string keys. It rejects tuples, subclasses and other arbitrary objects,
non-string dictionary keys, cyclic values, and non-finite floats. Decode also
rejects non-standard JSON constants. Use a custom codec when the output contract
needs another representation, and give it a versioned, stable codec ID.

Storage and manifest errors raise `FileMaterializationError`. A missing reference
for the requested identity returns `None`, meaning a cache miss. A broken reference,
missing generation file, unsupported schema or codec, or failed integrity check is
an error. A damaged store is not treated as a cache miss. Exceptions raised by a
codec's `encode` or `decode` methods retain their original type.

## Identity and manifest layout

The adapter hashes a canonical JSON identity containing the asset name, explicit
asset version, partition domain, partition-key presence and typed key, and ordered
direct input materialization IDs. Supported partition-key types are `None`,
`str`, `bool`, `int`, `float`, and an exact `datetime.date`; exact types are
preserved. Date keys use canonical ISO text. Floating-point keys use their
hexadecimal representation, so `0.0` and `-0.0` remain distinct.
`datetime.datetime` and custom key types are unsupported.

The root contains references, immutable generations, and—after an explicit
invalidation—a revocation ledger:

```text
<root>/
  refs/<identity-sha256>.json
  objects/<materialization-id>/
    manifest.json
    payload.bin
  invalidations/<identity-sha256>.json
```

The identity SHA-256 names the reference and ledger files; it is not a
materialization ID. Each committed generation gets a separate opaque 32-character
lowercase hexadecimal materialization ID. The payload SHA-256 detects accidental
damage and is not an identity or authenticity proof.

New references use schema version `2` with `materialization_id` and a nullable
revocation `epoch`. New immutable manifests use schema version `2` and include the
same epoch alongside the full canonical identity, codec ID, payload byte length,
and payload SHA-256. Resolve and load validate the identity, epoch, and payload
digest before returning a record or decoding output. The ledger has its own strict
schema version `1` and stores the exact identity, current epoch, and caller-provided
reason.

Updated readers also accept reference and manifest schema version `1` when no
revocation ledger exists for that identity. If a ledger exists, a legacy record has
no matching epoch and is treated as invalidated. New commits use the current ledger
epoch. Older kazeflow releases cannot read schema version `2`; stop old writers and
upgrade every process that writes to the root before publishing version `2` records.
Copy the whole root, including `invalidations/`, to preserve invalidation decisions.
Restoring old references and objects while retaining the current ledger does not
make those generations reusable. Deleting/restoring the ledger or rolling back the
entire root is outside this guarantee.

`resolve` returns `None` for a missing or invalidated identity. Corrupt storage and
incompatible schema or codec raise `FileMaterializationError`; they are not cache
misses. Codec errors retain their original exception. `load` rejects a record from
an earlier revocation epoch with `FileMaterializationError` whose `reason` is
`"invalidated"`.

## Inspect and invalidate

`inspect(context)` returns a frozen `FileMaterializationInspection` with `status`,
nullable `record`, and nullable diagnostic `reason`. Its statuses are:

| Status | Meaning | Record |
| --- | --- | --- |
| `available` | A compatible committed generation is present. | Committed record |
| `missing` | No reference or revocation exists for this identity. | `None` |
| `invalidated` | A revocation exists or the stored reference belongs to an older epoch. | `None` |
| `incompatible` | The schema or codec cannot be read by this adapter. | `None` |
| `corrupt` | Stored JSON, identity, reference, manifest, or payload integrity is invalid. | `None` |

Inspection does not write to storage, invoke an asset body, or call the codec's
decoder. Invalid context arguments still raise. It verifies the stored metadata and
payload integrity so an available record is safe to pass to `load`.

`invalidate(context, *, reason="explicit")` writes a fresh epoch for the complete
identity, even if no output exists or the identity was already invalidated. `reason`
must be a non-empty string. The epoch blocks old references and records but does not
delete generations; an explicit later commit in that epoch can be reused by future
runs. Invalidation applies only to that exact asset/version/domain/typed key and
ordered input IDs. It does not cascade to downstream identities or run automatically.

Callers must quiesce all users of the identity during invalidation. The adapter has
no cross-process lock or concurrent invalidation transaction. The reason is stored
as caller-controlled application metadata: do not put secrets or personal data in
it. See the [trust-boundary guide](../concepts/trust-boundary.md) for storage and
privacy limits.

## Publication and lifecycle

Commit encodes the value, writes a new immutable generation, and atomically
replaces the identity reference only after the generation is complete. A reader
therefore sees the previous committed generation or the new one. This is an
atomic-visibility guarantee for local filesystem rename; it does not promise
recovery after host power loss or reliable semantics on a network filesystem.

Writers do not lock. If multiple writers commit the same identity, the last
reference replacement wins. Each generation is retained, including generations
that lose the race or are orphaned by an interrupted commit. The adapter does not
delete or garbage collect materializations. Manage store growth and concurrent
writers at the application level.

The root and codec are trusted application inputs. Payloads and typed partition
keys are stored as raw application data. The adapter does not encrypt, redact, or
sandbox them. Keep the root private when outputs or keys are sensitive. A custom
codec can execute arbitrary application code.

The core invokes synchronous adapter methods in worker threads. Cancelling a flow
cannot stop an operation that has already started in a thread. A commit may finish
atomically after cancellation and be reusable by a later run.

## Use the adapter

See the [three-process example on GitHub](https://github.com/kj-9/kazeflow/blob/main/examples/file_materialization.py)
for a producer commit, a downstream failure, and a later process that reuses the
producer while retrying the failed downstream work.

See the [invalidation example on GitHub](https://github.com/kj-9/kazeflow/blob/main/examples/materialization_invalidation.py)
for a commit, explicit revocation, deliberate rebuild, and reuse of the new
generation.

See the [partial aggregation example on GitHub](https://github.com/kj-9/kazeflow/blob/main/examples/available_materializations.py)
for an explicit selection, partial availability after a producer failure, and a
single-file snapshot that stores aggregate rows with their publication receipt.
