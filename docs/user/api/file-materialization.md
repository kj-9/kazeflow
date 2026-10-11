# File materialization adapter

`kazeflow.file_materialization` provides an opt-in, local filesystem adapter for
the core materialization protocol. It uses only the Python standard library and
does not create a store until a commit is requested.

## Public API

```python
from kazeflow.file_materialization import (
    FileMaterialization,
    FileMaterializationError,
    JsonCodec,
    MaterializationCodec,
)
```

`FileMaterialization(root: str | Path, *, codec: MaterializationCodec)` implements
the synchronous `resolve(context)`, `load(record)`, and `commit(context, output)`
operations. Supply a codec explicitly; the adapter never chooses a serializer for
you. A codec exposes a stable string `codec_id`, `encode(output) -> bytes`, and
`decode(payload: bytes) -> Any` methods. The executor runs synchronous adapter
operations in worker threads.

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

The root contains two directories:

```text
<root>/
  refs/<identity-sha256>.json
  objects/<materialization-id>/
    manifest.json
    payload.bin
```

Each reference contains schema version `1` and a 32-character lowercase
hexadecimal materialization ID. The immutable generation manifest contains schema
version, ID, the full canonical identity, codec ID, payload byte length, and
payload SHA-256. Resolve and load validate these fields and the payload digest
before returning a record or decoding output. SHA-256 detects accidental damage;
it does not authenticate a store if an attacker can change both manifest and
payload.

Only a missing reference is a miss. An existing but malformed or dangling
reference, unsupported manifest, wrong identity or codec, or size/digest mismatch
raises `FileMaterializationError`. Codec errors retain their original exception.

## Publication and lifecycle

Commit encodes the value, writes a new immutable generation, and atomically
replaces the identity reference only after the generation is complete. A reader
therefore sees the previous committed generation or the new one. This is an
atomic-visibility guarantee for local filesystem rename; it does not promise
recovery after host power loss or reliable semantics on a network filesystem.

Writers do not lock. If multiple writers commit the same identity, the last
reference replacement wins. Each generation is retained, including generations
that lose the race or are orphaned by an interrupted commit. The adapter does not
enumerate, invalidate, or garbage collect materializations. Manage store growth and
concurrent writers at the application level.

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
