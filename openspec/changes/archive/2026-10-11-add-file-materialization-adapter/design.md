## Context

See proposal.md for motivation. The executor already calls synchronous adapters in a thread, checks identity, and publishes downstream only after commit. No executor or root-export changes are needed.

## Goals / Non-Goals

**Goals:** an explicitly imported stdlib adapter with a readable versioned manifest, immutable generations, and fresh-process reuse.
**Non-Goals:** generic object serialization, custom partition key codecs, cleanup/invalidation, locking, remote filesystems, or changes to events/results/CLI/storage contracts.

## Decisions

- Public module `kazeflow.file_materialization`: `FileMaterialization(root: str | Path, *, codec: MaterializationCodec)`, protocol `codec_id: str`, `encode(output: Any) -> bytes`, `decode(payload: bytes) -> Any`, `JsonCodec` with codec_id `kazeflow.json.v1`, `FileMaterializationError`. All methods synchronous; executor handles threads. Explicit codec avoids implicit serialization. JSON codec recursively rejects tuples, non-string map keys, arbitrary classes, cycles, and non-finite floats rather than silently changing values; JSON decode rejects non-finite constants too.
- Canonical identity JSON includes exact typed partition key (`none`, `str`, `bool`, `int`, `float`, `date`) and ordered input IDs. Floats encoded with `float.hex()` to distinguish signed zero; constructor contexts already validate presence. Exact `datetime.date` uses canonical ISO format; `datetime.datetime` and subclasses are rejected. This supports the builtin DatePartitionDef normalized keys without a user key codec. Unsupported key types fail clearly. Hash identity with SHA256 for path addressing; never use asset/key strings as path segments. Codec is NOT included in lookup identity so wrong-codec stores report explicit errors.
- Root layout: `refs/<identity_sha256>.json` selects `{schema_version: 1, materialization_id: <uuid hex>}`; `objects/<uuid hex>/manifest.json` and `payload.bin`. Manifest fields: `schema_version: 1`, `materialization_id`, `identity` (canonical identity), `codec_id`, `payload_size`, `payload_sha256`. Paths are root-relative; IDs/digests and manifest types strictly validated. Root is trusted local application storage, not a sandbox or authenticated store. SHA256 detects accidental damage, not malicious changes to both payload and manifest.
- Commit encodes and validates first, creates a unique temporary generation in objects, writes and flushes/fsyncs payload and manifest, renames to final UUID directory, writes a unique temp reference in refs, then os.replace publishes it. Best-effort cleanup of unpublished temp paths must not mask original failures. Orphan completed generations after failure are acceptable; no GC in scope. No fallible operations after publication except return. Atomic rename visibility on local filesystems is the promise; host power-loss recovery and network FS semantics are not guaranteed.
- Resolve reads reference, manifest, and payload, checks schema/types/ID/identity/codec/size/digest, returns a record retaining the requested typed key. Missing reference alone is a miss; broken reference is error. Load uses immutable ID and compares manifest identity with supplied record, rechecks integrity before decoding. Missing root is a miss and never creates directories. Exceptions wrap storage/format failures in FileMaterializationError with cause; codec failures can retain their original exception as executor attempt failures.
- Multiple writers independently generate immutable records; last atomic reference replacement wins. No deduplication or exactly-once body guarantee. Existing records are never deleted on replacement. This avoids locking and multi-file in-place updates.
- Ownership: adapter agent owns only src/kazeflow/file_materialization.py; tests agent owns tests/test_file_materialization.py; docs agent owns docs/user/api/file-materialization.md, examples/file_materialization.py, selected guide/nav/release updates and scripts/smoke_wheel_install.py. Root owns OpenSpec, roadmap, review/integration. Existing dirty changes are preserved.

## Risks / Trade-offs

- Disk growth from immutable and orphan generations → document manual store lifecycle; future invalidation/GC is separate.
- Untrusted custom codecs can execute application code → explicit opt-in and trusted local root; no pickle supplied.
- Payload/key data is stored verbatim → explain privacy difference from portable RunResult records.
- Cancellation cannot stop an already running synchronous thread → no claim of rollback; a completed atomic commit may remain available on next run.

## Migration Plan

Additive API, manifest v1 with explicit schema rejection; no migration of existing run records. New users opt in through submodule import. Existing memory/custom adapters remain unchanged.
