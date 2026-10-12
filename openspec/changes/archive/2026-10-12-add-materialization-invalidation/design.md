## Context

File storage uses refs keyed by canonical exact identity and immutable objects by UUID. Deleting refs loses revocation intent. See proposal for scope.

## Goals / Non-Goals

Preserve exact identity revocation without deleting artifacts or adding executor lifecycle state. Keep old adapters valid. No cascade, GC, automatic resume, CLI/version changes or cross-process locks.

## Decisions

- Add exported MaterializationInvalidation protocol in materialization.py with invalidate(context, *, reason: str="explicit") -> None | Awaitable[None]. Existing Materialization unchanged.
- FileMaterialization.invalidate(context, *, reason="explicit") -> None validates context/key and reason before writes. It atomically publishes root/invalidations/<identity SHA256>.json: schema_version1, identity, epoch (uuid4 hex), reason. Repeated calls publish fresh epochs; no deletion/clearing. Persistent ledger is codec-independent, exact identity includes version/input IDs. Reason is application metadata, no automatic redaction.
- New reference schema2 adds epoch (None or UUID hex), new manifest schema2 adds same epoch. Legacy refs/manifests1 retain existing fields. Each component version is validated independently; reference and manifest epochs must agree. Commit reads ledger epoch and includes it in both documents. Missing ledger -> epochNone. Any malformed ledger fails before reuse/publication; never infer absence. Do not interpret a missing output as human intent.
- Resolve parses/validates ledger and ref, compares epoch before reading payload/generation: stale ref gives None. load checks manifest epoch versus ledger before returning decoded output; stale record raises FileMaterializationError(reason="invalidated"). Old records remain loadable after ordinary commit within same epoch. No cleanup of generations.
- FileMaterializationError gains keyword reason default "corrupt", preserving Exception message/args. Unsupported ref/manifest/ledger schemas and codec mismatch use "incompatible"; stale record uses "invalidated". Input validation still raises normally.
- FileMaterializationInspection is frozen slots dataclass in file_materialization.py: status:str, record:MaterializationRecord|None=None, reason:str|None=None; valid statuses available/missing/invalidated/incompatible/corrupt. inspect(context) returns diagnosis using internal read path shared with resolve, no decoding/writes; raises invalid context. An absent ref with ledger is invalidated, without ledger missing. Only available carries record. Preserve original diagnostic messages for error cases; invalidated returns ledger reason when present.
- Atomic ledger publication uses synced temp file + os.replace, temporary cleanup on failure. Existing commit atomic publication retained. Invalidation requires quiescent identity users: no race/locking promises. Retaining latest ledger while copying old refs/objects preserves revocation, but deleting/rolling back ledger/full-root backups or hostile editing is outside guarantee. Power-loss durability is not promised.

## Ownership

Root: materialization.py, __init__.py, tests/test_materialization_invalidation.py, tests/test_public_api.py, OpenSpec/ROADMAP/final integration/commit.
Adapter agent: file_materialization.py and tests/test_file_materialization.py only.
Docs agent: docs/user/partitions.md, docs/user/guides/persistence.md, docs/user/concepts/trust-boundary.md, docs/user/api/file-materialization.md, docs/user/api/materialization.md, docs/release-notes.md, new examples/materialization_invalidation.py, scripts/smoke_wheel_install.py.

## Risks / Trade-offs

Older readers reject schema2 → document migration and stop older writers.
Partial restores lose intent → copy entire root incl ledger; explicitly limit full rollback guarantee.
Reason can leak application data → keep it caller-defined storage data; docs disclose privacy.
Concurrent invalidation → quiesce applications; avoid implying cross-run exactly-once.

## Migration Plan

Updated reader accepts legacy1 with no matching ledger; writes2 on new commits. No rewriting old records or changing record/event/CLI/store versions. Core remains stdlib-only and CI matrix3.10–3.13 retained.
