# Materialization protocols

The core materialization contract allows explicitly selected adapters to resolve,
load, and commit outputs for partitioned assets. These methods do not add persistence
to the core or automatically reuse outputs unless an asset declares an adapter and
version. See [Materialization protocol](core.md#materialization-protocol) for the
`Materialization`, `MaterializationContext`, and `MaterializationRecord` definitions.

## Optional collection capability

`MaterializationCollection` is a separate optional protocol exported from `kazeflow`.
An adapter can implement it alongside `Materialization`:

```python
from collections.abc import Awaitable, Sequence

from kazeflow import (
    MaterializationCollection,
    MaterializationContext,
    MaterializationRecord,
)


class MyCollectionAdapter:
    def available(
        self, contexts: Sequence[MaterializationContext]
    ) -> tuple[MaterializationRecord, ...] | Awaitable[tuple[MaterializationRecord, ...]]:
        ...


collection: MaterializationCollection = MyCollectionAdapter()
```

The protocol is structural. Existing adapters that only implement `resolve`,
`load`, and `commit` remain valid. `available(contexts)` may be synchronous or
asynchronous and returns committed records in the order of the explicit selection,
omitting identities with no currently published record. It does not enumerate a
store, infer keys from history, or automatically include prior runs. The core does
not load, aggregate, publish, or change a run result during collection.

`FileMaterialization.available` validates the whole non-text sequence and rejects
duplicate exact identities before reading storage. It follows the same resolve
semantics as a single lookup: missing or invalidated entries are omitted, while
corrupt, unsupported, incompatible, or unreadable entries fail explicitly. Returned
records remain subject to integrity and revocation checks when later passed to
`load`; collection and load do not form a transaction or lock out concurrent writers.
See the [partial aggregation example](../partitions.md#collect-available-outputs-after-a-partial-failure)
for caller-owned aggregation after a failed run.

## Optional invalidation capability

`MaterializationInvalidation` is a separate optional protocol exported from
`kazeflow`. An adapter may implement it in addition to `Materialization`:

```python
from kazeflow import MaterializationContext, MaterializationInvalidation

class MyAdapter:
    def invalidate(
        self, context: MaterializationContext, *, reason: str = "explicit"
    ) -> None:
        ...

adapter: MaterializationInvalidation = MyAdapter()
```

The protocol is structural: an adapter does not need to inherit from it. The method
may be synchronous or asynchronous and resolves to `None`. Existing adapters that
only provide `resolve`, `load`, and `commit` remain valid; the executor never
calls invalidation automatically. It is a caller-directed operation for a complete
identity, including asset name, version, partition domain, typed key and presence,
and ordered direct input materialization IDs. An adapter owns how the revocation is
persisted. For the file adapter's persistent epoch ledger and reasoned diagnostics,
see [File materialization adapter](file-materialization.md#inspect-and-invalidate).

Callers must coordinate and quiesce users of the identity before invalidating it.
The protocol does not promise cross-process locking. Invalidation does not imply a
cascade to downstream identities, artifact deletion, garbage collection, run resume,
or a new run selection. Applications explicitly select work in a later run.
