# Materialization protocols

The core materialization contract allows explicitly selected adapters to resolve,
load, and commit outputs for partitioned assets. These methods do not add persistence
to the core or automatically reuse outputs unless an asset declares an adapter and
version. See [Materialization protocol](core.md#materialization-protocol) for the
`Materialization`, `MaterializationContext`, and `MaterializationRecord` definitions.

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
