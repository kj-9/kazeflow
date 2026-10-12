"""Explicit collection of committed outputs, independent of producer success."""

from dataclasses import replace

import pytest

from kazeflow import MaterializationCollection
from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.file_materialization import (
    FileMaterialization,
    FileMaterializationError,
    JsonCodec,
)
from kazeflow.flow import Flow
from kazeflow.materialization import (
    Materialization,
    MaterializationContext,
    call_materialization,
)
from kazeflow.partition import PartitionDef
from kazeflow.results import AttemptStatus, FlowStatus


def context(key: object = "key"):
    return MaterializationContext("produce", "1", "collection-test", True, key)


def test_collection_is_optional_and_no_empty_selection_io(tmp_path, monkeypatch):
    assert "available" not in Materialization.__dict__
    root = tmp_path / "store"
    store = FileMaterialization(root, codec=JsonCodec())
    monkeypatch.setattr(
        store, "resolve", lambda context: pytest.fail("no identity was selected")
    )
    assert store.available([]) == ()
    assert not root.exists()


@pytest.mark.asyncio
async def test_optional_asynchronous_collection_protocol():
    class Adapter:
        async def available(self, contexts):
            assert contexts == []
            return ()

    adapter: MaterializationCollection = Adapter()
    assert await call_materialization(adapter.available, []) == ()


def test_explicit_contexts_preserve_falsey_order_prior_records_and_skip_revocation(
    tmp_path,
):
    store = FileMaterialization(tmp_path / "store", codec=JsonCodec())
    identities = [context(0), context(False), context(""), context("past")]
    records = [
        store.commit(identity, index) for index, identity in enumerate(identities)
    ]
    store.invalidate(identities[1])
    selected = [
        identities[3],
        context("missing"),
        identities[1],
        identities[2],
        identities[0],
    ]
    result = store.available(selected)
    assert isinstance(result, tuple)
    assert [item.materialization_id for item in result] == [
        records[3].materialization_id,
        records[2].materialization_id,
        records[0].materialization_id,
    ]
    assert [store.load(record) for record in result] == [3, 2, 0]
    # Other version/input identities are never inferred from stored artifacts.
    assert store.available([replace(identities[0], version="2")]) == ()
    assert (
        store.available(
            [replace(identities[0], input_materialization_ids=(("input", "other-id"),))]
        )
        == ()
    )
    store.invalidate(identities[3])
    with pytest.raises(FileMaterializationError):
        store.load(result[0])


@pytest.mark.parametrize(
    "invalid", [None, "key", b"key", {"key"}, iter([]), [None], [object()]]
)
def test_invalid_selection_rejected_before_storage_reads(
    tmp_path, monkeypatch, invalid
):
    store = FileMaterialization(tmp_path / "store", codec=JsonCodec())
    monkeypatch.setattr(
        store,
        "resolve",
        lambda context: pytest.fail("invalid selection must not read storage"),
    )
    with pytest.raises((TypeError, ValueError)):
        store.available(invalid)
    assert not store.root.exists()


def test_duplicate_and_unsupported_late_context_fail_before_storage_reads(
    tmp_path, monkeypatch
):
    store = FileMaterialization(tmp_path / "store", codec=JsonCodec())
    monkeypatch.setattr(
        store,
        "resolve",
        lambda context: pytest.fail("validate whole selection before lookup"),
    )
    with pytest.raises(ValueError):
        store.available([context(), context()])
    with pytest.raises(FileMaterializationError):
        store.available([context(), context(object())])
    assert not store.root.exists()


def test_collection_is_read_only_and_does_not_decode(tmp_path):
    class CountingCodec(JsonCodec):
        def __init__(self):
            self.calls = []

        def encode(self, value):
            self.calls.append("encode")
            return super().encode(value)

        def decode(self, payload):
            self.calls.append("decode")
            return super().decode(payload)

    codec = CountingCodec()
    store = FileMaterialization(tmp_path / "store", codec=codec)
    record = store.commit(context(), {"answer": 42})
    codec.calls.clear()
    before = {
        p.relative_to(store.root): p.read_bytes()
        for p in store.root.rglob("*")
        if p.is_file()
    }
    assert store.available([context()]) == (record,)
    assert codec.calls == []
    assert before == {
        p.relative_to(store.root): p.read_bytes()
        for p in store.root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("damage", ["payload", "codec"])
def test_bad_candidate_raises_instead_of_silent_partial_aggregate(tmp_path, damage):
    store = FileMaterialization(tmp_path / "store", codec=JsonCodec())
    valid = store.commit(context("valid"), 41)
    record = store.commit(context("damaged"), 42)
    if damage == "payload":
        (
            store.root / "objects" / record.materialization_id / "payload.bin"
        ).write_bytes(b"bad")
    else:

        class OtherCodec(JsonCodec):
            codec_id = "other.codec"

        store = FileMaterialization(store.root, codec=OtherCodec())
    with pytest.raises(FileMaterializationError):
        store.available([context("valid"), context("damaged")])
    assert valid.committed


class Keys(PartitionDef):
    @property
    def domain(self):
        return "collection-test"

    def range(self, start, end):
        raise AssertionError("not used")


@pytest.mark.asyncio
async def test_failed_run_can_be_explicitly_collected_without_unblocking_dependencies(
    tmp_path,
):
    store = FileMaterialization(tmp_path / "store", codec=JsonCodec())
    past = store.commit(context("past"), 99)
    registry = AssetRegistry()
    calls = []

    def produce(context: AssetContext):
        calls.append(context.partition_key)
        if context.partition_key == "failed":
            raise RuntimeError("failed producer")
        return 42

    def aggregate(produce):
        pytest.fail("normal aggregate still blocks when any producer failed")

    registry.register(produce, partition_def=Keys(), materialization=store, version="1")
    registry.register(aggregate)
    result = await Flow(["aggregate"], registry=registry).run_async(
        {"partition_keys": ["ok", "failed"], "max_concurrency": 1}
    )
    assert result.status is FlowStatus.FAILED
    assert result.tasks[-1].status is AttemptStatus.SKIPPED
    before = result.to_record(schema_version=4)
    records = store.available([context("past"), context("ok"), context("failed")])
    assert records[0] == past
    assert [store.load(record) for record in records] == [99, 42]
    assert calls == ["ok", "failed"]
    assert result.status is FlowStatus.FAILED
    assert result.to_record(schema_version=4) == before
