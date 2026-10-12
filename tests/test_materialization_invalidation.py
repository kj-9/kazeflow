"""Independent evidence for persistent exact-identity materialization revocation."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import inspect
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from kazeflow import MaterializationInvalidation
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


def context(key: object = "key") -> MaterializationContext:
    return MaterializationContext("produce", "1", "keys", True, key)


def store(root: Path) -> FileMaterialization:
    return FileMaterialization(root, codec=JsonCodec())


def test_invalidation_is_separate_optional_capability():
    assert "invalidate" not in Materialization.__dict__
    signature = inspect.signature(MaterializationInvalidation.invalidate)
    assert signature.parameters["reason"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["reason"].default == "explicit"


@pytest.mark.asyncio
async def test_optional_async_invalidation_contract():
    called = []

    class Adapter:
        async def invalidate(self, identity, *, reason="explicit"):
            called.append((identity, reason))

    adapter: MaterializationInvalidation = Adapter()
    identity = context(0)
    await call_materialization(adapter.invalidate, identity)
    assert called == [(identity, "explicit")]


def test_exact_identity_isolation_and_repeated_invalidation(tmp_path):
    adapter = store(tmp_path / "store")
    base = context(0)
    identities = [
        base,
        context(False),
        context(""),
        replace(base, version="2"),
        replace(base, input_materialization_ids=(("source", "id"),)),
        replace(base, partition_domain="other"),
        replace(base, asset_name="other"),
    ]
    records = [
        adapter.commit(identity, index) for index, identity in enumerate(identities)
    ]
    adapter.invalidate(base, reason="operator decision")
    assert adapter.resolve(base) is None
    assert adapter.inspect(base).status == "invalidated"
    assert adapter.inspect(base).reason == "operator decision"
    with pytest.raises(FileMaterializationError) as error:
        adapter.load(records[0])
    assert isinstance(error.value, FileMaterializationError)
    assert error.value.reason == "invalidated"
    for index, (identity, record) in enumerate(
        zip(identities[1:], records[1:]), start=1
    ):
        assert adapter.resolve(identity) == record
        assert adapter.load(record) == index
    new = adapter.commit(base, "rebuilt")
    assert new.materialization_id != records[0].materialization_id
    assert adapter.inspect(base).status == "available"
    assert adapter.load(new) == "rebuilt"
    adapter.invalidate(base)
    with pytest.raises(FileMaterializationError):
        adapter.load(new)
    assert adapter.resolve(base) is None


def test_invalidate_missing_identity_persists_and_inspection_is_read_only(tmp_path):
    root = tmp_path / "store"
    adapter = store(root)
    assert adapter.inspect(context()).status == "missing"
    assert not root.exists()
    adapter.invalidate(context())
    before = {
        p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }
    inspection = adapter.inspect(context())
    assert inspection.status == "invalidated"
    assert inspection.record is None
    with pytest.raises(FrozenInstanceError):
        setattr(inspection, "status", "available")
    assert before == {
        p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }
    current = adapter.commit(context(), 42)
    assert adapter.resolve(context()) == current


@pytest.mark.parametrize("reason", [None, 0, ""])
def test_invalid_reason_does_not_create_storage(tmp_path, reason):
    root = tmp_path / "store"
    with pytest.raises((TypeError, ValueError)):
        store(root).invalidate(context(), reason=reason)
    assert not root.exists()


def test_stale_refs_and_objects_cannot_revive_in_copied_root_or_fresh_process(tmp_path):
    root = tmp_path / "store"
    backup = tmp_path / "backup"
    copied = tmp_path / "copied"
    adapter = store(root)
    old = adapter.commit(context(), {"old": 42})
    shutil.copytree(root, backup)
    adapter.invalidate(context(), reason="stale output")
    rebuilt = adapter.commit(context(), {"new": 43})
    assert adapter.resolve(context()) == rebuilt
    shutil.copytree(root, copied)
    # Restore only artifacts, preserving revocation metadata in the copied root.
    shutil.copytree(backup / "refs", copied / "refs", dirs_exist_ok=True)
    shutil.copytree(backup / "objects", copied / "objects", dirs_exist_ok=True)
    clone = store(copied)
    assert clone.resolve(context()) is None
    assert clone.inspect(context()).status == "invalidated"
    with pytest.raises(FileMaterializationError):
        clone.load(old)
    program = """
import sys
from kazeflow.file_materialization import FileMaterialization, JsonCodec
from kazeflow.materialization import MaterializationContext
adapter = FileMaterialization(sys.argv[1], codec=JsonCodec())
identity = MaterializationContext("produce", "1", "keys", True, "key")
assert adapter.resolve(identity) is None
assert adapter.inspect(identity).status == "invalidated"
new = adapter.commit(identity, {"fresh": 44})
assert adapter.resolve(identity) == new
assert adapter.load(new) == {"fresh": 44}
"""
    subprocess.run([sys.executable, "-c", program, str(copied)], check=True)
    current = clone.resolve(context())
    assert current is not None
    assert current.materialization_id != old.materialization_id


def test_failed_atomic_invalidation_preserves_prior_reuse(tmp_path, monkeypatch):
    import kazeflow.file_materialization as module

    root = tmp_path / "store"
    adapter = store(root)
    previous = adapter.commit(context(), "original")
    original_replace = module.os.replace

    def fail_ledger(source, destination):
        if Path(destination).parent.name == "invalidations":
            raise OSError("ledger publish failed")
        return original_replace(source, destination)

    monkeypatch.setattr(module.os, "replace", fail_ledger)
    with pytest.raises(FileMaterializationError):
        adapter.invalidate(context())
    assert adapter.resolve(context()) == previous
    assert adapter.load(previous) == "original"
    assert not any(p.name.startswith(".") for p in root.rglob("*") if p.is_file())


def test_corrupt_ledger_fails_closed_on_resolve_load_and_commit(tmp_path):
    root = tmp_path / "store"
    adapter = store(root)
    old = adapter.commit(context(), 42)
    adapter.invalidate(context())
    ledger = next((root / "invalidations").glob("*.json"))
    ledger.write_text("{broken")
    for operation in (
        lambda: adapter.resolve(context()),
        lambda: adapter.load(old),
        lambda: adapter.commit(context(), 43),
    ):
        with pytest.raises(FileMaterializationError):
            operation()
    inspection = adapter.inspect(context())
    assert inspection.status == "corrupt"
    assert inspection.record is None
    assert inspection.reason


class Keys(PartitionDef):
    @property
    def domain(self):
        return "keys"

    def range(self, start, end):
        raise AssertionError("not used")


@pytest.mark.asyncio
async def test_live_flow_rebuilds_invalidated_upstream_and_input_identity(tmp_path):
    registry = AssetRegistry()
    upstream = store(tmp_path / "upstream")
    downstream = store(tmp_path / "downstream")
    calls = []

    def produce(context: AssetContext):
        calls.append("produce")
        return 42

    def consume(produce, context: AssetContext):
        calls.append("consume")
        return produce[context.partition_key] + 1

    registry.register(
        produce, partition_def=Keys(), materialization=upstream, version="1"
    )
    registry.register(
        consume, partition_def=Keys(), materialization=downstream, version="1"
    )
    flow = Flow(["consume"], registry=registry)
    first = await flow.run_async({"partition_keys": [0]})
    await flow.run_async({"partition_keys": [0]})
    assert calls == ["produce", "consume"]
    upstream.invalidate(context(0))
    third = await flow.run_async({"partition_keys": [0]})
    assert calls == ["produce", "consume", "produce", "consume"]
    assert (
        third.tasks[0].attempts[0].materialization_id
        != first.tasks[0].attempts[0].materialization_id
    )
    assert all(task.attempts[0].resolution == "executed" for task in third.tasks)


def test_unreadable_invalidation_parent_cannot_act_as_absent_ledger(tmp_path):
    root = tmp_path / "store"
    adapter = store(root)
    old = adapter.commit(context(), 42)
    (root / "invalidations").symlink_to(
        root / "missing-ledger-directory", target_is_directory=True
    )
    for operation in (
        lambda: adapter.resolve(context()),
        lambda: adapter.load(old),
        lambda: adapter.commit(context(), 43),
        lambda: adapter.invalidate(context()),
    ):
        with pytest.raises(FileMaterializationError):
            operation()
    assert adapter.inspect(context()).status == "corrupt"


@pytest.mark.parametrize("args", [(), ("message",), ("message", 42)])
def test_error_reason_preserves_existing_exception_arguments(args):
    error = FileMaterializationError(*args, reason="incompatible")
    assert error.args == args
    assert error.reason == "incompatible"
