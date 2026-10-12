"""Behavioral evidence for the explicit local file materialization adapter."""

from __future__ import annotations

import concurrent.futures
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap
from datetime import date, datetime

import pytest

from kazeflow.file_materialization import (
    FileMaterialization,
    FileMaterializationError,
    JsonCodec,
)
from kazeflow.materialization import MaterializationContext


def context(**changes: object) -> MaterializationContext:
    values: dict[str, object] = {
        "asset_name": "transform",
        "version": "v1",
        "partition_domain": "tenant",
        "partition_key_present": True,
        "partition_key": "acme",
        "input_materialization_ids": (("source", "source-id"),),
    }
    values.update(changes)
    return MaterializationContext(**values)  # type: ignore[arg-type]


def adapter(root: Path, *, codec: object | None = None) -> FileMaterialization:
    return FileMaterialization(root, codec=codec or JsonCodec())  # type: ignore[arg-type]


def refs(root: Path) -> list[Path]:
    return sorted((root / "refs").glob("*.json"))


def generation(root: Path, materialization_id: str) -> Path:
    return root / "objects" / materialization_id


def test_construction_and_plan_do_not_write_or_encode(tmp_path: Path) -> None:
    from kazeflow.assets import AssetRegistry
    from kazeflow.partition import PartitionDef
    from kazeflow.plan import PlanConfig, build_flow_plan

    class Keys(PartitionDef):
        def range(self, start, end):
            raise AssertionError("not used")

    class CountingCodec(JsonCodec):
        def __init__(self):
            self.encodes = 0

        def encode(self, output):
            self.encodes += 1
            return super().encode(output)

    root = tmp_path / "store"
    codec = CountingCodec()
    store = FileMaterialization(root, codec=codec)
    registry = AssetRegistry()

    def produce(context):
        raise AssertionError("planning must not execute assets")

    registry.register(
        produce,
        partition_def=Keys(),
        materialization=store,
        version="v1",
    )
    plan = build_flow_plan(
        ["produce"],
        config=PlanConfig(partition_keys=["acme"]),
        registry=registry,
    )

    assert plan.tasks[0].materialization_enabled
    assert not root.exists()
    assert codec.encodes == 0


@pytest.mark.parametrize("key", [0, False, ""])
def test_falsey_typed_partition_keys_have_distinct_records(
    tmp_path: Path, key: object
) -> None:
    store = adapter(tmp_path / "store")
    ctx = context(partition_key=key)
    record = store.commit(ctx, {"key_type": type(key).__name__, "key": key})

    assert store.resolve(ctx) == record
    assert store.load(record) == {"key_type": type(key).__name__, "key": key}


def test_equal_falsey_keys_of_different_types_do_not_alias(tmp_path: Path) -> None:
    store = adapter(tmp_path / "store")
    int_context = context(partition_key=0)
    bool_context = context(partition_key=False)
    int_record = store.commit(int_context, "int")
    bool_record = store.commit(bool_context, "bool")

    assert int_record.materialization_id != bool_record.materialization_id
    assert store.resolve(int_context) == int_record
    assert store.resolve(bool_context) == bool_record


def test_available_returns_only_explicit_committed_records_in_selection_order(
    tmp_path: Path,
) -> None:
    store = adapter(tmp_path / "store")
    contexts = (
        context(asset_name="first", partition_key=0),
        context(asset_name="missing", partition_key=False),
        context(asset_name="last", partition_key=""),
    )
    first = store.commit(contexts[0], {"order": 1})
    last = store.commit(contexts[2], {"order": 3})
    resolved: list[MaterializationContext] = []
    resolve = store.resolve

    def track(selected: MaterializationContext):
        resolved.append(selected)
        return resolve(selected)

    store.resolve = track  # type: ignore[method-assign]

    assert store.available(contexts) == (first, last)
    assert resolved == list(contexts)


@pytest.mark.parametrize("selection", ["text", b"bytes", bytearray(b"bytes"), iter(())])
def test_available_rejects_non_sequence_selection_without_storage_io(
    tmp_path: Path, selection: object
) -> None:
    store = adapter(tmp_path / "store")

    with pytest.raises(TypeError, match="non-text sequence"):
        store.available(selection)  # type: ignore[arg-type]

    assert not store.root.exists()


def test_available_validates_all_contexts_before_storage_lookup(tmp_path: Path) -> None:
    store = adapter(tmp_path / "store")
    calls: list[MaterializationContext] = []

    def track(selected: MaterializationContext):
        calls.append(selected)
        return None

    store.resolve = track  # type: ignore[method-assign]
    malformed = context(partition_key=object())

    with pytest.raises(FileMaterializationError):
        store.available((context(asset_name="valid"), malformed))

    assert calls == []


def test_available_rejects_duplicate_identity_before_storage_lookup(
    tmp_path: Path,
) -> None:
    store = adapter(tmp_path / "store")
    calls: list[MaterializationContext] = []

    def track(selected: MaterializationContext):
        calls.append(selected)
        return None

    store.resolve = track  # type: ignore[method-assign]
    selected = context(partition_key=0)

    with pytest.raises(ValueError, match="duplicate exact identity"):
        store.available((selected, context(partition_key=0)))

    assert calls == []
    assert not store.root.exists()


def test_available_preserves_typed_falsey_identity_distinctions(
    tmp_path: Path,
) -> None:
    store = adapter(tmp_path / "store")
    int_context = context(partition_key=0)
    bool_context = context(partition_key=False)
    int_record = store.commit(int_context, "int")
    bool_record = store.commit(bool_context, "bool")

    assert store.available((bool_context, int_context)) == (bool_record, int_record)


def test_available_empty_selection_does_not_touch_storage(tmp_path: Path) -> None:
    store = adapter(tmp_path / "store")

    assert store.available(()) == ()
    assert not store.root.exists()


def test_available_propagates_corrupt_candidate_errors(tmp_path: Path) -> None:
    store = adapter(tmp_path / "store")
    selected = context()
    store.commit(selected, "output")
    reference_path = refs(store.root)[0]
    reference = json.loads(reference_path.read_text(encoding="utf-8"))
    reference["schema_version"] = 999
    reference_path.write_text(json.dumps(reference), encoding="utf-8")

    with pytest.raises(FileMaterializationError) as error:
        store.available((selected,))

    assert isinstance(error.value, FileMaterializationError)
    assert error.value.reason == "incompatible"


def test_date_partition_keys_round_trip_without_aliasing_iso_strings(
    tmp_path: Path,
) -> None:
    store = adapter(tmp_path / "store")
    day = date(2026, 10, 11)
    date_context = context(partition_key=day)
    text_context = context(partition_key=day.isoformat())
    date_record = store.commit(date_context, "date")
    text_record = store.commit(text_context, "text")

    assert store.resolve(date_context) == date_record
    assert store.load(date_record) == "date"
    assert store.resolve(text_context) == text_record
    assert date_record.materialization_id != text_record.materialization_id


@pytest.mark.parametrize(
    "key", [datetime(2026, 10, 11), type("DateChild", (date,), {})(2026, 10, 11)]
)
def test_datetime_and_date_subclasses_are_not_supported_keys(
    tmp_path: Path, key: object
) -> None:
    with pytest.raises(FileMaterializationError):
        adapter(tmp_path / "store").commit(context(partition_key=key), "value")


@pytest.mark.parametrize(
    "value",
    [
        (1, 2),
        {1: "non-string key"},
        float("nan"),
        float("inf"),
        object(),
    ],
)
def test_json_codec_rejects_lossy_or_non_json_values_without_publication(
    tmp_path: Path, value: object
) -> None:
    root = tmp_path / "store"
    store = adapter(root)

    with pytest.raises((TypeError, ValueError)):
        store.commit(context(), value)

    assert refs(root) == []
    assert store.resolve(context()) is None


def test_json_codec_rejects_cycles_without_publication(tmp_path: Path) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    cycle: list[object] = []
    cycle.append(cycle)

    with pytest.raises((TypeError, ValueError)):
        store.commit(context(), cycle)

    assert refs(root) == []
    assert store.resolve(context()) is None


@pytest.mark.parametrize("payload", [b"NaN", b'{"x": Infinity}', b'{"x":1,"x":2}'])
def test_json_codec_decode_rejects_nonstandard_or_ambiguous_json(
    payload: bytes,
) -> None:
    with pytest.raises(ValueError):
        JsonCodec().decode(payload)


def test_unsupported_partition_key_fails_without_publishing(tmp_path: Path) -> None:
    root = tmp_path / "store"
    store = adapter(root)

    with pytest.raises((TypeError, ValueError, FileMaterializationError)):
        store.commit(context(partition_key=[]), "output")

    assert refs(root) == []


def test_copied_root_is_portable_to_a_fresh_process(tmp_path: Path) -> None:
    original = tmp_path / "original"
    copied = tmp_path / "copied"
    store = adapter(original)
    record = store.commit(context(), {"answer": 42})
    shutil.copytree(original, copied)

    script = textwrap.dedent(
        """
        import json
        import sys
        from pathlib import Path
        from kazeflow.file_materialization import FileMaterialization, JsonCodec
        from kazeflow.materialization import MaterializationContext
        root = Path(sys.argv[1])
        ctx = MaterializationContext("transform", "v1", "tenant", True, "acme", (("source", "source-id"),))
        store = FileMaterialization(root, codec=JsonCodec())
        record = store.resolve(ctx)
        print(json.dumps({"record": record.materialization_id, "value": store.load(record)}))
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(copied)],
        check=True,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parents[1],
    )

    assert json.loads(completed.stdout) == {
        "record": record.materialization_id,
        "value": {"answer": 42},
    }


def test_new_files_use_epoch_schema_and_inspection_is_available(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    ctx = context()
    record = store.commit(ctx, {"answer": 42})
    reference = json.loads(refs(root)[0].read_text(encoding="utf-8"))
    manifest_path = generation(root, record.materialization_id) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert reference == {
        "schema_version": 2,
        "materialization_id": record.materialization_id,
        "epoch": None,
    }
    assert manifest["schema_version"] == 2
    assert manifest["epoch"] is None
    inspection = store.inspect(ctx)
    assert inspection.status == "available"
    assert inspection.record == record
    assert inspection.reason is None


def test_legacy_reference_and_manifest_remain_readable(tmp_path: Path) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    ctx = context()
    record = store.commit(ctx, {"legacy": True})
    reference_path = refs(root)[0]
    reference = json.loads(reference_path.read_text(encoding="utf-8"))
    reference.pop("epoch")
    reference["schema_version"] = 1
    reference_path.write_text(json.dumps(reference), encoding="utf-8")
    manifest_path = generation(root, record.materialization_id) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("epoch")
    manifest["schema_version"] = 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    assert store.resolve(ctx) == record
    assert store.load(record) == {"legacy": True}
    assert store.inspect(ctx).status == "available"


def test_null_epoch_ledger_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    ctx = context()
    record = store.commit(ctx, "previous")
    ledger_dir = root / "invalidations"
    ledger_dir.mkdir()
    digest = Path(refs(root)[0]).stem
    (ledger_dir / f"{digest}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "identity": {
                    "asset_name": "transform",
                    "version": "v1",
                    "partition_domain": "tenant",
                    "partition_key_present": True,
                    "partition_key": {"type": "str", "value": "acme"},
                    "input_materialization_ids": [["source", "source-id"]],
                },
                "epoch": None,
                "reason": "manual",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FileMaterializationError) as error:
        store.resolve(ctx)
    assert isinstance(error.value, FileMaterializationError)
    assert error.value.reason == "corrupt"
    with pytest.raises(FileMaterializationError) as load_error:
        store.load(record)
    assert isinstance(load_error.value, FileMaterializationError)
    assert load_error.value.reason == "corrupt"
    assert store.inspect(ctx).status == "corrupt"


def test_dangling_ledger_and_reference_are_corrupt_not_missing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    ctx = context()
    store.commit(ctx, "output")
    ledger_dir = root / "invalidations"
    ledger_dir.mkdir()
    ref_path = refs(root)[0]
    ledger_path = ledger_dir / f"{ref_path.stem}.json"
    ledger_path.symlink_to(ledger_dir / "absent-ledger.json")

    with pytest.raises(FileMaterializationError):
        store.resolve(ctx)
    assert store.inspect(ctx).status == "corrupt"

    ledger_path.unlink()
    ref_path.unlink()
    ref_path.symlink_to(root / "refs" / "absent-reference.json")
    with pytest.raises(FileMaterializationError):
        store.resolve(ctx)
    assert store.inspect(ctx).status == "corrupt"


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", "v2"),
        ("partition_domain", "other-domain"),
        ("partition_key", "other-key"),
        ("input_materialization_ids", (("source", "other-id"),)),
    ],
)
def test_identity_changes_resolve_as_misses(
    tmp_path: Path, field: str, value: object
) -> None:
    store = adapter(tmp_path / "store")
    store.commit(context(), "output")

    assert store.resolve(context(**{field: value})) is None


class AlternateCodec:
    codec_id = "test.alternate.v1"

    def encode(self, output: object) -> bytes:
        return json.dumps(output).encode("utf-8")

    def decode(self, payload: bytes) -> object:
        return json.loads(payload)


class BrokenDecoderCodec(AlternateCodec):
    codec_id = "test.broken-decoder.v1"

    def decode(self, payload: bytes) -> object:
        raise RuntimeError("decode failed")


class NonBytesCodec(AlternateCodec):
    codec_id = "test.non-bytes.v1"

    def encode(self, output: object) -> bytes:
        return "not bytes"  # type: ignore[return-value]


def test_codec_mismatch_is_an_explicit_error(tmp_path: Path) -> None:
    root = tmp_path / "store"
    adapter(root).commit(context(), "output")

    with pytest.raises(FileMaterializationError):
        adapter(root, codec=AlternateCodec()).resolve(context())


def test_codec_decode_failure_is_preserved_and_non_bytes_encoding_is_rejected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    broken_decoder = adapter(root, codec=BrokenDecoderCodec())
    record = broken_decoder.commit(context(), "value")
    with pytest.raises(RuntimeError, match="decode failed"):
        broken_decoder.load(record)

    other_root = tmp_path / "non-bytes"
    with pytest.raises(TypeError, match="must return bytes"):
        adapter(other_root, codec=NonBytesCodec()).commit(context(), "value")
    assert refs(other_root) == []


@pytest.mark.parametrize(
    "damage",
    ["reference", "reference-version", "manifest", "payload", "missing-payload"],
)
def test_corruption_is_reported_instead_of_returning_output(
    tmp_path: Path, damage: str
) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    record = store.commit(context(), {"ok": True})
    ref_path = refs(root)[0]
    if damage == "reference":
        ref_path.write_text("{broken", encoding="utf-8")
    elif damage == "reference-version":
        reference = json.loads(ref_path.read_text(encoding="utf-8"))
        reference["schema_version"] = 999
        ref_path.write_text(json.dumps(reference), encoding="utf-8")
    else:
        object_dir = generation(root, record.materialization_id)
        manifest_path = object_dir / "manifest.json"
        payload_path = object_dir / "payload.bin"
        if damage == "manifest":
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["schema_version"] = 999
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        elif damage == "payload":
            payload_path.write_bytes(b"corrupt")
        else:
            payload_path.unlink()

    with pytest.raises(FileMaterializationError):
        store.resolve(context())


def test_invalid_reference_manifest_types_and_incomplete_generation_fail(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    record = store.commit(context(), "output")
    object_dir = generation(root, record.materialization_id)
    manifest_path = object_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["payload_size"] = "large"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(FileMaterializationError):
        store.resolve(context())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda identity: identity.__setitem__("partition_key_present", 1),
        lambda identity: identity["partition_key"].__setitem__("type", "bool"),
        lambda identity: identity["partition_key"].__setitem__("value", 0),
    ],
)
def test_manifest_rejects_typed_identity_corruption(tmp_path: Path, mutate) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    store.commit(context(), "output")
    manifest_path = (
        generation(root, json.loads(refs(root)[0].read_text())["materialization_id"])
        / "manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate(manifest["identity"])
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(FileMaterializationError):
        store.resolve(context())


def test_old_committed_record_remains_loadable_after_replacement(
    tmp_path: Path,
) -> None:
    store = adapter(tmp_path / "store")
    ctx = context()
    old_record = store.commit(ctx, {"generation": 1})
    new_record = store.commit(ctx, {"generation": 2})

    assert old_record.materialization_id != new_record.materialization_id
    assert store.resolve(ctx) == new_record
    assert store.load(old_record) == {"generation": 1}
    assert store.load(new_record) == {"generation": 2}


@pytest.mark.parametrize(
    "field,value", [("committed", 1), ("materialization_id", "../outside")]
)
def test_load_rejects_malformed_record_metadata(
    tmp_path: Path, field: str, value: object
) -> None:
    store = adapter(tmp_path / "store")
    record = store.commit(context(), "output")
    object.__setattr__(record, field, value)

    with pytest.raises(FileMaterializationError):
        store.load(record)


def test_failed_reference_publication_preserves_previous_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kazeflow.file_materialization as file_materialization

    root = tmp_path / "store"
    store = adapter(root)
    ctx = context()
    previous = store.commit(ctx, "previous")
    real_replace = os.replace

    def fail_reference_replace(source, destination):
        if Path(destination).parent.name == "refs":
            raise OSError("injected reference publication failure")
        return real_replace(source, destination)

    monkeypatch.setattr(file_materialization.os, "replace", fail_reference_replace)
    with pytest.raises(FileMaterializationError):
        store.commit(ctx, "unpublished")

    monkeypatch.setattr(file_materialization.os, "replace", real_replace)
    assert store.resolve(ctx) == previous
    assert store.load(previous) == "previous"


def test_failed_generation_write_preserves_previous_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kazeflow.file_materialization as file_materialization

    root = tmp_path / "store"
    store = adapter(root)
    ctx = context()
    previous = store.commit(ctx, "previous")
    real_write = file_materialization._write_synced

    def fail_manifest_write(path, data, *, existing=False):
        if Path(path).name == "manifest.json":
            raise OSError("injected generation write failure")
        return real_write(path, data, existing=existing)

    monkeypatch.setattr(file_materialization, "_write_synced", fail_manifest_write)
    with pytest.raises(FileMaterializationError):
        store.commit(ctx, "unpublished")

    assert store.resolve(ctx) == previous
    assert store.load(previous) == "previous"


def test_failure_before_first_publication_leaves_a_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kazeflow.file_materialization as file_materialization

    root = tmp_path / "store"
    store = adapter(root)
    real_replace = os.replace

    def fail_reference_replace(source, destination):
        if Path(destination).parent.name == "refs":
            raise OSError("injected reference publication failure")
        return real_replace(source, destination)

    monkeypatch.setattr(file_materialization.os, "replace", fail_reference_replace)
    with pytest.raises(FileMaterializationError):
        store.commit(context(), "unpublished")

    monkeypatch.setattr(file_materialization.os, "replace", real_replace)
    assert store.resolve(context()) is None


def test_concurrent_commits_and_readers_only_observe_complete_records(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    store = adapter(root)
    ctx = context()
    store.commit(ctx, {"writer": "initial", "items": list(range(10))})

    def write(index: int):
        record = store.commit(ctx, {"writer": index, "items": list(range(10))})
        value = store.load(record)
        assert value == {"writer": index, "items": list(range(10))}
        return record

    def read_many() -> None:
        for _ in range(40):
            record = store.resolve(ctx)
            assert record is not None
            value = store.load(record)
            assert isinstance(value, dict)
            assert len(value["items"]) == 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        writers = [pool.submit(write, i) for i in range(8)]
        readers = [pool.submit(read_many) for _ in range(4)]
        records = [future.result() for future in writers]
        for future in readers:
            future.result()

    assert len({record.materialization_id for record in records}) == len(records)
    selected = store.resolve(ctx)
    assert selected is not None
    assert store.load(selected)["writer"] in set(range(8))


def test_fresh_process_reuses_upstream_after_downstream_failure(tmp_path: Path) -> None:
    root = tmp_path / "store"
    script = textwrap.dedent(
        """
        import asyncio
        import json
        import sys
        from pathlib import Path
        from kazeflow.assets import AssetRegistry
        from kazeflow.file_materialization import FileMaterialization, JsonCodec
        from kazeflow.flow import Flow
        from kazeflow.partition import DatePartitionDef
        async def main():
            root = Path(sys.argv[1])
            mode = sys.argv[2]
            calls = []
            registry = AssetRegistry()
            upstream_store = FileMaterialization(root, codec=JsonCodec())
            async def upstream(context):
                calls.append("upstream")
                return {"value": "saved"}
            async def downstream(upstream):
                calls.append("downstream")
                if mode == "fail":
                    raise RuntimeError("retry me")
                return next(iter(upstream.values()))["value"]
            registry.register(upstream, partition_def=DatePartitionDef(), materialization=upstream_store, version="v1")
            registry.register(downstream, partition_def=DatePartitionDef())
            result = await Flow(["downstream"], registry=registry).run_async({"partition_keys": ["2026-10-11"]})
            print(json.dumps({"status": result.status.value, "calls": calls}))
        asyncio.run(main())
        """
    )

    def run(mode: str) -> dict[str, object]:
        completed = subprocess.run(
            [sys.executable, "-c", script, str(root), mode],
            check=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[1],
        )
        return json.loads(completed.stdout)

    first = run("fail")
    second = run("success")

    assert first == {"status": "failed", "calls": ["upstream", "downstream"]}
    assert second == {"status": "success", "calls": ["downstream"]}
