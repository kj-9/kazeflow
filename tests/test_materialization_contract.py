"""Focused tests for materialization identity and adapter call contracts."""

import asyncio
from dataclasses import FrozenInstanceError

import pytest

from kazeflow.materialization import (
    Materialization,
    MaterializationContext,
    MaterializationRecord,
    call_materialization,
    record_matches_context,
)


def make_context(**changes: object) -> MaterializationContext:
    fields: dict[str, object] = {
        "asset_name": "extract",
        "version": "v1",
        "partition_domain": "date",
        "partition_key_present": True,
        "partition_key": "2026-10-11",
        "input_materialization_ids": (("source", "source-id"),),
    }
    fields.update(changes)
    return MaterializationContext(**fields)  # type: ignore[arg-type]


def make_record(
    context: MaterializationContext, **changes: object
) -> MaterializationRecord:
    fields: dict[str, object] = {
        "materialization_id": "extract-id",
        "asset_name": context.asset_name,
        "version": context.version,
        "partition_domain": context.partition_domain,
        "partition_key_present": context.partition_key_present,
        "partition_key": context.partition_key,
        "input_materialization_ids": context.input_materialization_ids,
        "committed": True,
    }
    fields.update(changes)
    return MaterializationRecord(**fields)  # type: ignore[arg-type]


def test_context_and_record_are_frozen_and_normalize_input_pairs() -> None:
    context = make_context(input_materialization_ids=[["source", "source-id"]])
    record = make_record(context)

    assert context.input_materialization_ids == (("source", "source-id"),)
    assert record.input_materialization_ids == context.input_materialization_ids
    with pytest.raises(FrozenInstanceError):
        context.version = "v2"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        record.committed = False  # type: ignore[misc]


@pytest.mark.parametrize(
    "field,value",
    [
        ("materialization_id", ""),
        ("asset_name", ""),
        ("version", ""),
        ("partition_domain", ""),
        ("partition_key_present", 1),
        ("input_materialization_ids", (("source", ""),)),
        ("committed", 1),
    ],
)
def test_record_rejects_invalid_fields(field: str, value: object) -> None:
    context = make_context()
    with pytest.raises(ValueError):
        make_record(context, **{field: value})


def test_partition_presence_and_value_must_agree() -> None:
    with pytest.raises(ValueError, match="must agree"):
        make_context(partition_key_present=False, partition_key="unexpected")
    with pytest.raises(ValueError, match="must agree"):
        make_record(
            make_context(), partition_key_present=False, partition_key="unexpected"
        )
    with pytest.raises(ValueError, match="must agree"):
        make_context(partition_key_present=True, partition_key=None)


@pytest.mark.parametrize(
    "field,value",
    [
        ("asset_name", "other"),
        ("version", "v2"),
        ("partition_domain", "hour"),
        ("partition_key", "2026-10-12"),
        ("input_materialization_ids", (("source", "other-id"),)),
    ],
)
def test_record_identity_mismatch_is_not_reusable(field: str, value: object) -> None:
    context = make_context()
    assert not record_matches_context(make_record(context, **{field: value}), context)


@pytest.mark.parametrize("key", [0, False, ""])
def test_falsey_partition_keys_remain_present(key: object) -> None:
    context = make_context(partition_key=key)
    record = make_record(context)

    assert record_matches_context(record, context)


def test_equal_but_distinct_falsey_key_types_are_not_reusable() -> None:
    context = make_context(partition_key=0)
    record = make_record(context, partition_key=False)
    assert not record_matches_context(record, context)


def test_uncommitted_or_malformed_records_are_not_reusable() -> None:
    context = make_context()
    uncommitted = make_record(context, committed=False)
    malformed = make_record(context)
    object.__setattr__(malformed, "version", "")

    assert not record_matches_context(uncommitted, context)
    assert not record_matches_context(malformed, context)
    assert not record_matches_context(object(), context)


def test_materialization_protocol_and_sync_operations() -> None:
    context = make_context()
    record = make_record(context)

    class SyncAdapter:
        def resolve(self, candidate: MaterializationContext) -> MaterializationRecord:
            assert candidate == context
            return record

        def load(self, candidate: MaterializationRecord) -> str:
            assert candidate == record
            return "loaded"

        def commit(
            self, candidate: MaterializationContext, output: object
        ) -> MaterializationRecord:
            assert candidate == context
            assert output == "new output"
            return record

    adapter: Materialization = SyncAdapter()
    assert asyncio.run(call_materialization(adapter.resolve, context)) is record
    assert asyncio.run(call_materialization(adapter.load, record)) == "loaded"
    assert (
        asyncio.run(call_materialization(adapter.commit, context, "new output"))
        is record
    )


@pytest.mark.asyncio
async def test_async_and_sync_returning_awaitable_operations() -> None:
    async def async_operation(value: str) -> str:
        return f"async:{value}"

    def sync_operation(value: str):
        async def finish() -> str:
            return f"wrapped:{value}"

        return finish()

    assert await call_materialization(async_operation, "value") == "async:value"
    assert await call_materialization(sync_operation, "value") == "wrapped:value"
