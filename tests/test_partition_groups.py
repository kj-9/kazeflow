"""Bounded key progression without changing result/dependency contracts."""

import asyncio

import pytest

from kazeflow import PartitionGroup
from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.events import validate_event_sequence
from kazeflow.file_materialization import FileMaterialization, JsonCodec
from kazeflow.flow import Flow
from kazeflow.partition import PartitionDef
from kazeflow.results import AttemptStatus, FlowStatus, SkipReason


class Keys(PartitionDef):
    @property
    def domain(self):
        return "group-test-keys"

    def range(self, start, end):
        raise AssertionError("not used")


class Consumer:
    def __init__(self):
        self.events = []

    def on_event(self, event):
        self.events.append(event)


@pytest.mark.asyncio
async def test_single_active_key_runs_depth_first_and_restarts_fresh():
    registry = AssetRegistry()
    trace = []

    async def source(context: AssetContext):
        trace.append(("source", context.partition_key))
        return 42

    async def final(source, context: AssetContext):
        trace.append(("final", context.partition_key))
        return source[context.partition_key] + 1

    registry.register(source, partition_def=Keys())
    registry.register(final, partition_def=Keys())
    flow = Flow(
        ["final"],
        registry=registry,
        partition_groups=[PartitionGroup(("source", "final"))],
    )
    observer = Consumer()
    result = await flow.run_async(
        {"partition_keys": [0, ""], "max_concurrency": 3}, event_consumer=observer
    )
    expected = [("source", 0), ("final", 0), ("source", ""), ("final", "")]
    assert trace == expected
    assert [task.task.task_name for task in result.tasks] == ["source", "final"]
    assert [
        [item.attempt.partition_key for item in task.attempts] for task in result.tasks
    ] == [[0, ""], [0, ""]]
    assert result.status is FlowStatus.SUCCESS
    validate_event_sequence(observer.events)
    await flow.run_async({"partition_keys": [0, ""]})
    assert trace == expected + expected


@pytest.mark.asyncio
async def test_active_key_cap_releases_completed_key_and_preserves_result_order():
    registry = AssetRegistry()
    active = set()
    peak = 0
    source_keys = []
    finish_keys = []
    gates = {key: asyncio.Event() for key in range(3)}
    both_waiting = asyncio.Event()
    third_started = asyncio.Event()
    waiting = set()

    async def source(context: AssetContext):
        nonlocal peak
        key = context.partition_key
        source_keys.append(key)
        active.add(key)
        peak = max(peak, len(active))
        if key == 2:
            third_started.set()
        return key

    async def final(source, context: AssetContext):
        key = context.partition_key
        waiting.add(key)
        if 0 in waiting and 1 in waiting:
            both_waiting.set()
        await gates[key].wait()
        active.remove(key)
        finish_keys.append(key)
        return source[key]

    registry.register(source, partition_def=Keys())
    registry.register(final, partition_def=Keys())
    flow = Flow(
        ["final"],
        registry=registry,
        partition_groups=[PartitionGroup(("source", "final"), max_active_keys=2)],
    )
    observer = Consumer()
    running = asyncio.create_task(
        flow.run_async(
            {"partition_keys": [0, 1, 2], "max_concurrency": 3}, event_consumer=observer
        )
    )
    try:
        await asyncio.wait_for(both_waiting.wait(), 1)
        assert source_keys == [0, 1]
        gates[1].set()
        await asyncio.wait_for(third_started.wait(), 1)
        assert source_keys == [0, 1, 2]
        gates[2].set()
        gates[0].set()
        result = await asyncio.wait_for(running, 1)
    finally:
        if not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
    assert peak == 2
    assert finish_keys[0] == 1
    assert [attempt.attempt.partition_key for attempt in result.tasks[1].attempts] == [
        0,
        1,
        2,
    ]
    assert all(len(task.attempts) == 3 for task in result.tasks)
    validate_event_sequence(observer.events)


@pytest.mark.asyncio
async def test_failed_key_blocks_its_chain_and_releases_group_slot():
    registry = AssetRegistry()
    trace = []

    async def source(context: AssetContext):
        trace.append(("source", context.partition_key))
        if context.partition_key == 0:
            raise ValueError("failure")
        return 42

    async def final(source, context: AssetContext):
        trace.append(("final", context.partition_key))
        return source[context.partition_key]

    registry.register(source, partition_def=Keys())
    registry.register(final, partition_def=Keys())
    flow = Flow(
        ["final"],
        registry=registry,
        partition_groups=[PartitionGroup(("source", "final"))],
    )
    observer = Consumer()
    result = await flow.run_async({"partition_keys": [0, 1]}, event_consumer=observer)
    assert trace == [("source", 0), ("source", 1), ("final", 1)]
    assert result.status is FlowStatus.FAILED
    blocked = result.tasks[1].attempts[0]
    assert blocked.status is AttemptStatus.SKIPPED
    assert blocked.reason is SkipReason.DEPENDENCY_BLOCKED
    assert blocked.blocked_by == (result.tasks[0].attempts[0].attempt,)
    assert result.tasks[1].attempts[1].status is AttemptStatus.SUCCESS
    validate_event_sequence(observer.events)


@pytest.mark.asyncio
async def test_unpartitioned_prerequisite_and_aggregate_keep_existing_barriers():
    registry = AssetRegistry()
    trace = []

    def prerequisite():
        trace.append("prerequisite")
        return "ready"

    def source(prerequisite, context: AssetContext):
        assert prerequisite == "ready"
        trace.append(("source", context.partition_key))
        return context.partition_key

    def final(source, context: AssetContext):
        trace.append(("final", context.partition_key))
        return source[context.partition_key]

    def aggregate(final):
        trace.append("aggregate")
        return list(final.values())

    registry.register(prerequisite)
    registry.register(source, partition_def=Keys())
    registry.register(final, partition_def=Keys())
    registry.register(aggregate)
    flow = Flow(
        ["aggregate"],
        registry=registry,
        partition_groups=[PartitionGroup(("source", "final"))],
    )
    result = await flow.run_async({"partition_keys": [0, 1]})
    assert trace == [
        "prerequisite",
        ("source", 0),
        ("final", 0),
        ("source", 1),
        ("final", 1),
        "aggregate",
    ]
    assert result.tasks[-1].attempts[0].output == [0, 1]


@pytest.mark.asyncio
async def test_materialization_hits_progress_group_without_body_calls(tmp_path):
    registry = AssetRegistry()
    trace = []

    def source(context: AssetContext):
        trace.append(("source", context.partition_key))
        return 42

    def final(source, context: AssetContext):
        trace.append(("final", context.partition_key))
        return source[context.partition_key] + 1

    registry.register(
        source,
        partition_def=Keys(),
        materialization=FileMaterialization(tmp_path / "source", codec=JsonCodec()),
        version="1",
    )
    registry.register(
        final,
        partition_def=Keys(),
        materialization=FileMaterialization(tmp_path / "final", codec=JsonCodec()),
        version="1",
    )
    flow = Flow(
        ["final"],
        registry=registry,
        partition_groups=[PartitionGroup(("source", "final"))],
    )
    first = await flow.run_async({"partition_keys": [0, 1]})
    second = await flow.run_async({"partition_keys": [0, 1], "max_concurrency": 1})
    assert trace == [("source", 0), ("final", 0), ("source", 1), ("final", 1)]
    assert all(
        attempt.resolution == "materialized"
        for task in second.tasks
        for attempt in task.attempts
    )
    assert [
        attempt.materialization_id for task in first.tasks for attempt in task.attempts
    ] == [
        attempt.materialization_id for task in second.tasks for attempt in task.attempts
    ]
