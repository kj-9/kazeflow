"""Executor edge evidence for depth-first partition groups."""

import asyncio

import pytest

from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.events import EventKind, validate_event_sequence
from kazeflow.flow import Flow
from kazeflow.groups import PartitionGroup
from kazeflow.partition import PartitionDef
from kazeflow.resources import OpenCircuit, ResourcePolicy, Retry
from kazeflow.results import AttemptStatus, SkipReason


class IdentityPartitions(PartitionDef):
    def range(self, start, end):
        raise AssertionError("range is not used")


@pytest.mark.asyncio
async def test_retry_holds_group_slot_while_unrelated_work_uses_global_and_resource_capacity():
    registry = AssetRegistry()
    partition_def = IdentityPartitions()
    calls: list[tuple[str, object]] = []
    active = 0
    peak_global = 0
    active_api = 0
    peak_api = 0
    source_attempts: dict[object, int] = {}

    async def enter(kind: str, key: object) -> None:
        nonlocal active, peak_global, active_api, peak_api
        active += 1
        peak_global = max(peak_global, active)
        if kind != "cpu":
            active_api += 1
            peak_api = max(peak_api, active_api)
        calls.append((kind, key))

    async def leave(kind: str) -> None:
        nonlocal active, active_api
        active -= 1
        if kind != "cpu":
            active_api -= 1

    async def first(context: AssetContext) -> str:
        key = context.partition_key
        await enter("first", key)
        source_attempts[key] = source_attempts.get(key, 0) + 1
        try:
            if key == "k0" and source_attempts[key] == 1:
                await asyncio.sleep(0)
                raise RuntimeError("transient")
            await asyncio.sleep(0)
            return str(key)
        finally:
            await leave("first")

    async def second(context: AssetContext, first: dict[object, str]) -> str:
        key = context.partition_key
        await enter("second", key)
        try:
            await asyncio.sleep(0)
            return first[key]
        finally:
            await leave("second")

    async def api_free() -> None:
        await enter("api_free", "plain")
        try:
            await asyncio.sleep(0.015)
        finally:
            await leave("api_free")

    async def cpu_free() -> None:
        await enter("cpu", "plain")
        try:
            await asyncio.sleep(0.015)
        finally:
            await leave("cpu")

    registry.register(first, partition_def=partition_def, resource="api")
    registry.register(
        second,
        deps=["first"],
        partition_def=partition_def,
        resource="api",
    )
    registry.register(api_free, resource="api")
    registry.register(cpu_free, resource="cpu")
    events = []

    class Consumer:
        def on_event(self, event):
            events.append(event)

    flow = Flow(
        ["second", "api_free", "cpu_free"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1,
                max_attempts=2,
                classify_failure=lambda _: Retry(0.04),
            ),
            "cpu": ResourcePolicy(1),
        },
        partition_groups=[PartitionGroup(("first", "second"))],
    )
    result = await flow.run_async(
        {"partition_keys": ["k0", "k1"], "max_concurrency": 2},
        event_consumer=Consumer(),
    )

    results = {task.task.task_name: task for task in result.tasks}
    first_result = results["first"]
    second_result = results["second"]
    assert [attempt.status for attempt in first_result.attempts] == [
        AttemptStatus.SUCCESS,
        AttemptStatus.SUCCESS,
    ]
    assert len(first_result.attempts[0].invocations) == 2
    assert [attempt.status for attempt in second_result.attempts] == [
        AttemptStatus.SUCCESS,
        AttemptStatus.SUCCESS,
    ]
    assert calls.index(("second", "k0")) < calls.index(("first", "k1"))
    retry_index = [
        index for index, call in enumerate(calls) if call == ("first", "k0")
    ][1]
    assert calls.index(("api_free", "plain")) < retry_index
    assert peak_api == 1
    assert peak_global == 2
    validate_event_sequence(events)


@pytest.mark.asyncio
async def test_circuit_and_dependency_skips_terminalize_once_in_causal_order():
    registry = AssetRegistry()
    partition_def = IdentityPartitions()
    calls: list[tuple[str, object]] = []

    async def first(context: AssetContext) -> str:
        key = context.partition_key
        calls.append(("first", key))
        if key == "k0":
            raise RuntimeError("open the circuit")
        return str(key)

    async def second(context: AssetContext) -> str:
        calls.append(("second", context.partition_key))
        return str(context.partition_key)

    registry.register(first, partition_def=partition_def, resource="api")
    registry.register(
        second,
        deps=["first"],
        partition_def=partition_def,
    )
    events = []

    class Consumer:
        def on_event(self, event):
            events.append(event)

    result = await Flow(
        ["second"],
        registry=registry,
        resources={"api": ResourcePolicy(1, classify_failure=lambda _: OpenCircuit())},
        partition_groups=[PartitionGroup(("first", "second"))],
    ).run_async({"partition_keys": ["k0", "k1"]}, event_consumer=Consumer())

    first_result, second_result = result.tasks
    assert [attempt.status for attempt in first_result.attempts] == [
        AttemptStatus.FAILED,
        AttemptStatus.SKIPPED,
    ]
    assert [attempt.status for attempt in second_result.attempts] == [
        AttemptStatus.SKIPPED,
        AttemptStatus.SKIPPED,
    ]
    assert first_result.attempts[1].reason is SkipReason.RESOURCE_CIRCUIT_OPEN
    assert first_result.attempts[1].blocked_by == (first_result.attempts[0].attempt,)
    assert all(
        attempt.reason is SkipReason.DEPENDENCY_BLOCKED
        for attempt in second_result.attempts
    )
    terminal_events = [
        event for event in events if event.kind is EventKind.ATTEMPT_FINISHED
    ]
    references = [event.attempt for event in terminal_events]
    expected = [attempt.attempt for task in result.tasks for attempt in task.attempts]
    assert len(references) == len(set(references)) == len(expected)
    assert set(references) == set(expected)
    positions = {event.attempt: index for index, event in enumerate(terminal_events)}
    assert (
        positions[first_result.attempts[0].attempt]
        < positions[first_result.attempts[1].attempt]
    )
    for attempt in second_result.attempts:
        assert positions[attempt.blocked_by[0]] < positions[attempt.attempt]
    assert calls == [("first", "k0")]
    validate_event_sequence(events)


@pytest.mark.asyncio
async def test_external_cancellation_does_not_synthesize_group_completions():
    registry = AssetRegistry()
    partition_def = IdentityPartitions()
    started = asyncio.Event()
    release = asyncio.Event()
    events = []

    async def first(context: AssetContext) -> None:
        started.set()
        await release.wait()

    async def second(context: AssetContext) -> None:
        return None

    registry.register(first, partition_def=partition_def)
    registry.register(second, deps=["first"], partition_def=partition_def)

    class Consumer:
        def on_event(self, event):
            events.append(event)

    flow = Flow(
        ["second"],
        registry=registry,
        partition_groups=[PartitionGroup(("first", "second"))],
    )
    running = asyncio.create_task(
        flow.run_async({"partition_keys": ["k0", "k1"]}, event_consumer=Consumer())
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert [event.kind for event in events] == [
        EventKind.FLOW_STARTED,
        EventKind.TASK_STARTED,
        EventKind.ATTEMPT_STARTED,
    ]
    assert not any(
        event.kind
        in (
            EventKind.ATTEMPT_FINISHED,
            EventKind.TASK_FINISHED,
            EventKind.FLOW_FINISHED,
        )
        for event in events
    )


@pytest.mark.asyncio
async def test_observer_failure_does_not_synthesize_group_completions():
    registry = AssetRegistry()
    partition_def = IdentityPartitions()
    calls = []

    async def first(context: AssetContext) -> None:
        calls.append("first")

    async def second(context: AssetContext) -> None:
        calls.append("second")

    registry.register(first, partition_def=partition_def)
    registry.register(second, deps=["first"], partition_def=partition_def)
    events = []

    class Consumer:
        def on_event(self, event):
            events.append(event)
            if event.kind is EventKind.ATTEMPT_STARTED:
                raise LookupError("observer stopped execution")

    flow = Flow(
        ["second"],
        registry=registry,
        partition_groups=[PartitionGroup(("first", "second"))],
    )
    with pytest.raises(LookupError, match="observer stopped"):
        await flow.run_async(
            {"partition_keys": ["k0", "k1"]}, event_consumer=Consumer()
        )

    assert calls == []
    assert [event.kind for event in events] == [
        EventKind.FLOW_STARTED,
        EventKind.TASK_STARTED,
        EventKind.ATTEMPT_STARTED,
    ]


@pytest.mark.asyncio
async def test_empty_group_selection_and_ungrouped_partition_defaults():
    registry = AssetRegistry()
    partition_def = IdentityPartitions()
    calls: list[tuple[str, object]] = []

    async def first(context: AssetContext) -> object:
        calls.append(("first", context.partition_key))
        return context.partition_key

    async def second(context: AssetContext) -> object:
        calls.append(("second", context.partition_key))
        return context.partition_key

    async def independent(context: AssetContext) -> object:
        calls.append(("independent", context.partition_key))
        return context.partition_key

    registry.register(first, partition_def=partition_def)
    registry.register(second, deps=["first"], partition_def=partition_def)
    registry.register(independent, partition_def=partition_def)
    flow = Flow(
        ["second"],
        registry=registry,
        partition_groups=[PartitionGroup(("first", "second"))],
    )

    empty = await flow.run_async({"partition_keys": []})
    assert [task.reason for task in empty.tasks] == [
        SkipReason.NO_PARTITION_KEYS,
        SkipReason.NO_PARTITION_KEYS,
    ]
    assert all(task.attempts == () for task in empty.tasks)
    assert calls == []

    ungrouped = await Flow(["independent"], registry=registry).run_async(
        {"partition_keys": ["k2", "k1"]}
    )
    assert calls == [("independent", "k2"), ("independent", "k1")]
    assert [
        attempt.attempt.partition_key for attempt in ungrouped.tasks[0].attempts
    ] == [
        "k2",
        "k1",
    ]
