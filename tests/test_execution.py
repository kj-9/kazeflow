import asyncio

import pytest

from kazeflow.assets import AssetContext, AssetRegistry, asset, default_registry
from kazeflow.events import validate_event_sequence
from kazeflow.flow import Flow, run
from kazeflow.partition import DatePartitionDef
from kazeflow.results import AttemptStatus, FlowStatus, SkipReason


@pytest.fixture(autouse=True)
def clear_default_registry() -> None:
    default_registry.clear()


def test_flow_owns_targets_and_registry_and_plan_has_no_side_effect() -> None:
    registry = AssetRegistry()
    called: list[str] = []

    def target() -> str:
        called.append("target")
        return "custom"

    registry.register(target)
    flow = Flow(["target"], registry=registry)

    assert flow.targets == ("target",)
    assert flow.registry is registry
    assert flow.plan().tasks[0].name == "target"
    assert called == []
    assert run(["target"], registry=registry).tasks[0].attempts[0].output == "custom"


def test_checked_legacy_graph_rejects_drift_before_events_or_assets() -> None:
    called = False

    @asset
    def source() -> None:
        nonlocal called
        called = True

    @asset(deps=["source"])
    def target() -> None:
        nonlocal called
        called = True

    events: list[object] = []

    class Consumer:
        def on_event(self, event: object) -> None:
            events.append(event)

    flow = Flow({"target": set(), "source": set()})
    with pytest.raises(ValueError, match="exactly match"):
        asyncio.run(flow.run_async(event_consumer=Consumer()))
    assert not called
    assert events == []


@pytest.mark.asyncio
async def test_results_events_failure_and_branch_continuation() -> None:
    called: list[str] = []

    @asset
    async def root() -> str:
        return "root"

    @asset(deps=["root"])
    async def bad(root: str) -> None:
        called.append("bad")
        raise ValueError("broken")

    @asset(deps=["bad"])
    async def blocked(bad: object) -> None:
        called.append("blocked")

    @asset(deps=["root"])
    async def good(root: str) -> str:
        called.append("good")
        return root

    events = []

    class Consumer:
        def on_event(self, event: object) -> None:
            events.append(event)

    result = await Flow(["blocked", "good"]).run_async(event_consumer=Consumer())

    assert result.status is FlowStatus.FAILED
    assert [task.task.task_name for task in result.tasks] == [
        "root",
        "bad",
        "blocked",
        "good",
    ]
    assert [task.status for task in result.tasks] == [
        AttemptStatus.SUCCESS,
        AttemptStatus.FAILED,
        AttemptStatus.SKIPPED,
        AttemptStatus.SUCCESS,
    ]
    assert result.tasks[2].attempts[0].reason is SkipReason.DEPENDENCY_BLOCKED
    assert called == ["bad", "good"]
    validate_event_sequence(events)
    assert events[-1].status is FlowStatus.FAILED


@pytest.mark.asyncio
async def test_ready_work_and_partitions_drain_exactly_once_under_bound() -> None:
    active = 0
    peak = 0
    calls: list[object] = []
    lock = asyncio.Lock()

    async def work(context: AssetContext | None = None) -> object:
        nonlocal active, peak
        async with lock:
            active += 1
            peak = max(peak, active)
        calls.append(context.partition_key if context else "plain")
        await asyncio.sleep(0.01)
        async with lock:
            active -= 1
        return context.partition_key if context else "plain"

    for name in ("a", "b", "c"):
        work.__name__ = name
        default_registry.register(work)

    result = await Flow(["a", "b", "c"]).run_async({"max_concurrency": 2})
    assert result.status is FlowStatus.SUCCESS
    assert calls == [None, None, None]
    assert peak <= 2

    default_registry.clear()
    date_def = DatePartitionDef()

    @asset(partition_def=date_def)
    async def partitioned(context: AssetContext) -> object:
        nonlocal active, peak
        async with lock:
            active += 1
            peak = max(peak, active)
        calls.append(context.partition_key)
        await asyncio.sleep(0.01)
        async with lock:
            active -= 1
        return context.partition_key

    keys = date_def.range("2026-01-01", "2026-01-04")
    result = await Flow(["partitioned"]).run_async(
        {"max_concurrency": 2, "partition_keys": keys}
    )
    assert [
        attempt.attempt.partition_key for attempt in result.tasks[0].attempts
    ] == keys
    assert calls[-4:] == keys
    assert peak <= 2


@pytest.mark.asyncio
async def test_partition_shapes_falsey_keys_empty_reducer_and_fresh_rerun() -> None:
    date_def = DatePartitionDef()
    received: list[tuple[object, dict[object, object], dict[object, object]]] = []

    @asset(partition_def=date_def)
    async def left(context: AssetContext) -> str:
        return f"left-{context.partition_key}"

    @asset(partition_def=date_def)
    async def right(context: AssetContext) -> str:
        return f"right-{context.partition_key}"

    @asset(partition_def=date_def)
    async def joined(
        left: dict[object, object], right: dict[object, object], context: AssetContext
    ) -> object:
        received.append((context.partition_key, left, right))
        return context.partition_key

    @asset(deps=["joined"])
    async def reducer(joined: dict[object, object]) -> dict[object, object]:
        return joined

    flow = Flow(["reducer"])
    result = await flow.run_async({"partition_keys": [0, ""]})
    assert result.status is FlowStatus.SUCCESS
    assert received == [
        (0, {0: "left-0"}, {0: "right-0"}),
        ("", {"": "left-"}, {"": "right-"}),
    ]
    assert result.tasks[-1].attempts[0].output == {0: 0, "": ""}

    received.clear()
    result = await flow.run_async({"partition_keys": []})
    assert result.tasks[0].reason is SkipReason.NO_PARTITION_KEYS
    assert result.tasks[-1].attempts[0].output == {}
    assert received == []


@pytest.mark.asyncio
async def test_matching_partition_failure_blocks_only_matching_key() -> None:
    date_def = DatePartitionDef()
    seen: list[object] = []

    @asset(partition_def=date_def)
    async def upstream(context: AssetContext) -> object:
        if context.partition_key == 0:
            raise ValueError("zero")
        return context.partition_key

    @asset(partition_def=date_def)
    async def downstream(
        upstream: dict[object, object], context: AssetContext
    ) -> object:
        seen.append(upstream)
        return context.partition_key

    result = await Flow(["downstream"]).run_async({"partition_keys": [0, ""]})
    downstream_result = result.tasks[-1]
    assert downstream_result.attempts[0].status is AttemptStatus.SKIPPED
    assert downstream_result.attempts[1].status is AttemptStatus.SUCCESS
    assert seen == [{"": ""}]


@pytest.mark.asyncio
async def test_external_cancellation_has_no_terminal_event_or_pending_start() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []
    events = []

    @asset
    async def first() -> None:
        calls.append("first")
        started.set()
        await release.wait()

    @asset
    async def second() -> None:
        calls.append("second")

    class Consumer:
        def on_event(self, event: object) -> None:
            events.append(event)

    task = asyncio.create_task(
        Flow(["first", "second"]).run_async(
            {"max_concurrency": 1}, event_consumer=Consumer()
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == ["first"]
    assert not events or events[-1].kind.value != "flow_finished"


@pytest.mark.asyncio
async def test_cancellation_discards_synchronous_thread_output() -> None:
    import threading

    entered = threading.Event()
    release = threading.Event()
    dependent_calls: list[str] = []

    @asset
    def source() -> str:
        entered.set()
        assert release.wait(1)
        return "late-output"

    @asset(deps=["source"])
    async def dependent(source: str) -> None:
        dependent_calls.append(source)

    flow = Flow(["dependent"])
    task = asyncio.create_task(flow.run_async())
    await asyncio.to_thread(entered.wait)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await asyncio.sleep(0.02)
    assert dependent_calls == []
    assert flow.asset_outputs == {}


@pytest.mark.asyncio
async def test_sync_run_rejects_active_loop_before_asset_or_event() -> None:
    called = False
    events: list[object] = []

    @asset
    def target() -> None:
        nonlocal called
        called = True

    class Consumer:
        def on_event(self, event: object) -> None:
            events.append(event)

    with pytest.raises(RuntimeError, match="run_async"):
        run(["target"], event_consumer=Consumer())
    assert not called
    assert events == []
