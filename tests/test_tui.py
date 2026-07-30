import asyncio
from io import StringIO

import pytest
from rich.console import Console

from kazeflow.assets import AssetContext, asset, default_registry
from kazeflow.flow import Flow
from kazeflow.partition import DatePartitionDef
from kazeflow.results import FlowStatus
from kazeflow.tui import FlowTUIRenderer, show_flow_tree, show_plan_tree


@pytest.fixture(autouse=True)
def clear_default_registry() -> None:
    default_registry.clear()


def test_tree_renderers_accept_legacy_graph_and_flow_plan(
    capsys: pytest.CaptureFixture[str],
) -> None:
    @asset
    def source() -> None:
        return None

    @asset(deps=["source"])
    def target(source: object) -> None:
        return None

    output = StringIO()
    console = Console(file=output, force_terminal=False, color_system=None)
    show_flow_tree({"target": {"source"}, "source": set()})
    show_plan_tree(Flow(["target"]).plan(), console=console)
    legacy_output = capsys.readouterr().out
    assert "source" in legacy_output
    assert "target" in legacy_output
    assert "source" in output.getvalue()
    assert "target" in output.getvalue()


@pytest.mark.asyncio
async def test_renderer_consumes_events_and_preserves_result_semantics() -> None:
    partitions = DatePartitionDef()

    @asset(partition_def=partitions)
    async def work(context: AssetContext) -> object:
        if context.partition_key == 0:
            raise ValueError("zero is bad")
        return context.partition_key

    plain = await Flow(["work"]).run_async({"partition_keys": [0, ""]})
    console = Console(file=StringIO(), force_terminal=False, color_system=None)
    renderer = FlowTUIRenderer(total_assets=1, console=console)
    with renderer:
        rendered = await Flow(["work"]).run_async(
            {"partition_keys": [0, ""]}, event_consumer=renderer
        )

    assert plain.status is rendered.status is FlowStatus.FAILED
    assert [task.status for task in plain.tasks] == [
        task.status for task in rendered.tasks
    ]
    assert [
        (
            attempt.attempt.partition_key_present,
            attempt.attempt.partition_key,
            attempt.status,
            attempt.failure,
        )
        for attempt in plain.tasks[0].attempts
    ] == [
        (
            attempt.attempt.partition_key_present,
            attempt.attempt.partition_key,
            attempt.status,
            attempt.failure,
        )
        for attempt in rendered.tasks[0].attempts
    ]
    assert [event.sequence for event in renderer.events] == list(
        range(1, len(renderer.events) + 1)
    )
    assert renderer.events[-1].status is FlowStatus.FAILED
    labels = [task.description for task in renderer.failed_progress.tasks]
    labels.extend(task.description for task in renderer.completed_progress.tasks)
    assert "work [partition=0]" in labels
    assert "work [partition='']" in labels


@pytest.mark.asyncio
async def test_renderer_with_default_total_completes_successfully() -> None:
    @asset
    async def work() -> str:
        return "done"

    renderer = FlowTUIRenderer(console=Console(file=StringIO()))
    with renderer:
        result = await Flow(["work"]).run_async(event_consumer=renderer)

    overall = renderer.overall_progress.tasks[0]
    assert result.status is FlowStatus.SUCCESS
    assert overall.total is None
    assert overall.completed == 1
    assert renderer.events[-1].status is FlowStatus.SUCCESS


@pytest.mark.asyncio
async def test_renderer_closes_safely_after_cancelled_event_prefix() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    @asset
    async def work() -> None:
        started.set()
        await release.wait()

    renderer = FlowTUIRenderer(console=Console(file=StringIO()))
    with renderer:
        task = asyncio.create_task(Flow(["work"]).run_async(event_consumer=renderer))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert renderer.events
    assert renderer.events[-1].kind.value != "flow_finished"


def test_renderer_rejects_negative_total() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        FlowTUIRenderer(-1)
