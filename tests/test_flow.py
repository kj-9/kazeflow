import asyncio

import pytest
from kazeflow.assets import asset, clear_assets
from kazeflow.flow import Flow


@pytest.fixture(autouse=True)
def setup_assets():
    """Clears assets before each test."""
    clear_assets()


@pytest.mark.asyncio
async def test_run_flow_async():
    """Tests the asynchronous execution of a flow."""

    @asset()
    async def first():
        await asyncio.sleep(0.01)
        return "first"

    @asset(deps=["first"])
    async def second(first):
        await asyncio.sleep(0.01)
        return f"second after {first}"

    @asset(deps=["second"])
    async def third(second):
        await asyncio.sleep(0.01)
        return f"third after {second}"

    flow = Flow(asset_names=["third"])
    await flow.run_async()


def test_show_flow_tree():
    """Tests the display of the flow tree."""

    @asset()
    def first():
        return "first"

    @asset(deps=["first"])
    def second(first):
        return f"second after {first}"

    @asset(deps=["second"])
    def third(second):
        return f"third after {second}"

    from kazeflow.tui import show_flow_tree

    flow = Flow(asset_names=["third"])
    show_flow_tree(flow.graph)


@pytest.mark.asyncio
async def test_asset_io():
    """Tests that asset outputs are correctly passed as inputs to dependencies."""

    @asset()
    async def first():
        return "hello"

    @asset(deps=["first"])
    async def second(first: str):
        return f"{first} world"

    @asset(deps=["second"])
    async def third(second: str):
        return f"{second}!"

    flow = Flow(asset_names=["third"])
    await flow.run_async()

    assert flow.asset_outputs["third"] == "hello world!"


@pytest.mark.asyncio
async def test_run_async_failure():
    """Tests that independent assets continue execution when another asset fails in async flow."""
    execution_tracker = []

    @asset()
    async def start_node():
        execution_tracker.append("start")

    @asset(deps=["start_node"])
    async def failing_branch():
        execution_tracker.append("failing")
        raise ValueError("Failure")

    @asset(deps=["failing_branch"])
    async def after_failure():
        execution_tracker.append("after_failure")

    @asset(deps=["start_node"])
    async def successful_branch():
        execution_tracker.append("successful")

    flow = Flow(asset_names=["after_failure", "successful_branch"])
    await flow.run_async()

    assert "failing" in execution_tracker
    assert "successful" in execution_tracker
    assert "after_failure" not in execution_tracker


@pytest.mark.asyncio
async def test_max_concurrency():
    """Tests that max_concurrency is respected in run_async."""
    running_count = 0
    max_observed_concurrency = 0
    lock = asyncio.Lock()

    @asset()
    async def start():
        pass

    @asset(deps=["start"])
    async def a():
        nonlocal running_count, max_observed_concurrency
        async with lock:
            running_count += 1
            max_observed_concurrency = max(max_observed_concurrency, running_count)
        await asyncio.sleep(0.1)
        async with lock:
            running_count -= 1

    @asset(deps=["start"])
    async def b():
        nonlocal running_count, max_observed_concurrency
        async with lock:
            running_count += 1
            max_observed_concurrency = max(max_observed_concurrency, running_count)
        await asyncio.sleep(0.1)
        async with lock:
            running_count -= 1

    @asset(deps=["start"])
    async def c():
        nonlocal running_count, max_observed_concurrency
        async with lock:
            running_count += 1
            max_observed_concurrency = max(max_observed_concurrency, running_count)
        await asyncio.sleep(0.1)
        async with lock:
            running_count -= 1

    @asset(deps=["start"])
    async def d():
        nonlocal running_count, max_observed_concurrency
        async with lock:
            running_count += 1
            max_observed_concurrency = max(max_observed_concurrency, running_count)
        await asyncio.sleep(0.1)
        async with lock:
            running_count -= 1

    flow = Flow(asset_names=["a", "b", "c", "d"])
    await flow.run_async(max_concurrency=2)

    assert max_observed_concurrency == 2


@pytest.mark.asyncio
async def test_asset_context_injection():
    """Tests that the AssetContext is correctly injected."""
    from kazeflow.context import AssetContext

    result_log = []

    @asset()
    async def asset_with_context(context: AssetContext):
        result_log.append(f"Hello from {context.asset_name}")

    @asset()
    async def asset_without_context():
        # This asset should still run fine
        pass

    flow = Flow(asset_names=["asset_with_context", "asset_without_context"])
    await flow.run_async()

    assert "Hello from asset_with_context" in result_log
