import asyncio
import time

import pytest
from kazeflow.assets import asset, clear_assets
from kazeflow.flow import Flow


@pytest.fixture(autouse=True)
def setup_assets():
    """Clears assets before each test."""
    clear_assets()


def test_run_flow_sync():
    """Tests the synchronous execution of a flow."""

    @asset()
    def first():
        time.sleep(0.01)
        return "first"

    @asset(deps=["first"])
    def second():
        time.sleep(0.01)
        return "second after first"

    @asset(deps=["second"])
    def third():
        time.sleep(0.01)
        return "third after second"

    flow = Flow(asset_names=["third"])
    flow.run_sync()


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

    flow = Flow(asset_names=["third"])
    flow.show_flow_tree()


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


def test_run_sync_failure():
    """Tests that the synchronous flow stops upon asset failure."""
    execution_tracker = []

    @asset()
    def asset_a():
        execution_tracker.append("a")

    @asset(deps=["asset_a"])
    def asset_b():
        execution_tracker.append("b")
        raise ValueError("Failure in B")

    @asset(deps=["asset_b"])
    def asset_c():
        execution_tracker.append("c")

    flow = Flow(asset_names=["asset_c"])
    flow.run_sync()

    assert execution_tracker == ["a", "b"]


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
