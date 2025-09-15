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
