import asyncio

import pytest

from kazeflow.assets import asset
from kazeflow.flow import Flow


@asset()
async def first(): ...


@asset(deps=["first"])
async def second(): ...


@asset(deps=["second"])
async def third(): ...


def test_run_flow_sync():
    flow = Flow(asset_names=["third"])

    flow.run_sync()


def test_run_flow_async():
    flow = Flow(asset_names=["third"])
    asyncio.run(flow.run_async())


def test_flow_with_missing_dependency():
    with pytest.raises(ValueError, match="Asset 'non_existent_asset' not found."):
        flow = Flow(asset_names=["non_existent_asset"])
        asyncio.run(flow.run_async())
