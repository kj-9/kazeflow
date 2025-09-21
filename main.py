import asyncio

from kazeflow.assets import asset, build_graph
from kazeflow.flow import Flow


@asset
async def first():
    await asyncio.sleep(1)


@asset(deps=["first"])
async def second():
    await asyncio.sleep(2)
    raise Exception("oops")


@asset(deps=["second"])
async def third():
    await asyncio.sleep(2)


@asset(deps=["second"])
async def fourth():
    await asyncio.sleep(2)


@asset(deps=["first"])
async def fifth():
    return "hoge"


@asset
async def sixth(fifth, context):
    await asyncio.sleep(2)
    context.logger.info(fifth)


if __name__ == "__main__":
    asset_names = ["fourth", "third", "sixth"]
    graph = build_graph(asset_names)
    flow = Flow(graph)
    asyncio.run(flow.run_async())
