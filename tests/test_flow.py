import asyncio

import pytest

from kazeflow.assets import asset, default_registry
from kazeflow.flow import Flow
from kazeflow.partition import DatePartitionDef


@pytest.fixture(autouse=True)
def setup_assets():
    """Clears assets before each test."""
    default_registry.clear()


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

    asset_names = ["third"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)
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

    asset_names = ["third"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)
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

    asset_names = ["third"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)
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

    asset_names = ["after_failure", "successful_branch"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)
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

    asset_names = ["a", "b", "c", "d"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)
    await flow.run_async(max_concurrency=2)

    assert max_observed_concurrency == 2


@pytest.mark.asyncio
async def test_asset_context_injection():
    """Tests that the AssetContext is correctly injected."""
    from kazeflow.assets import AssetContext

    result_log = []

    @asset()
    async def asset_with_context(context: AssetContext):
        result_log.append(f"Hello from {context.asset_name}")

    @asset()
    async def asset_without_context():
        # This asset should still run fine
        pass

    asset_names = ["asset_with_context", "asset_without_context"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)
    await flow.run_async()

    assert "Hello from asset_with_context" in result_log


def test_merged_dependency_resolution():
    """Tests that explicit and implicit dependencies are merged."""

    @asset
    def explicit_dep():
        return "explicit"

    @asset
    def implicit_dep():
        return "implicit"

    @asset(deps=["explicit_dep"])
    def target_asset(implicit_dep: str):
        pass

    asset_names = ["target_asset"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)

    assert "explicit_dep" in flow.graph["target_asset"]
    assert "implicit_dep" in flow.graph["target_asset"]
    assert len(flow.graph["target_asset"]) == 2


@pytest.mark.asyncio
async def test_partitioned_asset():
    """Tests that a partitioned asset is executed for each partition key."""
    from kazeflow.assets import AssetContext

    date_def = DatePartitionDef()

    @asset(partition_def=date_def)
    async def partitioned(context: AssetContext):
        return context.partition_key

    asset_names = ["partitioned"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)

    partitions = date_def.range("2025-09-21", "2025-09-23")

    await flow.run_async(run_config={"partition_keys": partitions})

    assert set(flow.asset_outputs["partitioned"].keys()) == set(partitions)
    assert all(key == value for key, value in flow.asset_outputs["partitioned"].items())


@pytest.mark.asyncio
async def test_downstream_of_partitioned_asset():
    """Tests that a downstream asset receives the output of all partitions."""
    from kazeflow.assets import AssetContext

    date_def = DatePartitionDef()

    @asset(partition_def=date_def)
    async def upstream(context: AssetContext):
        return context.partition_key

    @asset(deps=["upstream"])
    async def downstream(upstream: dict):
        return upstream

    asset_names = ["downstream"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)

    partitions = date_def.range("2025-09-21", "2025-09-23")

    await flow.run_async(run_config={"partition_keys": partitions})

    assert set(flow.asset_outputs["downstream"].keys()) == set(partitions)
    for partition_key in partitions:
        assert flow.asset_outputs["downstream"][partition_key] == partition_key


@pytest.mark.asyncio
async def test_downstream_partitioned_asset_dependency():
    """
    Tests that a downstream partitioned asset also gets executed for all partitions
    and correctly receives the output from its upstream partitioned dependency.
    """
    from kazeflow.assets import AssetContext

    date_def = DatePartitionDef()

    @asset(partition_def=date_def)
    async def upstream_partitioned(context: AssetContext):
        return f"output_{context.partition_key}"

    @asset(partition_def=date_def)
    async def downstream_partitioned(
        upstream_partitioned: dict[str, str], context: AssetContext
    ):
        # Assert that the input from the upstream asset corresponds to the same partition
        import datetime

        assert upstream_partitioned == {
            datetime.date(2025, 9, 21): "output_2025-09-21",
            datetime.date(2025, 9, 22): "output_2025-09-22",
            datetime.date(2025, 9, 23): "output_2025-09-23",
        }
        return f"downstream_{context.partition_key}"

    asset_names = ["downstream_partitioned"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)

    partitions = date_def.range("2025-09-21", "2025-09-23")

    await flow.run_async(run_config={"partition_keys": partitions})

    # Check that the downstream asset was executed for all partitions
    assert set(flow.asset_outputs["downstream_partitioned"].keys()) == set(partitions)
    for partition_key, output in flow.asset_outputs["downstream_partitioned"].items():
        assert output == f"downstream_{partition_key}"

    # Also check the upstream asset's output
    assert set(flow.asset_outputs["upstream_partitioned"].keys()) == set(partitions)
    for partition_key, output in flow.asset_outputs["upstream_partitioned"].items():
        assert output == f"output_{partition_key}"
