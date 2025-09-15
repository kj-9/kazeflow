import asyncio
from graphlib import TopologicalSorter
from typing import Any, Optional

from .assets import get_asset
from .logger import get_logger

logger = get_logger(__name__)


class Flow:
    """A class representing a workflow of assets."""

    graph: dict[str, set[str]]
    ts: TopologicalSorter
    static_order: list[str]

    def __init__(self, asset_names: list[str]):
        self.asset_names = asset_names

    def _get_ts(self) -> TopologicalSorter:
        """Sets up the topological sorter based on asset dependencies."""
        graph = {}

        for asset_name in self.asset_names:
            asset = get_asset(asset_name)
            graph[asset_name] = set(asset["deps"])

            for dep in asset["deps"]:
                if dep not in graph:
                    dep_asset = get_asset(dep)
                    graph[dep] = set(dep_asset["deps"])

        ts = TopologicalSorter(graph)
        return ts

    def _execute_asset(
        self,
        asset_name: str,
    ) -> Any:
        """Executes a single asset and its dependencies."""

        logger.info(f"Executing asset: {asset_name}")
        asset = get_asset(asset_name)
        asset["func"]()
        logger.info(f"Finished executing asset: {asset_name}")

    async def _execute_asset_async(
        self,
        asset_name: str,
    ) -> Any:
        """Asynchronously executes a single asset and its dependencies."""

        logger.info(f"Executing asset: {asset_name}")
        asset = get_asset(asset_name)
        await asset["func"]()
        logger.info(f"Finished executing asset: {asset_name}")

    def run_sync(self, config: Optional[dict[str, Any]] = None) -> None:
        """Executes the assets in the flow."""

        ts = self._get_ts()
        static_order = list(ts.static_order())

        for asset_name in static_order:
            self._execute_asset(asset_name)

    async def run_async(
        self, config: Optional[dict[str, Any]] = None, num_workers=3
    ) -> list[str]:
        ts = self._get_ts()
        ts.prepare()
        records = []
        running = {
            asyncio.create_task(get_asset(n)["func"]()): n for n in ts.get_ready()
        }
        print(running)

        while running:
            done, _ = await asyncio.wait(
                running.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            for d in done:
                name = running.pop(d)
                ts.done(name)
                for new in ts.get_ready():
                    running[asyncio.create_task(get_asset(new)["func"]())] = new

        return records
