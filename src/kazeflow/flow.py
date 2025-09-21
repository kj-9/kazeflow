import asyncio
import logging
from graphlib import TopologicalSorter
from typing import Any, Optional

from .assets import get_asset, AssetResult
from .tui import FlowTUIRenderer, show_flow_tree


class Flow:
    """A class representing a workflow of assets."""

    def __init__(self, graph: dict[str, set[str]]):
        self.graph = graph
        self.asset_outputs: dict[str, Any] = {}

        self.static_order = list(TopologicalSorter(self.graph).static_order())

    async def _execute_asset(
        self, asset_name: str, logger: logging.Logger
    ) -> AssetResult:
        """Executes an asset and returns a result object."""
        asset = get_asset(asset_name)
        return await asset.execute(logger, self.asset_outputs)

    async def run_async(
        self,
        config: Optional[dict[str, Any]] = None,
        max_concurrency: Optional[int] = None,
    ) -> None:
        """Executes the assets in the flow asynchronously with a concurrency limit."""
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError("max_concurrency must be a positive integer or None.")

        show_flow_tree(self.graph)
        ts = TopologicalSorter(self.graph)
        ts.prepare()

        tui = FlowTUIRenderer(total_assets=len(self.static_order))
        running_tasks_map: dict[asyncio.Task, tuple[str, int]] = {}

        with tui:
            while ts.is_active():
                ready_to_run = list(ts.get_ready())

                limit = max_concurrency if max_concurrency is not None else float("inf")

                # Start new tasks if we have capacity and there are tasks ready to run
                while len(running_tasks_map) < limit and ready_to_run:
                    asset_name = ready_to_run.pop(0)
                    progress_task_id = tui.add_running_task(asset_name)
                    async_task = asyncio.create_task(
                        self._execute_asset(asset_name, tui.logger)
                    )
                    running_tasks_map[async_task] = (asset_name, progress_task_id)

                if not running_tasks_map:
                    break  # Nothing running, nothing new to start

                done, _ = await asyncio.wait(
                    running_tasks_map.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    asset_name, progress_task_id = running_tasks_map.pop(task)

                    asset_result: AssetResult = task.result()

                    tui.complete_running_task(progress_task_id, asset_result)

                    if asset_result.success:
                        self.asset_outputs[asset_name] = asset_result.output
                        ts.done(asset_name)
