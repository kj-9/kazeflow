import asyncio
from graphlib import TopologicalSorter
from typing import Any, Optional, TypedDict
from collections import defaultdict


from .assets import AssetContext, AssetResult, default_registry
from .tui import FlowTUIRenderer, show_flow_tree
from .partition import PartitionKeys


class RunConfig(TypedDict):
    partition_keys: PartitionKeys


class AssetExecutionManager:
    def __init__(
        self,
        graph: dict[str, set[str]],
        run_config: Optional[RunConfig],
        max_concurrency: Optional[int],
        tui: FlowTUIRenderer,
        asset_outputs: dict[str, Any],
    ):
        self.ts = TopologicalSorter(graph)
        self.run_config = run_config
        self.max_concurrency = max_concurrency or float("inf")
        self.tui = tui
        self.asset_outputs = asset_outputs

        self.partitions_to_process: dict[str, list[Optional[str]]] = {}
        self.running_tasks: dict[asyncio.Task, tuple[str, Optional[str], int]] = {}
        self.flow_failed = False
        self.done_assets = set()

    async def run(self):
        self.ts.prepare()
        while self.ts.is_active():
            if self.flow_failed:
                break
            self._spawn_tasks()

            if not self.running_tasks:
                break

            done, _ = await asyncio.wait(
                self.running_tasks.keys(), return_when=asyncio.FIRST_COMPLETED
            )
            self._process_completed_tasks(done)

    def _spawn_tasks(self):
        if self.flow_failed:
            return

        ready_assets = self.ts.get_ready()
        for asset_name in ready_assets:
            if asset_name not in self.partitions_to_process:
                asset = default_registry.get(asset_name)
                if asset.partition_def:
                    self.partitions_to_process[asset_name] = (
                        self.run_config or {}
                    ).get("partition_keys", [])
                else:
                    self.partitions_to_process[asset_name] = [None]

        while len(self.running_tasks) < self.max_concurrency:
            asset_to_run = None
            for asset_name in self.partitions_to_process:
                if asset_name in ready_assets:
                    asset_to_run = asset_name
                    break

            if not asset_to_run:
                break

            partition_key = self.partitions_to_process[asset_to_run].pop(0)

            task_name = (
                f"{asset_to_run} ({partition_key})" if partition_key else asset_to_run
            )
            progress_task_id = self.tui.add_running_task(task_name)

            asset = default_registry.get(asset_to_run)
            context = AssetContext(
                logger=self.tui.logger,
                asset_name=asset_to_run,
                partition_key=partition_key,
            )

            task = asyncio.create_task(asset.execute(context, self.asset_outputs))
            self.running_tasks[task] = (asset_to_run, partition_key, progress_task_id)

            if not self.partitions_to_process[asset_to_run]:
                del self.partitions_to_process[asset_to_run]

    def _process_completed_tasks(self, done_tasks: set[asyncio.Task]):
        for task in done_tasks:
            asset_name, partition_key, progress_task_id = self.running_tasks.pop(task)

            asset_result: AssetResult = task.result()
            self.tui.complete_running_task(progress_task_id, asset_result)

            if asset_result.success:
                if partition_key:
                    self.asset_outputs[asset_name][partition_key] = asset_result.output
                else:
                    self.asset_outputs[asset_name] = asset_result.output

                # If the asset (or all its partitions) are done, mark it in the sorter
                if (
                    asset_name not in self.partitions_to_process
                    and asset_name not in self.done_assets
                ):
                    self.ts.done(asset_name)
                    self.done_assets.add(asset_name)
            else:
                self.flow_failed = True


class Flow:
    """A class representing a workflow of assets."""

    def __init__(self, graph: dict[str, set[str]]):
        self.graph = graph
        self.asset_outputs: dict[str, Any] = defaultdict(dict)
        self.static_order = list(TopologicalSorter(self.graph).static_order())

    async def run_async(
        self,
        run_config: Optional[RunConfig] = None,
        max_concurrency: Optional[int] = None,
    ) -> None:
        """Executes the assets in the flow asynchronously with a concurrency limit."""
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError("max_concurrency must be a positive integer or None.")

        show_flow_tree(self.graph)
        tui = FlowTUIRenderer(total_assets=len(self.static_order))

        with tui:
            manager = AssetExecutionManager(
                self.graph, run_config, max_concurrency, tui, self.asset_outputs
            )
            await manager.run()
