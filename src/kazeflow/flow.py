import asyncio
from graphlib import TopologicalSorter
from typing import Any, Optional

from rich.tree import Tree
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

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

    def show_flow_tree(self) -> None:
        """Displays the task flow as a rich tree."""
        self._get_ts()
        graph = self.graph

        # Find root nodes (nodes with no dependencies)
        all_deps = set()
        for deps in graph.values():
            all_deps.update(deps)
        root_nodes = [node for node in graph.keys() if node not in all_deps]

        tree = Tree("[bold green]Task Flow[/bold green]")
        added_nodes = set()

        def add_to_tree(parent_tree, node_name):
            if node_name in added_nodes:
                return
            added_nodes.add(node_name)
            node_tree = parent_tree.add(node_name)
            for dep in graph.get(node_name, []):
                add_to_tree(node_tree, dep)

        for root in root_nodes:
            add_to_tree(tree, root)

        print(tree)

    def _get_ts(self) -> TopologicalSorter:
        """Sets up the topological sorter based on asset dependencies."""
        graph: dict[str, set[str]] = {}

        queue = list(self.asset_names)
        visited = set()

        while queue:
            asset_name = queue.pop(0)
            if asset_name in visited:
                continue
            visited.add(asset_name)

            asset = get_asset(asset_name)
            deps = set(asset["deps"])
            graph[asset_name] = deps

            for dep in deps:
                queue.append(dep)

        self.graph = graph
        ts = TopologicalSorter(self.graph)
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
        progress: Progress,
    ) -> Any:
        """Asynchronously executes a single asset and its dependencies."""

        progress.log(f"Executing asset: {asset_name}")
        asset = get_asset(asset_name)
        await asset["func"]()
        progress.log(f"Finished executing asset: {asset_name}")

    def run_sync(self, config: Optional[dict[str, Any]] = None) -> None:
        """Executes the assets in the flow with a progress bar."""

        ts = self._get_ts()
        static_order = list(ts.static_order())

        with Progress() as progress:
            tasks = {
                asset_name: progress.add_task(
                    f"[cyan]Executing {asset_name}[/cyan]", total=1
                )
                for asset_name in static_order
            }

            for asset_name in static_order:
                self._execute_asset(asset_name)
                progress.update(tasks[asset_name], advance=1)

    async def run_async(
        self, config: Optional[dict[str, Any]] = None, num_workers=3
    ) -> list[str]:
        ts = self._get_ts()
        all_assets = list(TopologicalSorter(self.graph).static_order())
        ts.prepare()
        records = []

        progress_columns = [
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        ]

        with Progress(*progress_columns) as progress:
            total_task = progress.add_task("Overall progress", total=len(all_assets))

            # Individual tasks, initially not started
            individual_tasks = {
                name: progress.add_task(name, start=False, total=1, transient=True)
                for name in all_assets
            }

            ready_to_run = ts.get_ready()
            for name in ready_to_run:
                progress.start_task(individual_tasks[name])

            running_tasks = {
                asyncio.create_task(self._execute_asset_async(name, progress)): name
                for name in ready_to_run
            }

            while running_tasks:
                done, _ = await asyncio.wait(
                    running_tasks.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    asset_name = running_tasks.pop(task)
                    progress.update(individual_tasks[asset_name], completed=1)
                    progress.update(total_task, advance=1)

                    ts.done(asset_name)

                    newly_ready = ts.get_ready()
                    for name in newly_ready:
                        progress.start_task(individual_tasks[name])
                        new_task = asyncio.create_task(
                            self._execute_asset_async(name, progress)
                        )
                        running_tasks[new_task] = name

        return records
