import asyncio
from graphlib import TopologicalSorter
from typing import Any, Optional

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.tree import Tree

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

    def _execute_asset(self, asset_name: str, live: Live) -> Any:
        """Executes a single asset and its dependencies."""

        live.console.log(f"Executing asset: {asset_name}")
        asset = get_asset(asset_name)
        asset["func"]()
        live.console.log(f"Finished executing asset: {asset_name}")

    async def _execute_asset_async(self, asset_name: str, live: Live) -> Any:
        """Asynchronously executes a single asset and its dependencies."""

        live.console.log(f"Executing asset: {asset_name}")
        asset = get_asset(asset_name)
        await asset["func"]()
        live.console.log(f"Finished executing asset: {asset_name}")

    def run_sync(self, config: Optional[dict[str, Any]] = None) -> None:
        """Executes the assets in the flow with a progress bar."""

        ts = self._get_ts()
        static_order = list(ts.static_order())

        completed_progress = Progress(
            TextColumn("✓ [green]{task.description}"),
        )
        running_progress = Progress(
            TextColumn("  [purple]Running: {task.description}"),
            SpinnerColumn("simpleDots"),
            TimeElapsedColumn(),
        )
        overall_progress = Progress(
            TextColumn("[bold blue]Overall Progress"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        )

        progress_group = Group(
            Panel(Group(completed_progress, running_progress), title="Assets"),
            overall_progress,
        )

        with Live(progress_group) as live:
            overall_task_id = overall_progress.add_task(
                "Assets", total=len(static_order)
            )

            for asset_name in static_order:
                task_id = running_progress.add_task(asset_name, total=1)
                self._execute_asset(asset_name, live)
                running_progress.stop_task(task_id)
                running_progress.update(task_id, visible=False)
                completed_progress.add_task(asset_name)
                overall_progress.update(overall_task_id, advance=1)

    async def run_async(self, config: Optional[dict[str, Any]] = None) -> list[str]:
        """Executes the assets in the flow with a progress bar."""
        ts = self._get_ts()
        all_assets = list(TopologicalSorter(self.graph).static_order())
        ts.prepare()
        records = []

        completed_progress = Progress(
            TextColumn("✓ [green]{task.description}"),
        )
        running_progress = Progress(
            TextColumn("  [purple]Running: {task.description}"),
            SpinnerColumn("simpleDots"),
            TimeElapsedColumn(),
        )
        overall_progress = Progress(
            TextColumn("[bold blue]Overall Progress"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        )

        progress_group = Group(
            Panel(Group(completed_progress, running_progress), title="Assets"),
            overall_progress,
        )

        running_tasks_map: dict[asyncio.Task, tuple[str, int]] = {}

        with Live(progress_group) as live:
            overall_task_id = overall_progress.add_task("Assets", total=len(all_assets))

            ready_to_run = ts.get_ready()
            for name in ready_to_run:
                progress_task_id = running_progress.add_task(name, total=1)
                async_task = asyncio.create_task(self._execute_asset_async(name, live))
                running_tasks_map[async_task] = (name, progress_task_id)

            while running_tasks_map:
                done, _ = await asyncio.wait(
                    running_tasks_map.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    asset_name, progress_task_id = running_tasks_map.pop(task)

                    running_progress.stop_task(progress_task_id)
                    running_progress.update(progress_task_id, visible=False)

                    completed_progress.add_task(f"{asset_name}")

                    overall_progress.update(overall_task_id, advance=1)

                    ts.done(asset_name)

                    newly_ready = ts.get_ready()
                    for name in newly_ready:
                        new_progress_task_id = running_progress.add_task(name, total=1)
                        new_async_task = asyncio.create_task(
                            self._execute_asset_async(name, live)
                        )
                        running_tasks_map[new_async_task] = (name, new_progress_task_id)

        return records
