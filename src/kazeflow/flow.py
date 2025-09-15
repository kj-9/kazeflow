import asyncio
import inspect
from graphlib import TopologicalSorter
from typing import Any, Optional

from rich.console import Console
from rich.live import Live
from rich.tree import Tree
from rich.traceback import Traceback

from .assets import get_asset
from .logger import get_logger
from .tui import FlowTUIRenderer

logger = get_logger(__name__)


class Flow:
    """A class representing a workflow of assets."""

    graph: dict[str, set[str]]
    ts: TopologicalSorter
    static_order: list[str]

    def __init__(self, asset_names: list[str]):
        self.asset_names = asset_names
        self.asset_outputs: dict[str, Any] = {}

    def show_flow_tree(self) -> None:
        """Displays the task flow as a rich tree, in execution order."""
        self._get_ts()
        graph = self.graph

        # Reverse the graph to show data flow from dependencies to dependents
        reversed_graph = {node: set() for node in graph}
        for node, deps in graph.items():
            for dep in deps:
                if dep in reversed_graph:
                    reversed_graph[dep].add(node)

        # Find root nodes (assets with no dependencies)
        root_nodes = [node for node, deps in graph.items() if not deps]

        tree = Tree("[bold green]Task Flow (Execution Order)[/bold green]")
        added_nodes = set()

        def add_to_tree(parent_tree, node_name):
            if node_name in added_nodes:
                return
            added_nodes.add(node_name)
            node_tree = parent_tree.add(node_name)
            # Use reversed_graph to find nodes that depend on the current one
            for dependent_node in sorted(list(reversed_graph.get(node_name, []))):
                add_to_tree(node_tree, dependent_node)

        for root in sorted(root_nodes):
            add_to_tree(tree, root)

        Console().print(tree)

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

    def _execute_asset(self, asset_name: str, live: Live) -> bool:
        """Executes a single asset.

        Returns:
            bool: True if the asset executed successfully, False otherwise.
        """
        try:
            live.console.log(f"Executing asset: {asset_name}")
            asset = get_asset(asset_name)

            asset_func = asset["func"]
            deps = asset["deps"]

            # Only pass outputs that are actual parameters of the asset function
            sig = inspect.signature(asset_func)
            params = sig.parameters
            input_kwargs = {
                dep: self.asset_outputs[dep]
                for dep in deps
                if dep in self.asset_outputs and dep in params
            }

            if asyncio.iscoroutinefunction(asset_func):
                live.console.log(
                    f"[bold red]Cannot run async asset '{asset_name}' in run_sync. "
                    "Please use run_async.[/bold red]"
                )
                return False

            output = asset_func(**input_kwargs)
            self.asset_outputs[asset_name] = output

            live.console.log(f"Finished executing asset: {asset_name}")
            return True
        except Exception:
            live.console.log(f"[bold red]Error executing asset {asset_name}[/bold red]")
            live.console.print(Traceback(show_locals=True))
            return False

    async def _execute_asset_async(self, asset_name: str, live: Live) -> Any:
        """Asynchronously executes a single asset and its dependencies."""
        try:
            live.console.log(f"Executing asset: {asset_name}")
            asset = get_asset(asset_name)

            asset_func = asset["func"]
            deps = asset["deps"]

            # Only pass outputs that are actual parameters of the asset function
            sig = inspect.signature(asset_func)
            params = sig.parameters
            input_kwargs = {
                dep: self.asset_outputs[dep]
                for dep in deps
                if dep in self.asset_outputs and dep in params
            }

            if asyncio.iscoroutinefunction(asset_func):
                output = await asset_func(**input_kwargs)
            else:
                # Run sync function in a thread pool executor to avoid blocking the event loop
                loop = asyncio.get_running_loop()
                # functools.partial is needed to pass keyword arguments to run_in_executor
                import functools

                p = functools.partial(asset_func, **input_kwargs)
                output = await loop.run_in_executor(None, p)

            self.asset_outputs[asset_name] = output

            live.console.log(f"Finished executing asset: {asset_name}")
        except Exception:
            live.console.log(f"[bold red]Error executing asset {asset_name}[/bold red]")
            live.console.print(Traceback(show_locals=True))
            raise

    def run_sync(self, config: Optional[dict[str, Any]] = None) -> None:
        """Executes the assets in the flow synchronously."""
        self.show_flow_tree()
        ts = self._get_ts()
        static_order = list(ts.static_order())

        tui = FlowTUIRenderer(total_assets=len(static_order))
        with tui as live:
            for asset_name in static_order:
                task_id = tui.add_running_task(asset_name)
                success = self._execute_asset(asset_name, live)
                tui.complete_running_task(task_id, asset_name, success)

                if not success:
                    live.console.log(
                        f"[bold red]Asset '{asset_name}' failed. Stopping workflow.[/bold red]"
                    )
                    break

    async def run_async(self, config: Optional[dict[str, Any]] = None) -> list[str]:
        """Executes the assets in the flow asynchronously."""
        self.show_flow_tree()
        ts = self._get_ts()
        all_assets = list(TopologicalSorter(self.graph).static_order())
        ts.prepare()
        records = []

        tui = FlowTUIRenderer(total_assets=len(all_assets))
        running_tasks_map: dict[asyncio.Task, tuple[str, int]] = {}

        with tui as live:
            ready_to_run = ts.get_ready()
            for name in ready_to_run:
                progress_task_id = tui.add_running_task(name)
                async_task = asyncio.create_task(self._execute_asset_async(name, live))
                running_tasks_map[async_task] = (name, progress_task_id)

            while running_tasks_map:
                done, _ = await asyncio.wait(
                    running_tasks_map.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    asset_name, progress_task_id = running_tasks_map.pop(task)
                    exc = task.exception()
                    success = exc is None

                    tui.complete_running_task(progress_task_id, asset_name, success)

                    if success:
                        ts.done(asset_name)
                        newly_ready = ts.get_ready()
                        for name in newly_ready:
                            new_progress_task_id = tui.add_running_task(name)
                            new_async_task = asyncio.create_task(
                                self._execute_asset_async(name, live)
                            )
                            running_tasks_map[new_async_task] = (
                                name,
                                new_progress_task_id,
                            )

        return records
