from typing import Any, Dict, Set

from rich.console import Console, Group
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

from .logger import get_logger
from .assets import AssetResult


def show_flow_tree(graph: Dict[str, Set[str]]) -> None:
    """Displays the task flow as a rich tree, in execution order."""

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

    def add_to_tree(parent_tree: Tree, node_name: str) -> None:
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


class FlowTUIRenderer:
    """Manages the Text-based User Interface for flow execution using rich."""

    def __init__(self, total_assets: int):
        self.completed_progress = Progress(TextColumn("✓ [green]{task.description}"))
        self.failed_progress = Progress(TextColumn("✗ [red]{task.description}"))
        self.running_progress = Progress(
            TextColumn("  [purple]Running: {task.description}"),
            SpinnerColumn("simpleDots"),
            TimeElapsedColumn(),
        )
        self.overall_progress = Progress(
            TextColumn("[bold blue]Overall Progress"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        )
        self.progress_group = Group(
            Panel(
                Group(
                    self.completed_progress,
                    self.failed_progress,
                    self.running_progress,
                ),
                title="Assets",
            ),
            self.overall_progress,
        )
        self.overall_task_id = self.overall_progress.add_task(
            "Assets", total=total_assets
        )
        self.live = Live(self.progress_group)
        self.logger = get_logger(__name__, console=self.live.console)
        self.results: list[AssetResult] = []

    def __enter__(self) -> Live:
        Console().print("\n[bold underline green]Execution Logs[/bold underline green]")
        return self.live.__enter__()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # Sort results by start time before final render
        self.results.sort(key=lambda r: r.start_time)

        # Group results by asset name
        grouped_results: Dict[str, list[AssetResult]] = {}
        for r in self.results:
            if r.name not in grouped_results:
                grouped_results[r.name] = []
            grouped_results[r.name].append(r)

        for name, results in grouped_results.items():
            # check if any result has a partition key
            is_partitioned = any(r.partition_key is not None for r in results)

            if is_partitioned:
                total_duration = sum(r.duration for r in results)
                avg_duration = total_duration / len(results)
                success_count = sum(1 for r in results if r.success)
                status_icon = "✓" if success_count == len(results) else "✗"
                progress_bar = (
                    self.completed_progress
                    if status_icon == "✓"
                    else self.failed_progress
                )
                description = (
                    f"{name} ({len(results)} partitions, avg: {avg_duration:.2f}s)"
                )
                progress_bar.add_task(description)
            else:
                # Non-partitioned asset (should only have one result)
                result = results[0]
                description = f"{result.name:<30} ({result.duration:.2f}s)"
                if result.success:
                    self.completed_progress.add_task(description)
                else:
                    self.failed_progress.add_task(description)

        return self.live.__exit__(exc_type, exc_val, exc_tb)

    def add_running_task(self, name: str) -> int:
        """Adds a task to the running progress bar."""
        return self.running_progress.add_task(name, total=1)

    def complete_running_task(self, task_id: int, result: AssetResult) -> None:
        """Moves a task from running to a temporary list for later sorting."""
        self.running_progress.stop_task(task_id)
        self.running_progress.update(task_id, visible=False)

        # Store the result for final sorting
        self.results.append(result)

        self.overall_progress.update(self.overall_task_id, advance=1)
