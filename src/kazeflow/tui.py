"""Optional Rich presentation adapters for kazeflow execution events.

This module deliberately sits outside the core execution import graph.  Callers opt
in by constructing :class:`FlowTUIRenderer`, entering it, and passing it as an
``event_consumer`` to ``run`` or ``Flow.run_async``.
"""

from __future__ import annotations

from types import TracebackType
from typing import Mapping, Sequence

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.tree import Tree

from .events import EventKind, ExecutionEvent
from .plan import FlowPlan, TaskPlan
from .results import AttemptReference, AttemptStatus


def show_flow_tree(graph: Mapping[str, Sequence[str] | set[str]]) -> None:
    """Render the legacy dependency graph form as a Rich tree.

    New callers should prefer :func:`show_plan_tree`, which accepts the deterministic
    ``FlowPlan`` model.  Keeping this small compatibility adapter avoids requiring
    legacy callers to migrate graph construction at the same time as presentation.
    """

    reversed_graph = {node: set() for node in graph}
    for node, dependencies in graph.items():
        for dependency in dependencies:
            if dependency in reversed_graph:
                reversed_graph[dependency].add(node)

    root_nodes = [node for node, dependencies in graph.items() if not dependencies]
    tree = Tree("[bold green]Task Flow (Execution Order)[/bold green]")
    added_nodes: set[str] = set()

    def add_to_tree(parent_tree: Tree, node_name: str) -> None:
        if node_name in added_nodes:
            return
        added_nodes.add(node_name)
        node_tree = parent_tree.add(node_name)
        for dependent_node in sorted(reversed_graph.get(node_name, ())):
            add_to_tree(node_tree, dependent_node)

    for root in sorted(root_nodes):
        add_to_tree(tree, root)
    Console().print(tree)


def show_plan_tree(plan: FlowPlan, *, console: Console | None = None) -> None:
    """Render an inspectable plan without executing any asset."""

    graph = {task.name: task.dependencies for task in plan.tasks}
    render_console = console or Console()
    reversed_graph = {node: set() for node in graph}
    for node, dependencies in graph.items():
        for dependency in dependencies:
            reversed_graph[dependency].add(node)

    tree = Tree("[bold green]Task Flow (Execution Order)[/bold green]")
    added_nodes: set[str] = set()

    def add_to_tree(parent_tree: Tree, node_name: str) -> None:
        if node_name in added_nodes:
            return
        added_nodes.add(node_name)
        node_tree = parent_tree.add(node_name)
        for dependent_node in sorted(reversed_graph[node_name]):
            add_to_tree(node_tree, dependent_node)

    for root in sorted(
        node for node, dependencies in graph.items() if not dependencies
    ):
        add_to_tree(tree, root)
    render_console.print(tree)


class FlowTUIRenderer:
    """An explicitly selected Rich ``ExecutionEventConsumer``.

    The renderer owns display state only.  It never reads a flow, result, asset
    output, exception object, or executor progress identifier; every update is based
    on the neutral event delivered to :meth:`on_event`.
    """

    def __init__(
        self,
        total_assets: int | None = None,
        *,
        plan: FlowPlan | None = None,
        console: Console | None = None,
    ) -> None:
        if plan is not None:
            if total_assets is not None and total_assets != len(plan.tasks):
                raise ValueError("total_assets must match the supplied plan")
            total_assets = len(plan.tasks)
        if total_assets is not None and total_assets < 0:
            raise ValueError("total_assets must be non-negative or None")
        self.task_state_progress = Progress(TextColumn("{task.description}"))
        self.completed_progress = Progress(TextColumn("✓ [green]{task.description}"))
        self.failed_progress = Progress(TextColumn("✗ [red]{task.description}"))
        self.skipped_progress = Progress(TextColumn("– [yellow]{task.description}"))
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
                    self.task_state_progress,
                    self.completed_progress,
                    self.failed_progress,
                    self.skipped_progress,
                    self.running_progress,
                ),
                title="Assets",
            ),
            self.overall_progress,
        )
        self.overall_task_id = self.overall_progress.add_task(
            "Assets", total=total_assets
        )
        self.live = Live(self.progress_group, console=console or Console())
        self.events: list[ExecutionEvent] = []
        self._planned_tasks = (
            {task.name: task for task in plan.tasks} if plan is not None else {}
        )
        self._task_state_ids = {
            task.name: self.task_state_progress.add_task(
                self._task_state_label(task, "○ Waiting")
            )
            for task in (plan.tasks if plan is not None else ())
        }
        self._running_task_ids: dict[AttemptReference, TaskID] = {}
        self._finished_tasks: set[str] = set()

    def __enter__(self) -> FlowTUIRenderer:
        self.live.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # Cancellation and consumer errors may leave the event stream as a prefix.
        # Live can always be closed; no terminal event is assumed here.
        self.live.__exit__(exc_type, exc_val, exc_tb)

    def on_event(self, event: ExecutionEvent) -> None:
        """Update presentation state from one neutral lifecycle event."""

        self.events.append(event)
        if event.kind is EventKind.TASK_STARTED:
            assert event.task is not None
            self._mark_task_running(event.task.task_name)
        elif event.kind is EventKind.ATTEMPT_STARTED:
            assert event.attempt is not None
            self._start_attempt(event.attempt)
        elif event.kind is EventKind.ATTEMPT_FINISHED:
            assert event.attempt is not None
            assert isinstance(event.status, AttemptStatus)
            self._finish_attempt(event.attempt, event.status)
        elif event.kind is EventKind.TASK_FINISHED:
            assert event.task is not None
            assert isinstance(event.status, AttemptStatus)
            self._finish_task(event.task.task_name, event.status)

    def _start_attempt(self, attempt: AttemptReference) -> None:
        self._running_task_ids[attempt] = self.running_progress.add_task(
            _attempt_label(attempt), total=1
        )

    def _finish_attempt(self, attempt: AttemptReference, status: AttemptStatus) -> None:
        task_id = self._running_task_ids.pop(attempt, None)
        if task_id is not None:
            self.running_progress.stop_task(task_id)
            self.running_progress.update(task_id, visible=False)
        self._terminal_progress(status).add_task(_attempt_label(attempt))

    def _finish_task(self, task_name: str, status: AttemptStatus) -> None:
        # Task-finished is the aggregate lifecycle event, so it is the only event
        # that advances total flow progress.  It also covers blocked/no-work tasks,
        # which legitimately have no attempt-start event.
        if task_name in self._finished_tasks:
            return
        self._finished_tasks.add(task_name)
        self._mark_task_terminal(task_name, status)
        self.overall_progress.update(self.overall_task_id, advance=1)
        if (
            not any(
                attempt.task.task_name == task_name
                for attempt in self._running_task_ids
            )
            and status is not AttemptStatus.SUCCESS
        ):
            self._terminal_progress(status).add_task(task_name)

    def _terminal_progress(self, status: AttemptStatus) -> Progress:
        if status is AttemptStatus.SUCCESS:
            return self.completed_progress
        if status is AttemptStatus.SKIPPED:
            return self.skipped_progress
        return self.failed_progress

    def _mark_task_running(self, task_name: str) -> None:
        task = self._planned_tasks.get(task_name)
        task_id = self._task_state_ids.get(task_name)
        if task is not None and task_id is not None:
            self.task_state_progress.update(
                task_id, description=self._task_state_label(task, "● Running")
            )

    def _mark_task_terminal(self, task_name: str, status: AttemptStatus) -> None:
        task = self._planned_tasks.get(task_name)
        task_id = self._task_state_ids.get(task_name)
        if task is None or task_id is None:
            return
        marker = {
            AttemptStatus.SUCCESS: "✓ Succeeded",
            AttemptStatus.SKIPPED: "– Skipped",
            AttemptStatus.FAILED: "✗ Failed",
            AttemptStatus.CANCELLED: "– Cancelled",
        }[status]
        self.task_state_progress.update(
            task_id, description=self._task_state_label(task, marker)
        )

    @staticmethod
    def _task_state_label(task: TaskPlan, state: str) -> str:
        partition = (
            ""
            if task.partition_keys is None
            else f" [partitions: {len(task.partition_keys)}]"
        )
        return f"{state}: {task.name}{partition}"


def _attempt_label(attempt: AttemptReference) -> str:
    """Return a visible, falsey-safe label without accessing raw execution state."""

    if attempt.partition_key_present:
        return f"{attempt.task.task_name} [partitioned]"
    return attempt.task.task_name
