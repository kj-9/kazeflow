"""Plan-driven, presentation-neutral core flow execution."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import time
import traceback
from typing import Any, Optional, TypedDict
from uuid import uuid4

from .assets import AssetContext, AssetRegistry, default_registry
from .events import (
    EventKind,
    ExecutionEvent,
    ExecutionEventConsumer,
    NoOpExecutionEventConsumer,
    validate_event_sequence,
)
from .plan import FlowPlan, PlanConfig, TaskPlan, build_flow_plan
from .partition import PartitionKeys
from .results import (
    AttemptReference,
    AttemptResult,
    AttemptStatus,
    FailureInfo,
    FlowStatus,
    RunResult,
    SkipReason,
    TaskReference,
    TaskResult,
)


class RunConfig(TypedDict, total=False):
    partition_keys: PartitionKeys
    partition_range: PartitionKeys
    max_concurrency: int


@dataclass(slots=True)
class _AttemptState:
    reference: AttemptReference
    partition_key: object | None
    result: AttemptResult | None = None
    future: asyncio.Task[object] | None = None
    started_at: datetime | None = None
    started_monotonic: float | None = None


@dataclass(slots=True)
class _TaskState:
    plan: TaskPlan
    reference: TaskReference
    attempts: list[_AttemptState]
    started_at: datetime | None = None
    started_monotonic: float | None = None
    result: TaskResult | None = None


class Flow:
    """An explicit target set and its exact asset registry.

    Passing a graph is a checked, deprecated migration form.  It is retained only
    so older callers can move to direct targets without changing execution meaning.
    """

    def __init__(
        self,
        targets: list[str] | tuple[str, ...] | dict[str, set[str]],
        *,
        registry: AssetRegistry = default_registry,
    ) -> None:
        self.registry = registry
        self.graph: dict[str, set[str]] = {}
        self._legacy_graph: dict[str, set[str]] | None = None
        if isinstance(targets, dict):
            if not all(
                isinstance(name, str)
                and isinstance(dependencies, set)
                and all(isinstance(dependency, str) for dependency in dependencies)
                for name, dependencies in targets.items()
            ):
                raise TypeError("legacy graph must be a dict[str, set[str]]")
            self._legacy_graph = {
                name: set(dependencies) for name, dependencies in targets.items()
            }
            self.graph = {
                name: set(dependencies) for name, dependencies in targets.items()
            }
            dependency_names = {
                dependency
                for dependencies in targets.values()
                for dependency in dependencies
            }
            self.targets = tuple(
                sorted(name for name in targets if name not in dependency_names)
            )
        elif isinstance(targets, (list, tuple)):
            self.targets = _canonical_targets(targets)
        else:
            raise TypeError("targets must be a list or tuple of strings")
        # A compatibility mirror only.  Execution never consumes this mapping.
        self.asset_outputs: dict[str, Any] = {}

    def plan(self, run_config: Optional[RunConfig] = None) -> FlowPlan:
        """Return the fully validated plan without invoking an asset."""
        config = _plan_config(run_config)
        plan = build_flow_plan(self.targets, config=config, registry=self.registry)
        if self._legacy_graph is not None:
            _validate_legacy_graph(self._legacy_graph, plan)
        return plan

    async def run_async(
        self,
        run_config: Optional[RunConfig] = None,
        *,
        event_consumer: ExecutionEventConsumer | None = None,
    ) -> RunResult:
        """Execute a preflighted plan and return its terminal result."""
        plan = self.plan(run_config)
        return await self._run_plan_async(plan, event_consumer=event_consumer)

    async def _run_plan_async(
        self,
        plan: FlowPlan,
        *,
        event_consumer: ExecutionEventConsumer | None = None,
    ) -> RunResult:
        """Execute one already validated plan without normalizing it again."""
        self.asset_outputs = {}
        executor = _Executor(plan, self.registry, self.asset_outputs, event_consumer)
        return await executor.run()


def run(
    asset_names: list[str] | tuple[str, ...],
    run_config: Optional[RunConfig] = None,
    *,
    registry: AssetRegistry = default_registry,
    event_consumer: ExecutionEventConsumer | None = None,
) -> RunResult:
    """Synchronously run explicit targets, unless this thread already has a loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            "run() cannot be called from an active event loop; await run_async()"
        )
    return asyncio.run(
        Flow(asset_names, registry=registry).run_async(
            run_config, event_consumer=event_consumer
        )
    )


def _plan_config(run_config: Optional[RunConfig]) -> PlanConfig:
    if run_config is None:
        return PlanConfig()
    if not isinstance(run_config, dict):
        raise TypeError("run_config must be a RunConfig or None")
    unknown = set(run_config) - {
        "max_concurrency",
        "partition_keys",
        "partition_range",
    }
    if unknown:
        raise ValueError(f"unknown run configuration keys: {sorted(unknown)!r}")
    return PlanConfig(
        max_concurrency=run_config.get("max_concurrency"),
        partition_keys=run_config.get("partition_keys"),
        partition_range=run_config.get("partition_range"),
    )


def _canonical_targets(targets: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    if not targets:
        raise ValueError("targets must not be empty")
    if any(not isinstance(name, str) for name in targets):
        raise TypeError("targets must contain only strings")
    if any(not name for name in targets):
        raise ValueError("target names must not be empty")
    if len(set(targets)) != len(targets):
        raise ValueError("target names must be unique")
    return tuple(sorted(targets))


def _validate_legacy_graph(graph: dict[str, set[str]], plan: FlowPlan) -> None:
    planned = {task.name: set(task.dependencies) for task in plan.tasks}
    if set(graph) != set(planned) or any(
        graph[name] != dependencies for name, dependencies in planned.items()
    ):
        raise ValueError("legacy Flow(graph) must exactly match the registry plan")


class _Executor:
    def __init__(
        self,
        plan: FlowPlan,
        registry: AssetRegistry,
        asset_outputs: dict[str, Any],
        consumer: ExecutionEventConsumer | None,
    ) -> None:
        self.plan = plan
        self.registry = registry
        self.asset_outputs = asset_outputs
        self.consumer: ExecutionEventConsumer = (
            consumer if consumer is not None else NoOpExecutionEventConsumer()
        )
        self.run_id = str(uuid4())
        self.events: list[ExecutionEvent] = []
        self._sequence = 0
        self._running: dict[asyncio.Task[object], _AttemptState] = {}
        self.tasks = {task.name: self._make_task_state(task) for task in plan.tasks}

    def _make_task_state(self, task: TaskPlan) -> _TaskState:
        reference = TaskReference(task.name)
        if task.partition_keys is None:
            attempts = [_AttemptState(AttemptReference(reference), None)]
        else:
            attempts = [
                _AttemptState(
                    AttemptReference(
                        reference, partition_key_present=True, partition_key=key
                    ),
                    key,
                )
                for key in task.partition_keys
            ]
        return _TaskState(task, reference, attempts)

    async def run(self) -> RunResult:
        flow_started_at = _utc_now()
        flow_started_monotonic = time.monotonic()
        self._emit(EventKind.FLOW_STARTED, status=FlowStatus.RUNNING)
        try:
            await self._drain()
        except BaseException:
            await self._cancel_running()
            raise

        ended_at = _utc_now()
        duration = timedelta(seconds=time.monotonic() - flow_started_monotonic)
        task_results = tuple(self.tasks[task.name].result for task in self.plan.tasks)
        assert all(result is not None for result in task_results)
        completed_tasks = tuple(result for result in task_results if result is not None)
        status = (
            FlowStatus.FAILED
            if any(task.status is AttemptStatus.FAILED for task in completed_tasks)
            else FlowStatus.SUCCESS
        )
        result = RunResult(
            self.run_id, status, flow_started_at, ended_at, duration, completed_tasks
        )
        self._emit(EventKind.FLOW_FINISHED, status=status)
        validate_event_sequence(self.events)
        return result

    async def _cancel_running(self) -> None:
        running = tuple(self._running)
        for future in running:
            future.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        self._running.clear()

    async def _drain(self) -> None:
        limit = self.plan.config.max_concurrency or max(
            1, sum(len(task.attempts) for task in self.tasks.values())
        )
        while not all(task.result is not None for task in self.tasks.values()):
            changed = self._finalize_ready_tasks()
            while len(self._running) < limit:
                selected = self._next_action()
                if selected is None:
                    break
                state, action = selected
                self._start_task(state)
                if action == "blocked":
                    self._finish_blocked(state)
                    changed = True
                    continue
                attempt = action
                assert isinstance(attempt, _AttemptState)
                self._start_attempt(state, attempt)
                changed = True
            if self._running:
                done, _ = await asyncio.wait(
                    self._running, return_when=asyncio.FIRST_COMPLETED
                )
                for future in done:
                    attempt = self._running.pop(future)
                    self._finish_running_attempt(attempt, future)
                continue
            if changed:
                continue
            raise RuntimeError(
                "valid FlowPlan contains work that cannot become eligible"
            )

    def _next_action(self) -> tuple[_TaskState, _AttemptState | str] | None:
        for task_plan in self.plan.tasks:
            task = self.tasks[task_plan.name]
            if task.result is not None:
                continue
            for attempt in task.attempts:
                if attempt.result is not None or attempt.future is not None:
                    continue
                eligibility = self._eligibility(task, attempt)
                if eligibility == "ready":
                    return task, attempt
                if eligibility == "blocked":
                    return task, "blocked"
        return None

    def _start_task(self, task: _TaskState) -> None:
        if task.started_at is None:
            task.started_at = _utc_now()
            task.started_monotonic = time.monotonic()
            self._emit(
                EventKind.TASK_STARTED,
                task=task.reference,
                status=AttemptStatus.RUNNING,
            )

    def _start_attempt(self, task: _TaskState, attempt: _AttemptState) -> None:
        self._emit(
            EventKind.ATTEMPT_STARTED,
            attempt=attempt.reference,
            status=AttemptStatus.RUNNING,
        )
        attempt.started_at = _utc_now()
        attempt.started_monotonic = time.monotonic()
        inputs = self._inputs(task, attempt)
        asset = self.registry.get(task.plan.name)
        context = AssetContext(
            task.plan.name, logging.getLogger("kazeflow.assets"), attempt.partition_key
        )
        future = asyncio.create_task(asset.invoke(context, inputs))
        attempt.future = future
        self._running[future] = attempt

    def _finish_running_attempt(
        self, attempt: _AttemptState, future: asyncio.Task[object]
    ) -> None:
        attempt.future = None
        ended_at = _utc_now()
        started_at = attempt.started_at or ended_at
        duration = timedelta(
            seconds=max(
                0.0, time.monotonic() - (attempt.started_monotonic or time.monotonic())
            )
        )
        try:
            output = future.result()
        except Exception as error:
            failure = FailureInfo(
                type(error).__name__,
                str(error),
                "".join(traceback.format_exception(error)),
            )
            attempt.result = AttemptResult(
                attempt.reference,
                AttemptStatus.FAILED,
                started_at,
                ended_at,
                duration,
                exception=error,
                failure=failure,
            )
            self._emit(
                EventKind.ATTEMPT_FINISHED,
                attempt=attempt.reference,
                status=AttemptStatus.FAILED,
                failure=failure,
            )
        else:
            attempt.result = AttemptResult(
                attempt.reference,
                AttemptStatus.SUCCESS,
                started_at,
                ended_at,
                duration,
                output=output,
            )
            self._store_compat_output(
                self.tasks[attempt.reference.task.task_name], attempt, output
            )
            self._emit(
                EventKind.ATTEMPT_FINISHED,
                attempt=attempt.reference,
                status=AttemptStatus.SUCCESS,
            )

    def _finish_blocked(self, task: _TaskState) -> None:
        attempt = next(
            candidate
            for candidate in task.attempts
            if candidate.result is None and candidate.future is None
        )
        blockers = self._blockers(task, attempt)
        now = _utc_now()
        started = task.started_at or now
        duration = timedelta(
            seconds=max(
                0.0, time.monotonic() - (task.started_monotonic or time.monotonic())
            )
        )
        attempt.result = AttemptResult(
            attempt.reference,
            AttemptStatus.SKIPPED,
            started,
            now,
            duration,
            reason=SkipReason.DEPENDENCY_BLOCKED,
            blocked_by=blockers,
        )
        self._emit(
            EventKind.ATTEMPT_FINISHED,
            attempt=attempt.reference,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.DEPENDENCY_BLOCKED,
            blocked_by=blockers,
        )

    def _finalize_ready_tasks(self) -> bool:
        changed = False
        for task in self.tasks.values():
            if task.result is None:
                if not task.attempts:
                    self._finalize_task(task, no_work=True)
                    changed = True
                elif all(attempt.result is not None for attempt in task.attempts):
                    self._finalize_task(task)
                    changed = True
        return changed

    def _finalize_task(self, task: _TaskState, *, no_work: bool = False) -> None:
        now = _utc_now()
        started = task.started_at or now
        duration = timedelta(
            seconds=max(
                0.0, time.monotonic() - (task.started_monotonic or time.monotonic())
            )
        )
        if no_work:
            result = TaskResult(
                task.reference,
                True,
                AttemptStatus.SKIPPED,
                started,
                now,
                duration,
                reason=SkipReason.NO_PARTITION_KEYS,
            )
        else:
            attempts = tuple(attempt.result for attempt in task.attempts)
            assert all(attempt is not None for attempt in attempts)
            completed = tuple(attempt for attempt in attempts if attempt is not None)
            if any(attempt.status is AttemptStatus.FAILED for attempt in completed):
                status, reason, blockers = AttemptStatus.FAILED, None, ()
            elif any(attempt.status is AttemptStatus.SKIPPED for attempt in completed):
                status = AttemptStatus.SKIPPED
                if all(
                    attempt.status is AttemptStatus.SKIPPED for attempt in completed
                ):
                    reason = SkipReason.DEPENDENCY_BLOCKED
                    blockers = tuple(
                        blocker
                        for attempt in completed
                        for blocker in attempt.blocked_by
                    )
                else:
                    reason, blockers = None, ()
            else:
                status, reason, blockers = AttemptStatus.SUCCESS, None, ()
            result = TaskResult(
                task.reference,
                task.plan.partition_keys is not None,
                status,
                started,
                now,
                duration,
                completed,
                reason,
                blockers,
            )
        task.result = result
        event_reason = result.reason
        event_blockers = result.blocked_by
        # The result model intentionally leaves a mixed partition aggregate
        # unattributed.  The event model requires every skipped finish to carry a
        # reason, so its lifecycle observation identifies the blocked attempts.
        if result.status is AttemptStatus.SKIPPED and event_reason is None:
            event_reason = SkipReason.DEPENDENCY_BLOCKED
            event_blockers = tuple(
                blocker
                for completed_attempt in result.attempts
                for blocker in completed_attempt.blocked_by
            )
        self._emit(
            EventKind.TASK_FINISHED,
            task=task.reference,
            status=result.status,
            reason=event_reason,
            blocked_by=event_blockers,
        )

    def _eligibility(self, task: _TaskState, attempt: _AttemptState) -> str:
        for dependency_name in task.plan.dependencies:
            dependency = self.tasks[dependency_name]
            if (
                task.plan.partition_keys is None
                and dependency.plan.partition_keys is not None
            ):
                if dependency.result is None:
                    return "wait"
                if (
                    dependency.result.status is AttemptStatus.SUCCESS
                    or dependency.result.reason is SkipReason.NO_PARTITION_KEYS
                ):
                    continue
                return "blocked"
            if (
                dependency.plan.partition_keys is not None
                and task.plan.partition_keys is not None
            ):
                matching = next(
                    (
                        item
                        for item in dependency.attempts
                        if item.partition_key == attempt.partition_key
                    ),
                    None,
                )
                if matching is None or matching.result is None:
                    return "wait"
                if matching.result.status is not AttemptStatus.SUCCESS:
                    return "blocked"
                continue
            if dependency.result is None:
                return "wait"
            if dependency.result.status is not AttemptStatus.SUCCESS:
                return "blocked"
        return "ready"

    def _blockers(
        self, task: _TaskState, attempt: _AttemptState
    ) -> tuple[AttemptReference, ...]:
        blockers: list[AttemptReference] = []
        for dependency_name in task.plan.dependencies:
            dependency = self.tasks[dependency_name]
            if (
                task.plan.partition_keys is not None
                and dependency.plan.partition_keys is not None
            ):
                matching = next(
                    item
                    for item in dependency.attempts
                    if item.partition_key == attempt.partition_key
                )
                if (
                    matching.result is not None
                    and matching.result.status is not AttemptStatus.SUCCESS
                ):
                    blockers.append(matching.reference)
            elif (
                dependency.result is not None
                and dependency.result.status is not AttemptStatus.SUCCESS
            ):
                blockers.extend(
                    item.reference
                    for item in dependency.attempts
                    if item.result is not None
                    and item.result.status is not AttemptStatus.SUCCESS
                )
        return tuple(blockers)

    def _inputs(self, task: _TaskState, attempt: _AttemptState) -> dict[str, Any]:
        inputs: dict[str, Any] = {}
        for dependency_name in task.plan.dependencies:
            dependency = self.tasks[dependency_name]
            if dependency.plan.partition_keys is None:
                assert dependency.attempts[0].result is not None
                inputs[dependency_name] = dependency.attempts[0].result.output
            elif task.plan.partition_keys is None:
                if (
                    dependency.result is not None
                    and dependency.result.reason is SkipReason.NO_PARTITION_KEYS
                ):
                    inputs[dependency_name] = {}
                else:
                    inputs[dependency_name] = {
                        item.partition_key: item.result.output
                        for item in dependency.attempts
                        if item.result is not None
                    }
            else:
                matching = next(
                    item
                    for item in dependency.attempts
                    if item.partition_key == attempt.partition_key
                )
                assert matching.result is not None
                inputs[dependency_name] = {
                    attempt.partition_key: matching.result.output
                }
        return inputs

    def _store_compat_output(
        self, task: _TaskState, attempt: _AttemptState, output: object
    ) -> None:
        if task.plan.partition_keys is None:
            self.asset_outputs[task.plan.name] = output
        else:
            self.asset_outputs.setdefault(task.plan.name, {})[attempt.partition_key] = (
                output
            )

    def _emit(self, kind: EventKind, **kwargs: Any) -> None:
        self._sequence += 1
        event = ExecutionEvent(self.run_id, self._sequence, _utc_now(), kind, **kwargs)
        self.events.append(event)
        self.consumer.on_event(event)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
