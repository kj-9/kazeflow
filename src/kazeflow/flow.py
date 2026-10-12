"""Plan-driven, presentation-neutral core flow execution."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections.abc import Iterator, Mapping, Sequence
import logging
import inspect
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
from .groups import PartitionGroup
from .plan import FlowPlan, PlanConfig, TaskPlan, build_flow_plan
from .materialization import (
    MaterializationContext,
    call_materialization,
    record_matches_context,
)
from .partition import PartitionKeys
from .results import (
    AttemptReference,
    AttemptResult,
    AttemptStatus,
    FailureInfo,
    FlowStatus,
    InvocationResult,
    RunResult,
    SkipReason,
    TaskReference,
    TaskResult,
)
from .resources import Fail, OpenCircuit, ResourcePolicy, Retry, _RateWindow


class RunConfig(TypedDict, total=False):
    partition_keys: PartitionKeys
    partition_range: PartitionKeys
    max_concurrency: int


@dataclass(slots=True)
class _AttemptState:
    reference: AttemptReference
    partition_key: object | None
    result: AttemptResult | None = None
    future: asyncio.Task[_InvocationOutcome | _PreparedInvocation] | None = None
    started_at: datetime | None = None
    started_monotonic: float | None = None
    prepared: _PreparedInvocation | None = None
    resource_reserved: bool = False
    ready_at: float = 0.0
    invocation_number: int = 0
    invocation_started_at: datetime | None = None
    invocation_started_monotonic: float | None = None
    invocations: tuple[InvocationResult, ...] = ()


class _BodyFailure(Exception):
    """Identify retryable body failures separately from adapter failures."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(str(error))


@dataclass(frozen=True, slots=True)
class _InvocationOutcome:
    output: object
    resolution: str
    materialization_id: str | None = None


@dataclass(frozen=True, slots=True)
class _PreparedInvocation:
    context: AssetContext
    inputs: dict[str, Any]
    identity: MaterializationContext


@dataclass(slots=True)
class _TaskState:
    plan: TaskPlan
    reference: TaskReference
    attempts: list[_AttemptState]
    started_at: datetime | None = None
    started_monotonic: float | None = None
    result: TaskResult | None = None


@dataclass(slots=True)
class _GroupState:
    declaration: PartitionGroup
    active: list[int]
    next_index: int = 0


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
        resources: Mapping[str, ResourcePolicy] | None = None,
        partition_groups: Sequence[PartitionGroup] | None = None,
    ) -> None:
        self.registry = registry
        self.resources = dict(resources) if resources is not None else {}
        if partition_groups is not None and (
            isinstance(partition_groups, (str, bytes, bytearray))
            or not isinstance(partition_groups, Sequence)
        ):
            raise TypeError("partition_groups must be a sequence of PartitionGroup")
        self.partition_groups = (
            tuple(partition_groups) if partition_groups is not None else ()
        )
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
        plan = build_flow_plan(
            self.targets,
            config=config,
            registry=self.registry,
            resources=self.resources,
            partition_groups=self.partition_groups,
        )
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
        executor = _Executor(
            plan, self.registry, self.asset_outputs, event_consumer, self.resources
        )
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
        resources: Mapping[str, ResourcePolicy],
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
        self._running: dict[
            asyncio.Task[_InvocationOutcome | _PreparedInvocation], _AttemptState
        ] = {}
        self._resource_running: dict[str, int] = {}
        self._policies = {
            resource.name: resources[resource.name] for resource in plan.resources
        }
        self._cooldowns: dict[str, float] = {}
        self._circuits: dict[str, AttemptReference] = {}
        self._resource_limits = {
            resource.name: resource.max_concurrency for resource in plan.resources
        }
        self._rate_windows = {
            resource.name: _RateWindow(resource.rate_limit)
            for resource in plan.resources
            if resource.rate_limit is not None
        }
        self._rate_deadline: float | None = None
        self._admission_wakeup: asyncio.Future[None] = (
            asyncio.get_running_loop().create_future()
        )
        self.tasks = {task.name: self._make_task_state(task) for task in plan.tasks}
        self._groups = {
            group.assets[0]: _GroupState(group, []) for group in plan.partition_groups
        }
        self._group_members = {
            name for group in plan.partition_groups for name in group.assets
        }

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
                state, attempt, reason = selected
                self._start_task(state)
                if reason is not None:
                    self._finish_blocked(state, attempt, reason)
                    changed = True
                    continue
                self._start_attempt(state, attempt)
                changed = True
            if self._running:
                # A body records its actual launch time after create_task runs.
                # That wakeup lets us compute a deadline even if it remains busy.
                timeout = None
                if len(self._running) < limit and self._rate_deadline is not None:
                    timeout = max(0.0, self._rate_deadline - time.monotonic())
                done, _ = await asyncio.wait(
                    (*self._running, self._admission_wakeup),
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self._admission_wakeup in done:
                    self._admission_wakeup = asyncio.get_running_loop().create_future()
                for future in tuple(self._running):
                    if future in done:
                        attempt = self._running.pop(future)
                        self._finish_running_attempt(attempt, future)
                continue
            if self._rate_deadline is not None:
                await asyncio.sleep(max(0.0, self._rate_deadline - time.monotonic()))
                continue
            if changed:
                continue
            raise RuntimeError(
                "valid FlowPlan contains work that cannot become eligible"
            )

    def _next_action(
        self,
    ) -> tuple[_TaskState, _AttemptState, SkipReason | None] | None:
        self._rate_deadline = None
        now = time.monotonic()
        for task, attempt in self._action_candidates():
            if attempt.result is not None or attempt.future is not None:
                continue
            eligibility = self._eligibility(task, attempt)
            if eligibility == "ready":
                if task.plan.materialization_enabled and attempt.prepared is None:
                    return task, attempt, None
                resource = task.plan.resource_name
                if resource in self._circuits:
                    return task, attempt, SkipReason.RESOURCE_CIRCUIT_OPEN
                if (
                    resource is not None
                    and self._resource_running.get(resource, 0)
                    >= self._resource_limits[resource]
                ):
                    continue
                window = (
                    self._rate_windows.get(resource) if resource is not None else None
                )
                ready_at = max(attempt.ready_at, self._cooldowns.get(resource, 0.0))
                if window is not None and not window.ready(now):
                    deadline = window.deadline(now)
                    if deadline is not None:
                        self._wait_until(max(ready_at, deadline))
                    continue
                if ready_at > now:
                    self._wait_until(ready_at)
                    continue
                return task, attempt, None
            if eligibility == "blocked":
                return task, attempt, SkipReason.DEPENDENCY_BLOCKED
        return None

    def _action_candidates(self) -> Iterator[tuple[_TaskState, _AttemptState]]:
        """Prefer the next stage of admitted keys, retaining ordinary outside order."""
        for task_plan in self.plan.tasks:
            group = self._groups.get(task_plan.name)
            if group is not None:
                members = [self.tasks[name] for name in group.declaration.assets]
                group.active[:] = [
                    index
                    for index in group.active
                    if not all(
                        member.attempts[index].result is not None for member in members
                    )
                ]
                while len(
                    group.active
                ) < group.declaration.max_active_keys and group.next_index < len(
                    members[0].attempts
                ):
                    group.active.append(group.next_index)
                    group.next_index += 1
                for index in group.active:
                    for member in members:
                        attempt = member.attempts[index]
                        if attempt.result is None:
                            yield member, attempt
                            break
            elif task_plan.name not in self._group_members:
                task = self.tasks[task_plan.name]
                if task.result is None:
                    for attempt in task.attempts:
                        yield task, attempt

    def _wait_until(self, deadline: float) -> None:
        self._rate_deadline = (
            deadline
            if self._rate_deadline is None
            else min(self._rate_deadline, deadline)
        )

    def _retry_enabled(self, resource: str | None) -> bool:
        policy = self._policies.get(resource) if resource is not None else None
        return policy is not None and (
            policy.max_attempts > 1 or policy.classify_failure is not None
        )

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
        if attempt.started_at is None:
            self._emit(
                EventKind.ATTEMPT_STARTED,
                attempt=attempt.reference,
                status=AttemptStatus.RUNNING,
            )
            attempt.started_at = _utc_now()
            attempt.started_monotonic = time.monotonic()
        preparing = task.plan.materialization_enabled and attempt.prepared is None
        resource = task.plan.resource_name
        if not preparing and resource is not None:
            window = self._rate_windows.get(resource)
            if window is not None:
                window.reserve(time.monotonic())
            attempt.resource_reserved = True
            self._resource_running[resource] = (
                self._resource_running.get(resource, 0) + 1
            )
        if not preparing:
            attempt.invocation_number += 1
            if self._retry_enabled(resource):
                attempt.invocation_started_at = _utc_now()
                attempt.invocation_started_monotonic = time.monotonic()
                self._emit(
                    EventKind.INVOCATION_STARTED,
                    attempt=attempt.reference,
                    status=AttemptStatus.RUNNING,
                    invocation_number=attempt.invocation_number,
                )
        future = asyncio.create_task(self._run_attempt(task, attempt, preparing))
        attempt.future = future
        self._running[future] = attempt

    async def _run_attempt(
        self,
        task: _TaskState,
        attempt: _AttemptState,
        preparing: bool,
    ) -> _InvocationOutcome | _PreparedInvocation:
        if preparing:
            return await self._prepare_materialization(task, attempt)
        return await self._invoke_body(task, attempt)

    async def _prepare_materialization(
        self, task: _TaskState, attempt: _AttemptState
    ) -> _InvocationOutcome | _PreparedInvocation:
        asset = self.registry.get(task.plan.name)
        adapter = asset.materialization
        assert adapter is not None and asset.version is not None
        assert task.plan.partition_domain is not None
        input_ids: list[tuple[str, str]] = []
        for dependency_name in task.plan.dependencies:
            dependency = self.tasks[dependency_name]
            matching = next(
                item
                for item in dependency.attempts
                if item.partition_key == attempt.partition_key
            )
            assert matching.result is not None
            committed_id = matching.result.materialization_id
            assert committed_id is not None
            input_ids.append((dependency_name, committed_id))
        identity = MaterializationContext(
            asset_name=task.plan.name,
            version=asset.version,
            partition_domain=task.plan.partition_domain,
            partition_key_present=attempt.reference.partition_key_present,
            partition_key=attempt.partition_key,
            input_materialization_ids=tuple(input_ids),
        )
        candidate = await call_materialization(adapter.resolve, identity)
        if record_matches_context(candidate, identity):
            output = await call_materialization(adapter.load, candidate)
            return _InvocationOutcome(
                output, "materialized", candidate.materialization_id
            )
        context = AssetContext(
            task.plan.name, logging.getLogger("kazeflow.assets"), attempt.partition_key
        )
        return _PreparedInvocation(context, self._inputs(task, attempt), identity)

    async def _invoke_body(
        self, task: _TaskState, attempt: _AttemptState
    ) -> _InvocationOutcome:
        asset = self.registry.get(task.plan.name)
        prepared = attempt.prepared
        context = (
            prepared.context
            if prepared is not None
            else AssetContext(
                task.plan.name,
                logging.getLogger("kazeflow.assets"),
                attempt.partition_key,
            )
        )
        inputs = (
            prepared.inputs if prepared is not None else self._inputs(task, attempt)
        )
        resource = task.plan.resource_name
        window = self._rate_windows.get(resource) if resource is not None else None
        if window is not None:
            window.record_start(time.monotonic())
            if not self._admission_wakeup.done():
                self._admission_wakeup.set_result(None)
        context.invocation_number = attempt.invocation_number
        try:
            output = await asset.invoke(context, inputs)
        except Exception as error:
            raise _BodyFailure(error) from error
        if prepared is None:
            return _InvocationOutcome(output, "executed")
        adapter = asset.materialization
        assert adapter is not None
        committed = await call_materialization(
            adapter.commit, prepared.identity, output
        )
        if not record_matches_context(committed, prepared.identity):
            raise ValueError(
                f"materialization commit for asset '{task.plan.name}' returned an invalid or uncommitted record"
            )
        return _InvocationOutcome(output, "executed", committed.materialization_id)

    def _finish_running_attempt(
        self,
        attempt: _AttemptState,
        future: asyncio.Task[_InvocationOutcome | _PreparedInvocation],
    ) -> None:
        attempt.future = None
        task_state = self.tasks[attempt.reference.task.task_name]
        resource = task_state.plan.resource_name
        if attempt.resource_reserved and resource is not None:
            attempt.resource_reserved = False
            remaining = self._resource_running[resource] - 1
            if remaining:
                self._resource_running[resource] = remaining
            else:
                del self._resource_running[resource]
        ended_at = _utc_now()
        started_at = attempt.started_at or ended_at
        duration = timedelta(
            seconds=max(
                0.0, time.monotonic() - (attempt.started_monotonic or time.monotonic())
            )
        )
        try:
            outcome = future.result()
        except Exception as error:
            body_failed = isinstance(error, _BodyFailure)
            original = error.error if isinstance(error, _BodyFailure) else error
            original_failure = _failure_info(original)
            action: Fail | Retry | OpenCircuit = Fail()
            classification_error: Exception | None = None
            policy = self._policies.get(resource) if resource is not None else None
            if (
                body_failed
                and policy is not None
                and policy.classify_failure is not None
            ):
                try:
                    selected = policy.classify_failure(original)
                    if isinstance(selected, Retry):
                        action = Retry(
                            selected.after_seconds, selected.cooldown_resource
                        )
                    elif isinstance(selected, OpenCircuit):
                        action = OpenCircuit()
                    elif isinstance(selected, Fail):
                        action = Fail()
                    else:
                        if inspect.iscoroutine(selected):
                            selected.close()
                        raise TypeError(
                            "failure classifier must return Fail, Retry or OpenCircuit"
                        )
                except Exception as callback_error:
                    classification_error = callback_error
            if isinstance(action, OpenCircuit) and resource is not None:
                self._circuits.setdefault(resource, attempt.reference)
            now = time.monotonic()
            cooldown = None
            if (
                isinstance(action, Retry)
                and action.cooldown_resource
                and resource is not None
            ):
                cooldown = action.after_seconds
                self._cooldowns[resource] = max(
                    self._cooldowns.get(resource, 0.0), now + cooldown
                )
            retry_delay = (
                action.after_seconds
                if isinstance(action, Retry)
                and classification_error is None
                and policy is not None
                and attempt.invocation_number < policy.max_attempts
                else None
            )
            self._record_invocation(
                attempt,
                AttemptStatus.FAILED,
                ended_at,
                failure=original_failure,
                retry_delay=retry_delay,
                cooldown=cooldown,
            )
            if retry_delay is not None:
                attempt.ready_at = now + retry_delay
                return
            terminal_error = (
                classification_error if classification_error is not None else original
            )
            failure = _failure_info(terminal_error)
            attempt.result = AttemptResult(
                attempt.reference,
                AttemptStatus.FAILED,
                started_at,
                ended_at,
                duration,
                exception=terminal_error,
                failure=failure,
                invocations=attempt.invocations,
            )
            self._emit(
                EventKind.ATTEMPT_FINISHED,
                attempt=attempt.reference,
                status=AttemptStatus.FAILED,
                failure=failure,
            )
        else:
            if isinstance(outcome, _PreparedInvocation):
                attempt.prepared = outcome
                return
            self._record_invocation(attempt, AttemptStatus.SUCCESS, ended_at)
            attempt.result = AttemptResult(
                attempt.reference,
                AttemptStatus.SUCCESS,
                started_at,
                ended_at,
                duration,
                output=outcome.output,
                resolution=outcome.resolution,
                materialization_id=outcome.materialization_id,
                invocations=attempt.invocations,
            )
            self._store_compat_output(
                self.tasks[attempt.reference.task.task_name], attempt, outcome.output
            )
            self._emit(
                EventKind.ATTEMPT_FINISHED,
                attempt=attempt.reference,
                status=AttemptStatus.SUCCESS,
                resolution=outcome.resolution,
                materialization_id=outcome.materialization_id,
            )

    def _record_invocation(
        self,
        attempt: _AttemptState,
        status: AttemptStatus,
        ended_at: datetime,
        *,
        failure: FailureInfo | None = None,
        retry_delay: float | None = None,
        cooldown: float | None = None,
    ) -> None:
        if attempt.invocation_started_at is None:
            return
        assert attempt.invocation_started_monotonic is not None
        invocation = InvocationResult(
            attempt.invocation_number,
            status,
            attempt.invocation_started_at,
            ended_at,
            timedelta(
                seconds=max(
                    0.0, time.monotonic() - attempt.invocation_started_monotonic
                )
            ),
            failure=failure,
            retry_delay_seconds=retry_delay,
            cooldown_seconds=cooldown,
        )
        attempt.invocations += (invocation,)
        self._emit(
            EventKind.INVOCATION_FINISHED,
            attempt=attempt.reference,
            status=status,
            failure=failure,
            invocation_number=attempt.invocation_number,
            retry_delay_seconds=retry_delay,
            cooldown_seconds=cooldown,
        )

    def _finish_blocked(
        self, task: _TaskState, attempt: _AttemptState, reason: SkipReason
    ) -> None:
        if reason is SkipReason.RESOURCE_CIRCUIT_OPEN:
            assert task.plan.resource_name is not None
            blockers = (self._circuits[task.plan.resource_name],)
        else:
            blockers = self._blockers(task, attempt)
        now = _utc_now()
        started = attempt.started_at or task.started_at or now
        duration = timedelta(
            seconds=max(
                0.0,
                time.monotonic()
                - (
                    attempt.started_monotonic
                    or task.started_monotonic
                    or time.monotonic()
                ),
            )
        )
        attempt.result = AttemptResult(
            attempt.reference,
            AttemptStatus.SKIPPED,
            started,
            now,
            duration,
            reason=reason,
            blocked_by=blockers,
            invocations=attempt.invocations,
        )
        self._emit(
            EventKind.ATTEMPT_FINISHED,
            attempt=attempt.reference,
            status=AttemptStatus.SKIPPED,
            reason=reason,
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
                    reasons = {attempt.reason for attempt in completed}
                    reason = (
                        next(iter(reasons))
                        if len(reasons) == 1
                        else SkipReason.MIXED_BLOCKED
                    )
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
            reasons = {
                attempt.reason
                for attempt in result.attempts
                if attempt.status is AttemptStatus.SKIPPED
            }
            event_reason = (
                next(iter(reasons)) if len(reasons) == 1 else SkipReason.MIXED_BLOCKED
            )
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


def _failure_info(error: Exception) -> FailureInfo:
    return FailureInfo(
        type(error).__name__, str(error), "".join(traceback.format_exception(error))
    )
