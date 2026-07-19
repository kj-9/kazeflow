"""Immutable, metadata-only flow planning models."""

from __future__ import annotations

from dataclasses import dataclass
import heapq
from typing import Any, Hashable, Optional, Sequence

from .assets import AssetRegistry, default_registry


@dataclass(frozen=True)
class PlanConfig:
    """Configuration captured by a :class:`FlowPlan`.

    ``partition_keys`` is ``None`` when no partition selection was supplied.
    An empty tuple is an explicit selection containing no partition keys.
    """

    max_concurrency: Optional[int] = None
    partition_keys: Optional[Sequence[Hashable]] = None

    def __post_init__(self) -> None:
        if self.partition_keys is not None:
            object.__setattr__(self, "partition_keys", tuple(self.partition_keys))


@dataclass(frozen=True)
class TaskPlan:
    """The selected partitions and direct dependencies for one task."""

    name: str
    dependencies: tuple[str, ...]
    partition_keys: Optional[tuple[Hashable, ...]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        if self.partition_keys is not None:
            object.__setattr__(self, "partition_keys", tuple(self.partition_keys))


@dataclass(frozen=True)
class FlowPlan:
    """A deterministic, inspectable description of selected flow work."""

    targets: tuple[str, ...]
    tasks: tuple[TaskPlan, ...]
    config: PlanConfig

    def __post_init__(self) -> None:
        object.__setattr__(self, "targets", tuple(self.targets))
        object.__setattr__(self, "tasks", tuple(self.tasks))


def build_flow_plan(
    targets: list[str] | tuple[str, ...],
    *,
    config: Optional[PlanConfig] = None,
    registry: AssetRegistry = default_registry,
) -> FlowPlan:
    """Build a validated, dependency-first plan without executing assets."""

    normalized_targets = _validate_targets(targets)
    normalized_config = _validate_config(config)
    dependencies_by_name = _collect_closure(normalized_targets, registry)

    has_partitioned_task = any(
        registry.get(name).partition_def is not None for name in dependencies_by_name
    )
    partition_keys = _validate_partition_keys(
        normalized_config.partition_keys, requires_selection=has_partitioned_task
    )
    normalized_config = PlanConfig(
        max_concurrency=normalized_config.max_concurrency,
        partition_keys=partition_keys,
    )

    ordered_names = _topological_order(dependencies_by_name)
    task_plans = tuple(
        TaskPlan(
            name=name,
            dependencies=dependencies_by_name[name],
            partition_keys=(
                partition_keys if registry.get(name).partition_def is not None else None
            ),
        )
        for name in ordered_names
    )
    return FlowPlan(
        targets=normalized_targets,
        tasks=task_plans,
        config=normalized_config,
    )


def _validate_targets(targets: Any) -> tuple[str, ...]:
    if not isinstance(targets, (list, tuple)):
        raise TypeError("targets must be a non-empty list or tuple of strings")
    if not targets:
        raise ValueError("targets must not be empty")
    if any(not isinstance(name, str) for name in targets):
        raise TypeError("targets must contain only strings")
    if any(not name for name in targets):
        raise ValueError("target names must not be empty")
    if len(set(targets)) != len(targets):
        raise ValueError("target names must be unique")
    return tuple(sorted(targets))


def _validate_config(config: Optional[PlanConfig]) -> PlanConfig:
    if config is None:
        config = PlanConfig()
    if not isinstance(config, PlanConfig):
        raise TypeError("config must be a PlanConfig or None")
    max_concurrency = config.max_concurrency
    if max_concurrency is not None and (
        isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int)
    ):
        raise ValueError("max_concurrency must be a positive integer or None")
    if max_concurrency is not None and max_concurrency <= 0:
        raise ValueError("max_concurrency must be a positive integer or None")
    return config


def _validate_partition_keys(
    partition_keys: Optional[Sequence[Hashable]], *, requires_selection: bool
) -> Optional[tuple[Hashable, ...]]:
    if partition_keys is None:
        if requires_selection:
            raise ValueError("partition_keys are required for partitioned tasks")
        return None

    normalized_keys = tuple(partition_keys)
    seen: list[Hashable] = []
    for key in normalized_keys:
        if key is None:
            raise ValueError("partition keys must not contain None")
        try:
            hash(key)
        except TypeError as error:
            raise ValueError("partition keys must be hashable") from error
        if any(key == previous for previous in seen):
            raise ValueError("partition keys must be unique")
        seen.append(key)
    return normalized_keys


def _collect_closure(
    targets: tuple[str, ...], registry: AssetRegistry
) -> dict[str, tuple[str, ...]]:
    dependencies_by_name: dict[str, tuple[str, ...]] = {}
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise ValueError(f"Dependency cycle detected at asset '{name}'")
        if name in dependencies_by_name:
            return

        try:
            asset = registry.get(name)
        except (KeyError, ValueError) as error:
            raise ValueError(f"Asset '{name}' not found") from error

        dependencies = _canonical_dependencies(asset.deps, name)
        visiting.add(name)
        for dependency in dependencies:
            visit(dependency)
        visiting.remove(name)
        dependencies_by_name[name] = dependencies

    for target in targets:
        visit(target)
    return dependencies_by_name


def _canonical_dependencies(dependencies: Any, asset_name: str) -> tuple[str, ...]:
    try:
        normalized_dependencies = tuple(dependencies)
    except TypeError as error:
        raise ValueError(
            f"Dependencies for asset '{asset_name}' are invalid"
        ) from error
    if any(not isinstance(name, str) or not name for name in normalized_dependencies):
        raise ValueError(f"Dependencies for asset '{asset_name}' are invalid")
    if len(set(normalized_dependencies)) != len(normalized_dependencies):
        raise ValueError(f"Dependencies for asset '{asset_name}' are duplicated")
    return tuple(sorted(normalized_dependencies))


def _topological_order(
    dependencies_by_name: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    remaining_dependencies = {
        name: len(dependencies) for name, dependencies in dependencies_by_name.items()
    }
    dependents = {name: [] for name in dependencies_by_name}
    for name, dependencies in dependencies_by_name.items():
        for dependency in dependencies:
            dependents[dependency].append(name)

    ready = [name for name, count in remaining_dependencies.items() if count == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        name = heapq.heappop(ready)
        ordered.append(name)
        for dependent in dependents[name]:
            remaining_dependencies[dependent] -= 1
            if remaining_dependencies[dependent] == 0:
                heapq.heappush(ready, dependent)

    if len(ordered) != len(dependencies_by_name):
        raise ValueError("Dependency cycle detected")
    return tuple(ordered)
