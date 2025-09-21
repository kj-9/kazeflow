import inspect
from typing import Any, Callable, Optional, Protocol, TypedDict, Union

from .partition import PartitionKey
from dataclasses import dataclass
import logging


class NamedCallable(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class Asset(TypedDict):
    func: NamedCallable
    deps: list[str]
    partition_by: Optional[Any]


@dataclass
class AssetContext:
    """Holds contextual information for an asset's execution."""

    logger: logging.Logger
    asset_name: str
    partition_key: Optional[PartitionKey] = None


class AssetRegistry:
    """Manages the registration and retrieval of assets."""

    def __init__(self):
        self._assets: dict[str, Asset] = {}

    def register(
        self,
        func: NamedCallable,
        deps: Optional[list[str]] = None,
        partition_by: Optional[PartitionKey] = None,
    ):
        """Registers an asset."""
        resolved_deps = set(deps or [])

        sig = inspect.signature(func)
        for param in sig.parameters.values():
            if param.name in ("context", "config", "partition_key"):
                continue
            if param.annotation is AssetContext:
                continue
            resolved_deps.add(param.name)

        self._assets[func.__name__] = Asset(
            {
                "func": func,
                "deps": list(resolved_deps),
                "partition_by": partition_by,
            }
        )

    def get(self, name: str) -> Asset:
        """Retrieves an asset's metadata."""
        if name not in self._assets:
            raise ValueError(f"Asset '{name}' not found.")
        return self._assets[name]

    def clear(self) -> None:
        """Clears all registered assets."""
        self._assets.clear()

    def build_graph(self, asset_names: list[str]) -> dict[str, set[str]]:
        """Builds a dependency graph for a list of assets."""
        graph: dict[str, set[str]] = {}
        queue = list(asset_names)
        visited = set()

        while queue:
            asset_name = queue.pop(0)
            if asset_name in visited:
                continue
            visited.add(asset_name)

            asset = self.get(asset_name)
            deps = set(asset["deps"])
            graph[asset_name] = deps

            for dep in deps:
                queue.append(dep)

        return graph


# Default global registry
default_registry = AssetRegistry()


def asset(
    _func: Optional[NamedCallable] = None,
    *,
    deps: Optional[list[str]] = None,
    partition_by: Optional[PartitionKey] = None,
) -> Union[Callable[[NamedCallable], NamedCallable], NamedCallable]:
    """
    A decorator to define an asset, its dependencies, and its configuration schema.
    """

    def decorator(func: NamedCallable) -> NamedCallable:
        default_registry.register(func, deps=deps, partition_by=partition_by)
        return func

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def get_asset(name: str) -> Asset:
    """Retrieves an asset's metadata including its function, dependencies,
    and config schema."""
    return default_registry.get(name)


def clear_assets() -> None:
    """Clears all registered assets.

    This is useful for testing purposes.
    """
    default_registry.clear()


def build_graph(asset_names: list[str]) -> dict[str, set[str]]:
    """Builds a dependency graph for a list of assets."""
    return default_registry.build_graph(asset_names)


@dataclass
class AssetResult:
    """Holds the result of a single asset's execution."""

    name: str
    success: bool
    duration: float
    start_time: float
    output: Optional[Any] = None
    exception: Optional[Exception] = None
