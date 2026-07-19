import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, Union

from .partition import PartitionDef, PartitionKey


class NamedCallable(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


@dataclass
class AssetContext:
    """Holds contextual information for an asset's execution."""

    asset_name: str
    logger: logging.Logger
    partition_key: Optional[PartitionKey]


@dataclass
class AssetResult:
    """Holds the result of a single asset's execution."""

    name: str
    success: bool
    duration: float
    start_time: float
    partition_key: Optional[PartitionKey] = None
    output: Optional[Any] = None
    exception: Optional[Exception] = None


class Asset:
    """Represents a single asset, including its metadata and execution logic."""

    def __init__(
        self,
        func: NamedCallable,
        deps: list[str],
        partition_def: Optional[PartitionDef] = None,
    ):
        self.func = func
        self.deps = deps
        self.partition_def = partition_def
        self.name = func.__name__

    async def invoke(self, context: AssetContext, inputs: dict[str, Any]) -> Any:
        """Invoke the plain asset callable with already-resolved inputs.

        Exception handling, lifecycle timing, and presentation belong to the flow
        executor.  Keeping this method neutral lets direct calls to the decorated
        function retain their normal Python behaviour.
        """
        params = inspect.signature(self.func).parameters
        input_kwargs = {name: value for name, value in inputs.items() if name in params}
        if "context" in params:
            input_kwargs["context"] = context

        if asyncio.iscoroutinefunction(self.func):
            return await self.func(**input_kwargs)

        loop = asyncio.get_running_loop()
        import functools

        return await loop.run_in_executor(
            None, functools.partial(self.func, **input_kwargs)
        )


class AssetRegistry:
    """Manages the registration and retrieval of assets."""

    def __init__(self):
        self._assets: dict[str, Asset] = {}

    def register(
        self,
        func: NamedCallable,
        deps: Optional[list[str]] = None,
        partition_def: Optional[PartitionDef] = None,
    ):
        """Registers an asset."""
        resolved_deps = set(deps or [])

        sig = inspect.signature(func)
        for param in sig.parameters.values():
            if param.name in ("context"):
                continue
            if param.annotation is AssetContext:
                continue
            resolved_deps.add(param.name)

        asset_obj = Asset(
            func=func,
            deps=list(resolved_deps),
            partition_def=partition_def,
        )
        self._assets[func.__name__] = asset_obj

    def get(self, name: str) -> Asset:
        """Retrieves an asset."""
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
            deps = set(asset.deps)
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
    partition_def: Optional[PartitionDef] = None,
) -> Union[Callable[[NamedCallable], NamedCallable], NamedCallable]:
    """
    A decorator to define an asset, its dependencies, and its configuration schema.
    """

    def decorator(func: NamedCallable) -> NamedCallable:
        default_registry.register(func, deps=deps, partition_def=partition_def)
        return func

    if _func is None:
        return decorator
    else:
        return decorator(_func)
