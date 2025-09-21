import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, Union

from .partition import PartitionKey


class NamedCallable(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


@dataclass
class AssetContext:
    """Holds contextual information for an asset's execution."""

    logger: logging.Logger
    asset_name: str
    partition_key: Optional[PartitionKey] = None


@dataclass
class AssetResult:
    """Holds the result of a single asset's execution."""

    name: str
    success: bool
    duration: float
    start_time: float
    output: Optional[Any] = None
    exception: Optional[Exception] = None


class Asset:
    """Represents a single asset, including its metadata and execution logic."""

    def __init__(
        self,
        func: NamedCallable,
        deps: list[str],
        partition_by: Optional[PartitionKey] = None,
    ):
        self.func = func
        self.deps = deps
        self.partition_by = partition_by
        self.name = func.__name__

    async def execute(
        self, context: AssetContext, asset_outputs: dict[str, Any]
    ) -> AssetResult:
        """Executes the asset and returns a result object."""
        start_time = time.monotonic()
        output = None
        exception = None
        success = False
        try:
            context.logger.info(f"Executing asset: {self.name}")

            sig = inspect.signature(self.func)
            params = sig.parameters
            input_kwargs = {
                dep: asset_outputs[dep]
                for dep in self.deps
                if dep in asset_outputs and dep in params
            }

            if "context" in params:
                input_kwargs["context"] = context

            if asyncio.iscoroutinefunction(self.func):
                output = await self.func(**input_kwargs)
            else:
                loop = asyncio.get_running_loop()
                import functools

                p = functools.partial(self.func, **input_kwargs)
                output = await loop.run_in_executor(None, p)
            success = True

        except Exception as e:
            exception = e
            context.logger.exception(f"Error executing asset {self.name}: {e}")

        duration = time.monotonic() - start_time
        if success:
            context.logger.info(
                f"Finished executing asset: {self.name} in {duration:.2f}s"
            )

        return AssetResult(
            name=self.name,
            success=success,
            duration=duration,
            start_time=start_time,
            output=output,
            exception=exception,
        )


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

        asset_obj = Asset(
            func=func,
            deps=list(resolved_deps),
            partition_by=partition_by,
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
