import inspect
from typing import Any, Callable, Optional, Protocol, TypedDict, Union

from .context import AssetContext
from .partition import PartitionKey


class NamedCallable(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class Asset(TypedDict):
    func: NamedCallable
    deps: list[str]
    partition_by: Optional[Any]


_assets: dict[str, Asset] = {}


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
        # Start with explicit deps, or an empty list
        resolved_deps = set(deps or [])

        # Add implicit deps from signature
        sig = inspect.signature(func)
        for param in sig.parameters.values():
            # Do not add special runtime-provided params to deps
            if param.name in ("context", "config", "partition_key"):
                continue
            if param.annotation is AssetContext:
                continue

            resolved_deps.add(param.name)

        _assets[func.__name__] = Asset(
            {
                "func": func,
                "deps": list(resolved_deps),
                "partition_by": partition_by,
            }
        )
        return func

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def get_asset(name: str) -> Asset:
    """Retrieves an asset's metadata including its function, dependencies,
    and config schema."""
    if name not in _assets:
        raise ValueError(f"Asset '{name}' not found.")
    return _assets[name]


def clear_assets() -> None:
    """Clears all registered assets.

    This is useful for testing purposes.
    """
    _assets.clear()
