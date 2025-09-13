from typing import Any, Optional, Protocol, TypedDict

from .config import Config


class NamedCallable(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class AssetMeta(TypedDict):
    func: NamedCallable
    deps: list[str]
    config_schema: Optional[Config]


_assets: dict[str, AssetMeta] = {}


def asset(
    deps: Optional[list[str]] = None,
    config_schema: Optional[Config] = None,
):
    """
    A decorator to define an asset, its dependencies, and its configuration schema.
    """

    def decorator(func: NamedCallable) -> NamedCallable:
        _assets[func.__name__] = {
            "func": func,
            "deps": deps or [],
            "config_schema": config_schema,
        }
        return func

    return decorator


def get_asset(name: str) -> AssetMeta:
    """Retrieves an asset's metadata including its function, dependencies,
    and config schema."""
    if name not in _assets:
        raise ValueError(f"Asset '{name}' not found.")
    return _assets[name]
