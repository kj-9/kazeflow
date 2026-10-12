"""Immutable partition-group declarations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, init=False)
class PartitionGroup:
    """Bound the selected partition keys progressing through an asset chain.

    ``assets`` are ordered from the upstream asset to the downstream asset.
    The flow planner checks that the names form a valid partitioned chain.
    """

    assets: tuple[str, ...]
    max_active_keys: int = 1

    def __init__(self, assets: Sequence[str], max_active_keys: int = 1) -> None:
        if isinstance(assets, (str, bytes, bytearray)) or not isinstance(
            assets, Sequence
        ):
            raise TypeError("PartitionGroup assets must be a sequence of names")
        frozen_assets = tuple(assets)
        if len(frozen_assets) < 2:
            raise ValueError("PartitionGroup must contain at least two assets")
        if any(not isinstance(name, str) for name in frozen_assets):
            raise TypeError("PartitionGroup asset names must be strings")
        if any(not name for name in frozen_assets):
            raise ValueError("PartitionGroup asset names must not be empty")
        if len(set(frozen_assets)) != len(frozen_assets):
            raise ValueError("PartitionGroup asset names must be distinct")
        object.__setattr__(self, "assets", frozen_assets)
        limit = max_active_keys
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("max_active_keys must be a positive integer")
        object.__setattr__(self, "max_active_keys", limit)
