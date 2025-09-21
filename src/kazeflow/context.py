import logging
from dataclasses import dataclass
from typing import Optional

from .partition import PartitionKey

@dataclass
class AssetContext:
    """Holds contextual information for an asset's execution."""

    logger: logging.Logger
    asset_name: str
    partition_key: Optional[PartitionKey] = None
