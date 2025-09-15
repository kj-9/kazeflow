import logging
from dataclasses import dataclass


@dataclass
class AssetContext:
    """Holds contextual information for an asset's execution."""

    logger: logging.Logger
    asset_name: str
