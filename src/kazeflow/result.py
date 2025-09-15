from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class AssetResult:
    """Holds the result of a single asset's execution."""

    name: str
    success: bool
    duration: float
    start_time: float
    output: Optional[Any] = None
    exception: Optional[Exception] = None
