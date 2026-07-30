"""Compatibility helpers for callers that configure standard-library logging."""

import logging
from typing import Any


def get_logger(name: str = __name__, console: Any = None) -> logging.Logger:
    """Return a standard-library logger without changing global configuration.

    Rich-specific logging moved to the optional TUI adapter.  ``console`` is retained
    as an ignored compatibility argument; handler selection remains with callers.
    """

    del console
    return logging.getLogger(name)
