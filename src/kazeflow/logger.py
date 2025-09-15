import logging
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


def get_logger(
    name: str = __name__, console: Optional[Console] = None
) -> logging.Logger:
    """Get a logger that uses rich for pretty printing."""
    logger = logging.getLogger(name)
    # Clear existing handlers to allow for reconfiguration
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)
    handler = RichHandler(
        show_path=False, rich_tracebacks=True, console=console, show_time=False
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger
