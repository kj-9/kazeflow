import logging

from rich.logging import RichHandler


def get_logger(name: str = __name__) -> logging.Logger:
    """Get a logger that uses rich for pretty printing."""
    logger = logging.getLogger(name)
    if logger.handlers:
        # Logger is already configured
        return logger

    logger.setLevel(logging.INFO)
    handler = RichHandler(show_path=False, rich_tracebacks=True)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
