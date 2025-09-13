from typing import Any


class Config:
    """A simple class for asset configuration."""

    def __init__(self, config: dict[str, Any]):
        self._config = config

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieves a configuration value."""
        return self._config.get(key, default)
