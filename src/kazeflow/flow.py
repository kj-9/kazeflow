import logging
from typing import Any, Optional

from .assets import get_asset
from .config import Config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


class Flow:
    """A class representing a workflow of assets."""

    def __init__(self, asset_names: list[str]):
        self.asset_names = asset_names

    def run(self, config: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Executes the assets in the flow."""
        results: dict[str, Any] = {}
        for asset_name in self.asset_names:
            results[asset_name] = self._execute_asset(asset_name, results, config or {})
        return results

    def _execute_asset(
        self,
        asset_name: str,
        results: dict[str, Any],
        config: dict[str, Any],
    ) -> Any:
        """Executes a single asset and its dependencies."""
        if asset_name in results:
            return results[asset_name]

        logger.info(f"Executing asset: {asset_name}")

        asset_info = get_asset(asset_name)
        deps: list[str] = asset_info.get("deps", [])

        resolved_deps: list[Any] = []
        for dep_name in deps:
            resolved_deps.append(self._execute_asset(dep_name, results, config))

        asset_config = config.get(asset_name)
        if asset_config:
            config_instance = Config(asset_config)
            result = asset_info["func"](config_instance, *resolved_deps)
        else:
            result = asset_info["func"](*resolved_deps)

        results[asset_name] = result
        logger.info(f"Finished executing asset: {asset_name}")
        return result
