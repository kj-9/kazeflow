from typing import List

import pytest

from kazeflow.assets import _assets, asset
from kazeflow.config import Config
from kazeflow.flow import Flow


@asset()
def raw_data() -> List[int]:
    return [1, 2, 3, 4, 5]


@asset(deps=["raw_data"])
def processed_data(raw_data: List[int]) -> int:
    return sum(raw_data)


@asset(deps=["raw_data"], config_schema=Config(config={}))
def configured_data(config: Config, raw_data: List[int]) -> int:
    multiplier = config.get("multiplier", 1)
    return sum(raw_data) * multiplier


def test_asset_deps():
    assert "raw_data" in _assets
    assert _assets["raw_data"]["deps"] == []

    assert "processed_data" in _assets
    assert _assets["processed_data"]["deps"] == ["raw_data"]

    assert "configured_data" in _assets
    assert _assets["configured_data"]["deps"] == ["raw_data"]
    assert _assets["configured_data"]["config_schema"] is not None


def test_simple_flow():
    flow = Flow(asset_names=["processed_data"])
    results = flow.run()
    assert results["raw_data"] == [1, 2, 3, 4, 5]
    assert results["processed_data"] == 15


def test_flow_with_config():
    flow = Flow(asset_names=["configured_data"])
    config = {"configured_data": {"multiplier": 3}}
    results = flow.run(config=config)
    assert results["raw_data"] == [1, 2, 3, 4, 5]
    assert results["configured_data"] == 45


def test_flow_with_missing_dependency():
    with pytest.raises(ValueError, match="Asset 'non_existent_asset' not found."):
        flow = Flow(asset_names=["non_existent_asset"])
        flow.run()
