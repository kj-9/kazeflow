
import pytest

from kazeflow.assets import asset
from kazeflow.flow import Flow


@asset()
def first():
    ...


@asset(deps=["first"])
def second():
    ...

@asset(deps=["second"])
def third():
    ...


def test_simple_flow():
    flow = Flow(asset_names=["third"])
    
    flow.run()
    

def test_flow_with_missing_dependency():
    with pytest.raises(ValueError, match="Asset 'non_existent_asset' not found."):
        flow = Flow(asset_names=["non_existent_asset"])
        flow.run()
