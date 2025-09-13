
# kazeflow

`kazeflow` is a lightweight, asset-based task flow engine inspired by Dagster. It is designed to be simple, flexible, and easy to use.

## Design Philosophy

The core philosophy of `kazeflow` is to model data pipelines as a collection of assets. An asset is a persistent object in the real world, such as a file, a database table, or a machine learning model. A task is a function that produces or updates an asset.

This asset-based approach has several advantages over traditional task-based workflow engines:

*   **Data-centric**: By focusing on the assets, `kazeflow` provides a more natural way to reason about data dependencies and lineage.
*   **Declarative**: Assets and their dependencies are defined declaratively using the `@asset` decorator. This makes the data flow easy to understand and visualize.
*   **Loose coupling**: Assets are loosely coupled, which makes them easy to test, reuse, and maintain.

## Core Concepts

*   **`@asset` decorator**: A decorator to define an asset and its dependencies. The decorator takes the asset's name, a list of dependencies, and an optional configuration schema.
*   **`Flow` class**: A container for a set of assets that are meant to be executed together. The `Flow` class is responsible for resolving the dependency graph and executing the assets in the correct order.
*   **`Config` class**: A simple class for accessing configuration within an asset.

## Example

Here is a simple example of how to use `kazeflow` to define and execute a data flow:

```python
from kazeflow.assets import asset
from kazeflow.flow import Flow
from kazeflow.config import Config

@asset
def raw_data():
    """This asset returns a simple list of numbers."""
    return [1, 2, 3, 4, 5]

@asset(deps=["raw_data"], config_schema=Config)
def processed_data(config: Config, raw_data):
    """This asset processes the raw data by summing and multiplying it."""
    multiplier = config.get("multiplier", 1)
    return sum(raw_data) * multiplier

if __name__ == "__main__":
    my_flow = Flow(asset_names=["processed_data"])
    flow_config = {
        "processed_data": {"multiplier": 2}
    }
    results = my_flow.run(config=flow_config)
    print(f"Flow results: {results}")
```
