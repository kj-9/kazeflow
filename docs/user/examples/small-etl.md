# Small ETL flow

Use separate assets to make a small extract-transform-publish script reviewable.

```python title="etl.py"
from kazeflow import Flow, asset


@asset
def extract() -> list[str]:
    return ["north,12", "south,9"]


@asset
def transform(extract: list[str]) -> dict[str, int]:
    return {
        region: int(value)
        for region, value in (row.split(",") for row in extract)
    }


@asset
def publish(transform: dict[str, int]) -> None:
    print(transform)


flow = Flow(["publish"])
```

Review and run:

```console
kazeflow plan etl.py
kazeflow run etl.py
```

The functions remain independently testable. kazeflow supplies dependency order,
review, execution status, and timing rather than replacing the transformation code.
