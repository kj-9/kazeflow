import asyncio
from pathlib import Path

from kazeflow.assets import asset, AssetContext, default_registry
from kazeflow.flow import Flow
from kazeflow.partition import DatePartitionDef


date_key_def = DatePartitionDef()


@asset(partition_def=date_key_def)
async def process_day(context: AssetContext) -> Path:
    """
    A partitioned asset that simulates processing data for a single day.
    The `partition_key` will be a date string like '2025-09-21'.
    """
    context.logger.info(f"Processing data for date: {context.partition_key}")
    output_path = Path(f"processed_data/{context.partition_key}.txt")
    output_path.parent.mkdir(exist_ok=True)
    output_path.touch()
    output_path.write_text(f"Data for {context.partition_key}")
    return output_path


@asset
async def summarize_results(
    process_day: dict[str, Path], context: AssetContext
) -> None:
    """
    This asset gathers the results from all partitions of `process_day`.
    The `process_day` argument will be a dictionary mapping the partition key (date)
    to the output of that partition (the file path).
    """
    context.logger.info(f"Summarizing results for {len(process_day)} days.")
    for date_str, path in process_day.items():
        context.logger.info(f"  - {date_str}: {path}")
    # In a real scenario, you might merge these files or write a summary report.


if __name__ == "__main__":
    # Define a flow that includes the final asset we want to generate.
    asset_names = ["summarize_results"]
    graph = default_registry.build_graph(asset_names)
    flow = Flow(graph)

    # Define a runtime configuration for the flow.
    # This config specifies that the `process_day` asset should be partitioned
    # for a range of dates.

    # Run the flow asynchronously, passing the runtime config.
    asyncio.run(
        flow.run_async(
            run_config={
                "partition_keys": date_key_def.range(
                    start_date="2025-9-21", end_date="2025-9-23"
                )
            }
        )
    )
