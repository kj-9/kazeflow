import asyncio
from pathlib import Path

import kazeflow


date_key_def = kazeflow.DatePartitionDef()


@kazeflow.asset(partition_def=date_key_def)
async def process_day(context: kazeflow.AssetContext) -> Path:
    """
    A partitioned asset that simulates processing data for a single day.
    The `partition_key` will be a date string like '2025-09-21'.
    """
    context.logger.info(f"Processing data for date: {context.partition_key}")
    output_path = Path(f"processed_data/{context.partition_key}.txt")
    output_path.parent.mkdir(exist_ok=True)
    output_path.touch()
    output_path.write_text(f"Data for {context.partition_key}")
    await asyncio.sleep(3)
    return output_path


@kazeflow.asset(partition_def=date_key_def)
async def process_day2(process_day, context: kazeflow.AssetContext) -> Path:
    """
    A partitioned asset that simulates processing data for a single day.
    The `partition_key` will be a date string like '2025-09-21'.
    """
    context.logger.info(f"Processing data for date: {context.partition_key}")
    output_path = Path(f"processed_data/{context.partition_key}.txt")
    output_path.parent.mkdir(exist_ok=True)
    output_path.touch()
    output_path.write_text(f"Data for {context.partition_key}")
    await asyncio.sleep(3)
    return output_path


@kazeflow.asset
async def summarize_results(
    process_day2: dict[str, Path], context: kazeflow.AssetContext
) -> None:
    """
    This asset gathers the results from all partitions of `process_day`.
    The `process_day` argument will be a dictionary mapping the partition key (date)
    to the output of that partition (the file path).
    """
    context.logger.info(f"Summarizing results for {len(process_day2)} days.")
    for date_str, path in process_day2.items():
        context.logger.info(f"  - {date_str}: {path}")


if __name__ == "__main__":
    kazeflow.run(
        asset_names=["summarize_results"],
        run_config={
            "partition_keys": date_key_def.range(
                start_date="2025-09-21", end_date="2025-09-23"
            ),
            "max_concurrency": 4,
        },
    )
