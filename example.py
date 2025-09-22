import time
from pathlib import Path

import kazeflow


# Asset 1: Create a raw data file
@kazeflow.asset
def create_raw_data(context: kazeflow.AssetContext) -> Path:
    """Creates a dummy raw data file."""
    context.logger.info("Creating raw data file...")
    raw_data_path = Path("raw_data.txt")
    raw_data_path.write_text("hello world\nkazeflow is awesome\nhello kazeflow")
    time.sleep(1)
    context.logger.info(f"Raw data created at {raw_data_path}")
    return raw_data_path


# Asset 2: Process the raw data file
@kazeflow.asset
def process_data(create_raw_data: Path, context: kazeflow.AssetContext) -> Path:
    """Reads the raw data, processes it, and saves to a new file."""
    context.logger.info(f"Processing data from {create_raw_data}...")
    processed_data_path = Path("processed_data.txt")
    content = create_raw_data.read_text()
    processed_content = content.upper()
    processed_data_path.write_text(processed_content)
    time.sleep(1)
    context.logger.info(f"Processed data saved at {processed_data_path}")
    return processed_data_path


# Asset 3: Summarize the results
@kazeflow.asset
def summarize(process_data: Path, context: kazeflow.AssetContext):
    """Reads the processed data and prints a summary."""
    context.logger.info(f"Summarizing data from {process_data}...")
    content = process_data.read_text()
    word_count = len(content.split())
    context.logger.info(f"Summary: The processed file contains {word_count} words.")
    time.sleep(1)


if __name__ == "__main__":
    kazeflow.run(
        asset_names=["summarize"],
        run_config={"max_concurrency": 2},
    )
