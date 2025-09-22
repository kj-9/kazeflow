
# kazeflow

`kazeflow` is a lightweight, asset-based task flow engine inspired by Dagster. It is designed to be simple, flexible, and easy to use.

## Example

Here is a simple example of how to use `kazeflow` to define and execute a data flow with dependencies, inputs/outputs, and logging.

When you run this script, `kazeflow` will execute the assets in the correct order based on their dependencies and provide a rich terminal UI to visualize the progress.


`example.py`:
```python
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

```

This will produce the following output:

```bash
❯ uv run python example.py
Task Flow (Execution Order)
└── create_raw_data
    └── process_data
        └── summarize

Execution Logs
INFO     Executing asset: create_raw_data                                       
INFO     Creating raw data file...                                              
INFO     Raw data created at raw_data.txt                                       
INFO     Finished executing asset: create_raw_data in 1.01s                     
INFO     Executing asset: process_data                                          
INFO     Processing data from raw_data.txt...                                   
INFO     Processed data saved at processed_data.txt                             
INFO     Finished executing asset: process_data in 1.01s                        
INFO     Executing asset: summarize                                             
INFO     Summarizing data from processed_data.txt...                            
INFO     Summary: The processed file contains 7 words.                          
INFO     Finished executing asset: summarize in 1.01s                           
╭─────────────────────────────────── Assets ───────────────────────────────────╮
│ ✓ create_raw_data                (1.01s)                                     │
│ ✓ process_data                   (1.01s)                                     │
│ ✓ summarize                      (1.01s)                                     │
╰──────────────────────────────────────────────────────────────────────────────╯
Overall Progress ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/3 0:00:03
```
