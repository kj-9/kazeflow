
# kazeflow

`kazeflow` is a lightweight, asset-based task flow engine inspired by Dagster. It is designed to be simple, flexible, and easy to use.

## Example

When you run this script, `kazeflow` will execute the assets in the correct order, handle the failure of `failing_asset` gracefully, and provide a rich terminal UI to visualize the progress.


Here is a simple example of how to use `kazeflow` to define and execute a data flow with dependencies, inputs/outputs, and logging.


example.py:
```python
import asyncio

from kazeflow.assets import asset, AssetContext, default_registry
from kazeflow.flow import Flow


# A simple asset with no dependencies
@asset()
async def users() -> list[str]:
    """This asset returns a list of user names."""
    return ["Alice", "Bob", "Charlie"]


# This asset depends on the `users` asset.
# The output of `users` is automatically passed as an argument.
@asset(deps=["users"])
async def greetings(users: list[str], context: AssetContext) -> list[str]:
    """This asset receives the list of users and a context object.

    It uses the context to get a logger and log a message.
    """
    context.logger.info(f"Generating greetings for {len(users)} users.")
    return [f"Hello, {user}!" for user in users]


# This asset fails intentionally to demonstrate error handling.
@asset(deps=["users"])
async def failing_asset(users: list[str]):
    """This asset always fails."""
    raise ValueError("This asset is designed to fail.")


if __name__ == "__main__":
    # Define a flow that includes the final assets we want to generate.
    # kazeflow automatically includes all upstream dependencies.
    asset_names = ["greetings", "failing_asset"]
    ts = default_registry.build_graph(asset_names)
    flow = Flow(ts)

    # Run the flow asynchronously.
    # You can limit the number of concurrent assets with `max_concurrency`.
    asyncio.run(flow.run_async(max_concurrency=2))

```
```bash
❯ uv run example.py
Task Flow (Execution Order)
└── users
    ├── failing_asset
    └── greetings

Execution Logs
INFO     Executing asset: users                                         
INFO     Finished executing asset: users in 0.00s                       
INFO     Executing asset: greetings                                     
INFO     Generating greetings for 3 users.                              
INFO     Finished executing asset: greetings in 0.00s                   
INFO     Executing asset: failing_asset                                 
ERROR    Error executing asset failing_asset: This asset is designed to 
         fail.                                                          
         ╭───────────── Traceback (most recent call last) ─────────────╮
         │ /Users/kh03/work/repos/myflow/src/kazeflow/flow.py:82 in    │
         │ _execute_asset                                              │
         │                                                             │
         │    79 │   │   │   │   input_kwargs["context"] = context     │
         │    80 │   │   │                                             │
         │    81 │   │   │   if asyncio.iscoroutinefunction(asset_func │
         │ ❱  82 │   │   │   │   output = await asset_func(**input_kwa │
         │    83 │   │   │   else:                                     │
         │    84 │   │   │   │   # Run sync function in a thread pool  │
         │    85 │   │   │   │   loop = asyncio.get_running_loop()     │
         │                                                             │
         │ /Users/kh03/work/repos/myflow/example.py:31 in              │
         │ failing_asset                                               │
         │                                                             │
         │   28 @asset(deps=["users"])                                 │
         │   29 async def failing_asset(users: list[str]):             │
         │   30 │   """This asset always fails."""                     │
         │ ❱ 31 │   raise ValueError("This asset is designed to fail." │
         │   32                                                        │
         │   33                                                        │
         │   34 if __name__ == "__main__":                             │
         ╰─────────────────────────────────────────────────────────────╯
         ValueError: This asset is designed to fail.                    
╭─────────────────────────────── Assets ───────────────────────────────╮
│ ✓ users                          (0.00s)                             │
│ ✓ greetings                      (0.00s)                             │
│ ✗ failing_asset                  (0.04s)                             │
╰──────────────────────────────────────────────────────────────────────╯
Overall Progress ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/3 0:00:00

```
