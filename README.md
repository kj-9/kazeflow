# kazeflow

`kazeflow` is a lightweight, asset-based task flow engine for small Python
programs. Define ordinary functions as assets, inspect their dependency plan, then
run selected targets and receive a structured result.

## Define, inspect, and run a flow

Assets remain plain Python functions. Dependencies can be inferred from parameter
names or declared explicitly with `deps`.

```python
import kazeflow
from kazeflow.flow import Flow


@kazeflow.asset
def create_raw_data() -> list[str]:
    return ["hello", "kazeflow"]


@kazeflow.asset
def summarize(create_raw_data: list[str]) -> int:
    return len(create_raw_data)


run_config = {"max_concurrency": 2}
flow = Flow(["summarize"])

# Planning validates the selected dependency closure without running either asset.
plan = flow.plan(run_config)
for task in plan.tasks:
    print(task.name, task.dependencies)

# The default execution path is quiet and returns a structured RunResult.
result = kazeflow.run(["summarize"], run_config)
assert result.status.value == "success"
assert result.tasks[-1].attempts[0].output == 2
```

`Flow.plan()` provides deterministic task order, dependencies, selected partitions,
and run configuration before execution. `run()` and `Flow.run_async()` return a
`RunResult` containing flow, task, and partition-attempt statuses, timings, outputs,
and serializable failure metadata. A failed asset is represented in the returned
result, while independent branches continue when possible.

## Logging from an asset

An asset that accepts `context: kazeflow.AssetContext` receives a standard-library
logger and, for partitioned work, its partition key. kazeflow does not configure
global logging: configure handlers in the application if those messages should be
shown.

```python
import logging

import kazeflow

logging.basicConfig(level=logging.INFO)


@kazeflow.asset
def report(context: kazeflow.AssetContext) -> str:
    context.logger.info("creating the report")
    return "done"
```

## Opt into Rich terminal rendering

Core execution never creates a terminal UI. To render lifecycle progress, explicitly
import the Rich-backed adapter, enter its presentation context, and pass it as the
event consumer. The renderer observes neutral execution events; it does not change
planning, scheduling, outputs, statuses, or failure handling.

```python
import kazeflow
from kazeflow.flow import Flow
from kazeflow.tui import FlowTUIRenderer, show_plan_tree


flow = Flow(["summarize"])
run_config = {"max_concurrency": 2}
plan = flow.plan(run_config)

# This is an explicit presentation choice and does not execute assets.
show_plan_tree(plan)

with FlowTUIRenderer(total_assets=len(plan.tasks)) as renderer:
    result = kazeflow.run(
        ["summarize"], run_config, event_consumer=renderer
    )

assert result.status.value == "success"
```

For asynchronous applications, use the same renderer around `await
flow.run_async(run_config, event_consumer=renderer)`. If a run is cancelled, the
renderer closes safely after the event prefix already observed.

Rich presentation is selected only by importing `kazeflow.tui` and passing a
renderer. Package metadata for a dedicated TUI extra is planned separately; this
release does not require a special install command for the example above.
