# kazeflow

`kazeflow` is a lightweight, asset-based task flow engine for small Python
programs. Define ordinary functions as assets, inspect their dependency plan, then
run selected targets and receive a structured result.

## Install

Install the standard-library-only core to define assets, inspect plans, and run
flows:

```bash
pip install kazeflow
```

Rich terminal rendering is optional. Install it only when you want to use the TUI
adapter:

```bash
pip install "kazeflow[tui]"
```

## Define, inspect, and run a flow

Assets remain plain Python functions. Dependencies can be inferred from parameter
names or declared explicitly with `deps`.

```python
from kazeflow import Flow, asset, run


@asset
def create_raw_data() -> list[str]:
    return ["hello", "kazeflow"]


@asset
def summarize(create_raw_data: list[str]) -> int:
    return len(create_raw_data)


run_config = {"max_concurrency": 2}
flow = Flow(["summarize"])

# Planning validates the selected dependency closure without running either asset.
plan = flow.plan(run_config)
for task in plan.tasks:
    print(task.name, task.dependencies)

# The default execution path is quiet and returns a structured RunResult.
result = run(["summarize"], run_config)
assert result.status.value == "success"
assert result.tasks[-1].attempts[0].output == 2
```

## Review a flow before running it

For a flow you wrote yourself or received from an AI or another person, use the plan
as an explicit review step before deciding to run it. `Flow.plan()` is
side-effect-free: it describes the selected work but does not invoke an asset.

```python
from kazeflow import Flow, run


flow = Flow(["summarize"])
run_config = {"max_concurrency": 2}
plan = flow.plan(run_config)

# Review the exact targets, dependency-first task order, and partition selections.
assert plan.targets == ("summarize",)
for task in plan.tasks:
    print(task.name, task.dependencies, task.partition_keys)

# Review the normalized execution settings before choosing whether to run.
print(plan.config.max_concurrency, plan.config.partition_keys)

# The caller makes the decision to execute after reviewing this information.
result = run(["summarize"], run_config)

# Inspect the flow result, then each task and partition attempt.
print(result.status, result.started_at, result.ended_at)
for task in result.tasks:
    print(task.task.task_name, task.status)
    for attempt in task.attempts:
        print(attempt.attempt.partition_key, attempt.status)
        print(attempt.output, attempt.failure, attempt.blocked_by)
```

`FlowPlan` is structured pre-execution information: selected targets,
dependency-first task order, partition selections, and normalized run configuration.
`RunResult` is structured terminal information for one run: flow, task, and
partition-attempt statuses, timings, outputs, and serializable failure metadata.
Logs are optional, time-ordered detail for progress and diagnosis. They do not
replace reviewing a plan before execution or inspecting a result afterward.

Review support helps make a declared flow easier to understand; it is not a security
sandbox, a proof that code is safe, a way to prevent asset side effects, or an
automatic approval to execute. Asset functions are arbitrary Python, including when
they were AI-generated: review the code itself and decide whether to run it.

## Inspect a script from the command line

The core installation also provides `kazeflow assets` and `kazeflow plan` for
reviewing a trusted Python flow script from the shell. A bare script entry discovers
the assets registered while the script loads. If it provides a module-level `flow`,
that flow supplies the default targets; otherwise `plan` derives all discovered
terminal assets as its targets.

```bash
# List assets discovered while loading the script.
kazeflow assets path/to/flow.py

# Review the default targets and their dependency-first plan.
kazeflow plan path/to/flow.py

# Review one target, or emit one machine-readable JSON document to stdout.
kazeflow plan path/to/flow.py --target summarize
kazeflow plan path/to/flow.py --format json
```

`assets` and `plan` do not call asset functions. Loading a script is still ordinary
Python execution, however, so its top-level statements and imports can have side
effects. Treat a script entry as trusted code to load; the inspection commands are
not a sandbox or a safety approval. The current CLI only inspects flows: `run`, Rich
TUI integration, and SQLite record storage are not CLI features. See the
[CLI guide](docs/cli.md) for entry forms, target selection, JSON output, and error
handling.

For a partitioned flow with a failed and dependency-blocked attempt, see the
[review workflow guide](docs/reviewable-flows.md) and its runnable
[core-only example](examples/review_flow.py).

## Persist run records explicitly

Core execution never creates a database or persists run history. To opt into a
caller-owned SQLite database, construct `SQLiteRunStore` explicitly. Its stored
records are portable but deliberately lossy: they omit raw outputs, exceptions, and
partition keys. See the [SQLite run-store guide](docs/sqlite-run-store.md) for the
adapter API and schema compatibility behavior.

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

After installing `kazeflow[tui]`, explicitly import the Rich-backed adapter, enter
its presentation context, and pass it as the event consumer. Core execution never
creates a terminal UI. The renderer observes neutral execution events; it does not
change planning, scheduling, outputs, statuses, or failure handling.

```python
from kazeflow import Flow, run
from kazeflow.tui import FlowTUIRenderer, show_plan_tree


flow = Flow(["summarize"])
run_config = {"max_concurrency": 2}
plan = flow.plan(run_config)

# This is an explicit presentation choice and does not execute assets.
show_plan_tree(plan)

with FlowTUIRenderer(total_assets=len(plan.tasks)) as renderer:
    result = run(
        ["summarize"], run_config, event_consumer=renderer
    )

assert result.status.value == "success"
```

For asynchronous applications, use the same renderer around `await
flow.run_async(run_config, event_consumer=renderer)`. If a run is cancelled, the
renderer closes safely after the event prefix already observed.
