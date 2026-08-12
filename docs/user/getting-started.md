# Your first reviewed flow

The normal kazeflow workflow is short:

1. Write ordinary Python functions.
2. Inspect the selected work.
3. Explicitly decide to execute it.
4. Review the structured result.

## 1. Install

kazeflow supports Python 3.10 through 3.13. Check the interpreter, then create an
isolated environment:

=== "macOS / Linux"

    ```console
    python3 --version
    python3 -m venv .venv
    source .venv/bin/activate
    ```

=== "Windows PowerShell"

    ```powershell
    py --version
    py -m venv .venv
    .\.venv\Scripts\Activate.ps1
    ```

Install and verify the core. It has no required third-party runtime dependency:

```console
python -m pip install kazeflow
kazeflow --help
```

Install Rich only when you want the optional progress display. Keep the quotes in
shells such as zsh:

```console
python -m pip install "kazeflow[tui]"
```

## 2. Declare a flow

Create `daily.py`:

```python title="daily.py"
from kazeflow import Flow, asset


@asset
def fetch() -> str:
    return "report input"


@asset
def publish(fetch: str) -> None:
    print(fetch)


flow = Flow(["publish"])
```

A parameter named `fetch` declares that `publish` depends on the `fetch` asset.
The module-level `flow` chooses `publish` as the default target. Asset bodies remain
ordinary functions that can be unit-tested directly.

## 3. Inspect without invoking assets

```console
$ kazeflow plan daily.py
Plan: publish
2 assets · no partition selection · default concurrency
Graph:
  fetch
  fetch --> publish *
```

The `*` marks the selected target. Planning validates the dependency closure and
configuration without invoking either decorated asset body.

## 4. Approve and run

```console
$ kazeflow run daily.py
Planned run:
Plan: publish
2 assets · no partition selection · default concurrency
Graph:
  fetch
  fetch --> publish *
Proceed? [y/N] y
Run result:
- run_id: <run-id>
- status: success
- duration: <duration>
Tasks:
- ✓ fetch (success; <duration>)
- ✓ publish (success; <duration>)
```

Run IDs and durations vary. In CI or a pipe, make the same decision explicitly:

```console
kazeflow run daily.py --yes
```

Use `--format json` when another program needs one portable result document rather
than human-oriented text. Portable failure messages and tracebacks can contain
application values, so JSON is structurally limited but not automatically redacted.

## 5. Review the before and after

| Value | Purpose |
| --- | --- |
| `FlowPlan` | Targets, dependencies, partition selection, and normalized configuration before execution. |
| `RunResult` | Terminal flow, task, and attempt statuses, timings, in-memory outputs, and failure metadata. |
| Logs and events | Optional progress and diagnostic observations; they replace neither the plan nor the result. |

The same model is available from Python:

```python
from kazeflow import run

plan = flow.plan({"max_concurrency": 2})
for task in plan.tasks:
    print(task.name, task.dependencies, task.partition_keys)

result = run(["publish"], {"max_concurrency": 2})
print(result.status, result.duration)
```

!!! warning "Loading is still Python execution"

    Module top-level code, imports, and an explicitly selected factory can run while
    the CLI loads an entry. Plan review is not a sandbox or an automatic approval.

## Next steps

- [Define dependencies and targets](guides/assets-and-dependencies.md)
- [Select a partition](partitions.md)
- [Read the complete CLI reference](cli.md)
- [Inspect results and history](results.md)
