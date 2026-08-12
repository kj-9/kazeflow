# Show Rich progress

The default execution path is quiet and imports no presentation dependency. Install
and request the optional Rich renderer only when a live terminal view is useful.

```console
pip install "kazeflow[tui]"
kazeflow run daily.py --tui
```

The run preflight still appears before execution. After approval, the TUI shows
waiting, running, succeeded, skipped, and failed tasks plus overall progress on
standard error. JSON result output can therefore remain one clean document on
standard output.

## Python integration

```python
from kazeflow.tui import FlowTUIRenderer

plan = flow.plan()
with FlowTUIRenderer(plan=plan) as renderer:
    result = await flow.run_async(event_consumer=renderer)
```

The renderer consumes neutral execution events. It does not change scheduling,
outputs, statuses, or failure semantics. If the requested adapter cannot load or
fails while consuming events, the CLI reports an infrastructure failure.
