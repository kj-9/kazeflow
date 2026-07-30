"""Smoke coverage for using the core without the optional Rich renderer."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_core_modules_plan_and_run_without_rich() -> None:
    """Core imports and renderer-free execution must not resolve Rich."""
    program = """
import sys


class BlockRichImports:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "rich" or fullname.startswith("rich."):
            raise ModuleNotFoundError("Rich is deliberately unavailable in this smoke test")
        return None


for module_name in list(sys.modules):
    if module_name == "rich" or module_name.startswith("rich."):
        del sys.modules[module_name]
sys.meta_path.insert(0, BlockRichImports())

import kazeflow
from kazeflow.assets import AssetRegistry
from kazeflow.events import ExecutionEvent
from kazeflow.flow import Flow, run
from kazeflow.plan import FlowPlan
from kazeflow.results import AttemptStatus, FlowStatus, RunResult


registry = AssetRegistry()


def target():
    return "completed without rich"


registry.register(target)
plan = Flow(["target"], registry=registry).plan()
result = run(["target"], registry=registry)

assert isinstance(plan, FlowPlan)
assert plan.tasks[0].name == "target"
assert isinstance(result, RunResult)
assert result.status is FlowStatus.SUCCESS
assert result.tasks[0].status is AttemptStatus.SUCCESS
assert result.tasks[0].attempts[0].output == "completed without rich"
assert ExecutionEvent.__module__ == "kazeflow.events"
assert not any(name == "rich" or name.startswith("rich.") for name in sys.modules)
"""

    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == ""
    assert completed.stderr == ""
