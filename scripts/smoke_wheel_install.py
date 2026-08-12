#!/usr/bin/env python3
"""Run a core or TUI smoke test from an isolated installed wheel."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import venv


CORE_PROGRAM = """
import json
from pathlib import Path
import subprocess
import sys

import kazeflow
from kazeflow import Flow, FlowPlan, RunResult
from kazeflow.results import AttemptStatus, FlowStatus

side_effects = []


@kazeflow.asset
def complete():
    side_effects.append("ran")
    return "complete"


plan = Flow(["complete"]).plan()
assert isinstance(plan, FlowPlan)
assert plan.tasks[0].name == "complete"
assert side_effects == []

result = kazeflow.run(["complete"])
assert isinstance(result, RunResult)
assert result.status is FlowStatus.SUCCESS
assert result.tasks[0].status is AttemptStatus.SUCCESS
assert result.tasks[0].attempts[0].output == "complete"
assert side_effects == ["ran"]
assert not any(name == "rich" or name.startswith("rich.") for name in sys.modules)

flow_file = Path("smoke_flow.py")
flow_file.write_text(
    '''
from kazeflow import asset

@asset
def source():
    return "not called"

@asset
def publish(source):
    return "not called"
''',
    encoding="utf-8",
)
command = Path(sys.executable).with_name("kazeflow")
assets = subprocess.run(
    [str(command), "assets", str(flow_file), "--format", "json"],
    check=True,
    capture_output=True,
    text=True,
)
assets_document = json.loads(assets.stdout)
assert assets_document["document_type"] == "kazeflow.assets"
assert assets_document["schema_version"] == 1
assert [asset["name"] for asset in assets_document["data"]["assets"]] == ["publish", "source"]
plan_cli = subprocess.run(
    [str(command), "plan", str(flow_file), "--format", "json"],
    check=True,
    capture_output=True,
    text=True,
)
plan_document = json.loads(plan_cli.stdout)
assert plan_document["document_type"] == "kazeflow.plan"
assert plan_document["schema_version"] == 1
assert plan_document["data"]["targets"] == ["publish"]
run_cli = subprocess.run(
    [str(command), "run", str(flow_file), "--yes", "--format", "json"],
    check=True,
    capture_output=True,
    text=True,
)
run_document = json.loads(run_cli.stdout)
assert run_document["document_type"] == "kazeflow.run-result"
assert run_document["schema_version"] == 1
assert run_document["data"]["record_schema_version"] == 1
assert run_document["data"]["record"]["status"] == "success"
history_directory = Path(".kazeflow")
history_directory.mkdir()
history_store = history_directory / "runs.sqlite3"
stored_run = subprocess.run(
    [str(command), "run", str(flow_file), "--yes", "--store", str(history_store), "--format", "json"],
    check=True,
    capture_output=True,
    text=True,
)
stored_document = json.loads(stored_run.stdout)
assert stored_document["document_type"] == "kazeflow.run-result"
stored_run_id = stored_document["data"]["record"]["run_id"]
history = subprocess.run(
    [str(command), "runs", "show", stored_run_id, "--format", "json"],
    check=True,
    capture_output=True,
    text=True,
)
history_document = json.loads(history.stdout)
assert history_document["document_type"] == "kazeflow.runs-show"
assert history_document["data"]["run_id"] == stored_run_id
assert history_document["data"]["record_schema_version"] == 1
assert history_document["data"]["store_schema_version"] >= 1
"""

TUI_PROGRAM = """
import json
from pathlib import Path
import subprocess
import sys

from kazeflow.tui import FlowTUIRenderer

renderer = FlowTUIRenderer(total_assets=0)
assert renderer.overall_progress.tasks[0].total == 0

flow_file = Path("tui_flow.py")
flow_file.write_text(
    '''
from kazeflow import Flow, asset

@asset
def source():
    return "source"

@asset
def publish(source):
    return source

flow = Flow(["publish"])
''',
    encoding="utf-8",
)
command = Path(sys.executable).with_name("kazeflow")
run_cli = subprocess.run(
    [str(command), "run", str(flow_file), "--yes", "--tui", "--format", "json"],
    check=True,
    capture_output=True,
    text=True,
)
run_document = json.loads(run_cli.stdout)
assert run_document["document_type"] == "kazeflow.run-result"
assert run_document["data"]["record"]["status"] == "success"
assert "Overall Progress" in run_cli.stderr
"""


def _run(command: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    subprocess.run(command, cwd=cwd, env=env, check=True)


def _resolve_wheel(path: Path) -> Path:
    if path.is_file():
        return path
    if not path.is_dir():
        raise FileNotFoundError(path)

    wheels = sorted(path.glob("kazeflow-*.whl"))
    if len(wheels) != 1:
        raise AssertionError(
            f"expected exactly one kazeflow wheel in {path}, found {wheels}"
        )
    return wheels[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("core", "tui"))
    args = parser.parse_args()
    wheel = _resolve_wheel(args.wheel).resolve()

    with tempfile.TemporaryDirectory(prefix="kazeflow-wheel-smoke-") as temporary:
        root = Path(temporary)
        environment = root / "environment"
        outside_checkout = root / "outside-checkout"
        outside_checkout.mkdir()
        venv.EnvBuilder(with_pip=True).create(environment)
        python = environment / "bin" / "python"
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)

        if args.mode == "core":
            _run(
                [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
                cwd=outside_checkout,
                env=env,
            )
            program = CORE_PROGRAM
        else:
            _run(
                [str(python), "-m", "pip", "install", f"{wheel}[tui]"],
                cwd=outside_checkout,
                env=env,
            )
            program = TUI_PROGRAM

        _run(
            [str(python), "-c", textwrap.dedent(program)],
            cwd=outside_checkout,
            env=env,
        )


if __name__ == "__main__":
    main()
