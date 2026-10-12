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
import asyncio
from datetime import date
from importlib import resources
from pathlib import Path
import subprocess
import sys
import tempfile

import kazeflow
from kazeflow import (
    AssetContext,
    DatePartitionDef,
    Flow,
    FlowPlan,
    MaterializationCollection,
    PartitionGroup,
    Materialization,
    MaterializationContext,
    MaterializationInvalidation,
    MaterializationRecord,
    OpenCircuit,
    ResourcePolicy,
    RunResult,
)
from kazeflow.results import AttemptStatus, FlowStatus
from kazeflow.file_materialization import (
    FileMaterialization,
    FileMaterializationError,
    JsonCodec,
    MaterializationCodec,
)

side_effects = []
assert Materialization is not None
assert MaterializationCollection is not None
assert MaterializationContext is not None
assert MaterializationRecord is not None
assert MaterializationInvalidation is not None
assert MaterializationCodec is not None
assert FileMaterializationError is not None

with tempfile.TemporaryDirectory(prefix="kazeflow-file-materialization-") as temporary:
    store_root = Path(temporary) / "store"
    adapter = FileMaterialization(store_root, codec=JsonCodec())
    assert not store_root.exists()
    context = MaterializationContext(
        asset_name="wheel_smoke",
        version="v1",
        partition_domain="wheel_smoke",
        partition_key_present=True,
        partition_key="key",
    )
    committed = adapter.commit(context, {"answer": [42]})
    assert committed.committed
    resolved = adapter.resolve(context)
    assert resolved == committed
    assert adapter.load(resolved) == {"answer": [42]}
    assert adapter.available((context,)) == (committed,)
    assert adapter.available(()) == ()
    try:
        JsonCodec().encode((42,))
    except (TypeError, ValueError):
        pass
    else:
        raise AssertionError("JsonCodec must reject tuples")


class EventCollector:
    def __init__(self):
        self.events = []

    def on_event(self, event):
        self.events.append(event)


with tempfile.TemporaryDirectory(prefix="kazeflow-provenance-") as temporary:
    provenance_store = FileMaterialization(
        Path(temporary) / "store", codec=JsonCodec()
    )
    body_calls = []

    @kazeflow.asset(
        partition_def=DatePartitionDef(),
        materialization=provenance_store,
        version="wheel-provenance-v1",
    )
    def materialized_asset(context: AssetContext):
        body_calls.append(context.partition_key)
        return {"day": context.partition_key.isoformat(), "rows": [42]}

    provenance_flow = Flow(["materialized_asset"])
    first_events = EventCollector()
    first_result = asyncio.run(
        provenance_flow.run_async(
            {"partition_keys": ["2026-10-12"]}, event_consumer=first_events
        )
    )
    second_events = EventCollector()
    second_result = asyncio.run(
        provenance_flow.run_async(
            {"partition_keys": ["2026-10-12"]}, event_consumer=second_events
        )
    )
    assert len(body_calls) == 1
    first_attempt = first_result.tasks[0].attempts[0]
    second_attempt = second_result.tasks[0].attempts[0]
    assert first_attempt.resolution == "executed"
    assert first_attempt.materialization_id
    assert second_attempt.resolution == "materialized"
    assert second_attempt.materialization_id == first_attempt.materialization_id
    assert first_result.to_record()["tasks"][0]["attempts"][0].get("resolution") is None
    first_record = first_result.to_record(schema_version=4)
    second_record = second_result.to_record(schema_version=4)
    assert first_record["tasks"][0]["attempts"][0]["resolution"] == "executed"
    assert second_record["tasks"][0]["attempts"][0]["resolution"] == "materialized"
    assert (
        first_record["tasks"][0]["attempts"][0]["materialization_id"]
        == second_record["tasks"][0]["attempts"][0]["materialization_id"]
    )
    first_finish = next(
        event
        for event in first_events.events
        if event.kind.value == "attempt_finished"
    )
    second_finish = next(
        event
        for event in second_events.events
        if event.kind.value == "attempt_finished"
    )
    assert first_finish.to_record()["schema_version"] == 4
    assert second_finish.to_record()["schema_version"] == 4
    assert first_finish.to_record()["resolution"] == "executed"
    assert second_finish.to_record()["resolution"] == "materialized"
    assert (
        first_finish.to_record()["materialization_id"]
        == second_finish.to_record()["materialization_id"]
    )


with tempfile.TemporaryDirectory(prefix="kazeflow-invalidation-") as temporary:
    invalidation_store = FileMaterialization(
        Path(temporary) / "store", codec=JsonCodec()
    )
    invalidation_partition = DatePartitionDef()
    invalidation_key = date(2026, 10, 12)
    invalidation_calls = []

    @kazeflow.asset(
        partition_def=invalidation_partition,
        materialization=invalidation_store,
        version="wheel-invalidation-v1",
    )
    def invalidation_asset(context: AssetContext):
        invalidation_calls.append(context.partition_key)
        return {"day": context.partition_key.isoformat()}

    invalidation_flow = Flow(["invalidation_asset"])
    invalidation_config = {"partition_keys": [invalidation_key]}
    invalidation_context = MaterializationContext(
        asset_name="invalidation_asset",
        version="wheel-invalidation-v1",
        partition_domain=invalidation_partition.domain,
        partition_key_present=True,
        partition_key=invalidation_key,
    )
    before_invalidation = asyncio.run(
        invalidation_flow.run_async(invalidation_config)
    )
    old_id = before_invalidation.tasks[0].attempts[0].materialization_id
    assert old_id
    assert invalidation_store.inspect(invalidation_context).status == "available"
    previously_collected = invalidation_store.available((invalidation_context,))
    assert previously_collected == (invalidation_store.resolve(invalidation_context),)
    invalidation_store.invalidate(
        invalidation_context, reason="wheel smoke correction"
    )
    assert invalidation_store.inspect(invalidation_context).status == "invalidated"
    assert invalidation_store.available((invalidation_context,)) == ()
    try:
        invalidation_store.load(previously_collected[0])
    except FileMaterializationError:
        pass
    else:
        raise AssertionError("load must reject a collected record invalidated later")

    rebuilt = asyncio.run(invalidation_flow.run_async(invalidation_config))
    rebuilt_attempt = rebuilt.tasks[0].attempts[0]
    assert rebuilt_attempt.resolution == "executed"
    new_id = rebuilt_attempt.materialization_id
    assert new_id and new_id != old_id
    reused = asyncio.run(invalidation_flow.run_async(invalidation_config))
    reused_attempt = reused.tasks[0].attempts[0]
    assert reused_attempt.resolution == "materialized"
    assert reused_attempt.materialization_id == new_id
    assert len(invalidation_calls) == 2


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

@kazeflow.asset(partition_def=DatePartitionDef(), resource="wheel-query")
def query_partitions(context: AssetContext):
    if context.partition_key.isoformat() == "2026-08-12":
        raise OSError("temporary remote failure")
    return "published"

query_flow = Flow(
    ["query_partitions"],
    resources={
        "wheel-query": ResourcePolicy(
            max_concurrency=1,
            classify_failure=lambda error: OpenCircuit(),
        )
    },
)
query_result = asyncio.run(
    query_flow.run_async(
        {"partition_keys": ["2026-08-11", "2026-08-12", "2026-08-13"]}
    )
)
assert query_result.failed_partition_keys("query_partitions") == (
    date(2026, 8, 12),
)
assert query_result.circuit_skipped_partition_keys("query_partitions") == (
    date(2026, 8, 13),
)
query_record_before = query_result.to_record(schema_version=3)
assert query_result.failed_partition_keys("query_partitions")
assert query_result.to_record(schema_version=3) == query_record_before

group_trace = []

@kazeflow.asset(partition_def=DatePartitionDef())
def grouped_extract(context: AssetContext):
    partition_key = context.partition_key
    assert partition_key is not None
    key = str(partition_key)
    group_trace.append(("extract", key))
    return key

@kazeflow.asset(partition_def=DatePartitionDef())
def grouped_publish(grouped_extract: dict[object, str], context: AssetContext):
    partition_key = context.partition_key
    assert partition_key is not None
    key = str(partition_key)
    assert grouped_extract[partition_key] == key
    group_trace.append(("publish", key))
    return key

group_flow = Flow(
    ["grouped_publish"],
    partition_groups=[
        PartitionGroup(("grouped_extract", "grouped_publish"), max_active_keys=1)
    ],
)
group_config = {"partition_keys": ["2026-08-11", "2026-08-12"]}
group_plan = group_flow.plan(group_config)
assert isinstance(group_plan, FlowPlan)
assert group_plan.partition_groups[0].assets == (
    "grouped_extract",
    "grouped_publish",
)
assert group_plan.partition_groups[0].max_active_keys == 1
group_result = asyncio.run(group_flow.run_async(group_config))
assert group_result.status is FlowStatus.SUCCESS
assert group_trace == [
    ("extract", "2026-08-11"),
    ("publish", "2026-08-11"),
    ("extract", "2026-08-12"),
    ("publish", "2026-08-12"),
]

flow_file = Path("smoke_flow.py")
flow_file.write_text(
    '''
from kazeflow import Fail, Flow, OpenCircuit, RateLimit, ResourcePolicy, Retry, asset

invocations = []

def classify_failure(error):
    if isinstance(error, TimeoutError):
        return Retry(after_seconds=0, cooldown_resource=True)
    return Fail()

@asset(resource="network")
def source():
    invocations.append("source")
    if len(invocations) == 1:
        raise TimeoutError("transient")
    return "not called"

@asset(resource="network")
def publish(source):
    return "not called"

flow = Flow(["publish"], resources={"network": ResourcePolicy(max_concurrency=2, rate_limit=RateLimit(2, 1.5), max_attempts=2, classify_failure=classify_failure)})
''',
    encoding="utf-8",
)
command = Path(sys.executable).with_name("kazeflow")
instructions = subprocess.run(
    [str(command), "instructions"],
    check=True,
    capture_output=True,
    text=True,
)
assert instructions.stdout == (
    resources.files("kazeflow")
    .joinpath("development_instructions.md")
    .read_text(encoding="utf-8")
)
assert instructions.stderr == ""
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
assert plan_document["schema_version"] == 5
assert plan_document["data"]["targets"] == ["publish"]
assert plan_document["data"]["partition_groups"] == []
assert plan_document["data"]["resources"] == [
    {
        "name": "network",
        "max_concurrency": 2,
        "rate_limit": {"requests": 2, "per_seconds": 1.5},
        "max_attempts": 2,
        "classifier_enabled": True,
    }
]
assert [task["resource"] for task in plan_document["data"]["tasks"]] == [
    "network",
    "network",
]
run_cli = subprocess.run(
    [str(command), "run", str(flow_file), "--yes", "--format", "json"],
    check=True,
    capture_output=True,
    text=True,
)
run_document = json.loads(run_cli.stdout)
assert run_document["document_type"] == "kazeflow.run-result"
assert run_document["schema_version"] == 4
assert run_document["data"]["record_schema_version"] == 4
assert run_document["data"]["record"]["status"] == "success"
source_record = next(task for task in run_document["data"]["record"]["tasks"] if task["task"]["task_name"] == "source")
assert [item["number"] for item in source_record["attempts"][0]["invocations"]] == [1, 2]
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
assert history_document["schema_version"] == 4
assert history_document["data"]["record_schema_version"] == 4
assert history_document["data"]["store_schema_version"] >= 1

circuit_file = Path("circuit_flow.py")
circuit_file.write_text(
    '''
from kazeflow import Flow, OpenCircuit, ResourcePolicy, asset
from pathlib import Path

def classify(error):
    return OpenCircuit()

@asset(resource="network")
def a_source():
    raise PermissionError("resource unavailable")

@asset(resource="network")
def z_pending():
    Path("pending-called").write_text("called")

flow = Flow(["a_source", "z_pending"], resources={"network": ResourcePolicy(max_concurrency=1, classify_failure=classify)})
''',
    encoding="utf-8",
)
circuit_run = subprocess.run(
    [str(command), "run", str(circuit_file), "--yes", "--format", "json"],
    check=False,
    capture_output=True,
    text=True,
)
assert circuit_run.returncode == 1
circuit_document = json.loads(circuit_run.stdout)
assert circuit_document["schema_version"] == 4
assert circuit_document["data"]["record_schema_version"] == 4
circuit_tasks = {
    task["task"]["task_name"]: task
    for task in circuit_document["data"]["record"]["tasks"]
}
assert circuit_tasks["a_source"]["status"] == "failed"
assert circuit_tasks["z_pending"]["status"] == "skipped"
assert circuit_tasks["z_pending"]["reason"] == "resource_circuit_open"
assert circuit_tasks["z_pending"]["blocked_by"] == [
    {"task": {"task_name": "a_source"}, "partition": {"present": False}}
]
assert not Path("pending-called").exists()
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
