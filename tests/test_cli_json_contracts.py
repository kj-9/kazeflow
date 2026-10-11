"""Executable evidence for the published CLI JSON contracts."""

from __future__ import annotations

import copy
import io
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

import kazeflow.cli as cli
from kazeflow.cli import main


_ROOT = Path(__file__).parents[1]
_SCHEMA_PATH = _ROOT / "docs/user/schemas/cli/v6/schema.json"
_GOLDENS = _ROOT / "tests/fixtures/cli-json/v6"
_HISTORICAL_V5_SCHEMA_PATH = _ROOT / "docs/user/schemas/cli/v5/schema.json"
_HISTORICAL_V5_GOLDENS = _ROOT / "tests/fixtures/cli-json/v5"
_HISTORICAL_V1_SCHEMA_PATH = _ROOT / "docs/user/schemas/cli/v1/schema.json"
_HISTORICAL_V1_GOLDENS = _ROOT / "tests/fixtures/cli-json/v1"
_CURRENT_SCHEMA_PATH = _SCHEMA_PATH
_CURRENT_GOLDENS = _GOLDENS
_PLAN_V2_SCHEMA_PATH = _ROOT / "docs/user/schemas/cli/v2/schema.json"
_PLAN_V2_GOLDENS = _ROOT / "tests/fixtures/cli-json/v2"
_PLAN_V3_SCHEMA_PATH = _ROOT / "docs/user/schemas/cli/v3/schema.json"
_PLAN_V3_GOLDENS = _ROOT / "tests/fixtures/cli-json/v3"
_PLAN_V4_SCHEMA_PATH = _ROOT / "docs/user/schemas/cli/v4/schema.json"
_PLAN_V4_GOLDENS = _ROOT / "tests/fixtures/cli-json/v4"


def _validator() -> Any:
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _plan_v2_validator() -> Any:
    schema = json.loads(_PLAN_V2_SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _golden(name: str) -> dict[str, Any]:
    return json.loads((_GOLDENS / name).read_text(encoding="utf-8"))


def _plan_v3_validator() -> Any:
    schema = json.loads(_PLAN_V3_SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _golden_v2(name: str) -> dict[str, Any]:
    return json.loads((_PLAN_V2_GOLDENS / name).read_text(encoding="utf-8"))


def _golden_v3(name: str) -> dict[str, Any]:
    return json.loads((_PLAN_V3_GOLDENS / name).read_text(encoding="utf-8"))


def _current_validator() -> Any:
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _current_golden(name: str) -> dict[str, Any]:
    return json.loads((_GOLDENS / name).read_text(encoding="utf-8"))


def _script(tmp_path: Path, name: str, source: str) -> Path:
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    return path


def _run(
    capsys: pytest.CaptureFixture[str], argv: list[str]
) -> tuple[int, dict[str, Any], str]:
    status = main(argv)
    captured = capsys.readouterr()
    return status, json.loads(captured.out), captured.err


class _TTY(io.StringIO):
    def isatty(self) -> bool:
        return True


def _normalized(value: Any, *, run_ids: dict[str, str] | None = None) -> Any:
    """Normalize only values explicitly declared volatile by the contract."""
    if isinstance(value, list):
        return [_normalized(item, run_ids=run_ids) for item in value]
    if not isinstance(value, dict):
        return value
    result: dict[str, Any] = {}
    for key, item in value.items():
        if key == "run_id" and isinstance(item, str) and run_ids is not None:
            result[key] = run_ids.get(item, item)
        elif key == "started_at":
            result[key] = "2026-08-12T00:00:00+00:00"
        elif key == "ended_at":
            result[key] = "2026-08-12T00:00:01+00:00"
        elif key == "saved_at":
            result[key] = "2026-08-12T00:00:02+00:00"
        elif key == "duration_seconds":
            result[key] = 1.0
        elif key == "duration_seconds_delta":
            result[key] = 0.0
        elif key == "traceback":
            result[key] = "TRACEBACK"
        else:
            result[key] = _normalized(item, run_ids=run_ids)
    return result


def test_checked_in_goldens_validate_against_the_normative_schema() -> None:
    validator = _validator()
    golden_paths = sorted(_GOLDENS.glob("*.json"))

    assert len(golden_paths) == 15
    for path in golden_paths:
        validator.validate(json.loads(path.read_text(encoding="utf-8")))

    legacy_validator = Draft202012Validator(
        json.loads(_HISTORICAL_V1_SCHEMA_PATH.read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    )
    for path in _HISTORICAL_V1_GOLDENS.glob("*.json"):
        legacy_validator.validate(json.loads(path.read_text(encoding="utf-8")))

    historical_v5_validator = Draft202012Validator(
        json.loads(_HISTORICAL_V5_SCHEMA_PATH.read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    )
    historical_v5_goldens = sorted(_HISTORICAL_V5_GOLDENS.glob("*.json"))
    assert len(historical_v5_goldens) == 15
    for path in historical_v5_goldens:
        historical_v5_validator.validate(json.loads(path.read_text(encoding="utf-8")))

    historical_validator = _plan_v2_validator()
    historical_goldens = sorted(_PLAN_V2_GOLDENS.glob("*.json"))
    assert len(historical_goldens) == 4
    for path in historical_goldens:
        historical_validator.validate(json.loads(path.read_text(encoding="utf-8")))

    plan_validator = _plan_v3_validator()
    plan_goldens = sorted(_PLAN_V3_GOLDENS.glob("*.json"))
    assert len(plan_goldens) == 5
    for path in plan_goldens:
        plan_validator.validate(json.loads(path.read_text(encoding="utf-8")))

    historical_v4 = Draft202012Validator(
        json.loads((_ROOT / "docs/user/schemas/cli/v4/schema.json").read_text()),
        format_checker=FormatChecker(),
    )
    for path in (_ROOT / "tests/fixtures/cli-json/v4").glob("*.json"):
        historical_v4.validate(json.loads(path.read_text(encoding="utf-8")))


def test_resource_plan_json_v4_matches_golden(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "resource_flow.py",
        """
from kazeflow import Flow, RateLimit, ResourcePolicy, asset

@asset(resource="network")
def fetch(): return None

@asset(resource="network")
def publish(fetch): return None

flow = Flow(["publish"], resources={"network": ResourcePolicy(max_concurrency=3, rate_limit=RateLimit(2, 1.5))})
""",
    )
    status, plan, stderr = _run(capsys, ["plan", str(entry), "--format", "json"])
    assert status == 0
    assert stderr == ""
    _current_validator().validate(plan)
    assert plan == _current_golden("plan-rate.json")


def test_resource_plan_without_rate_limit_has_explicit_null(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "resource_flow.py",
        """
from kazeflow import Flow, ResourcePolicy, asset

@asset(resource="network")
def fetch(): return None

@asset(resource="network")
def publish(fetch): return None

flow = Flow(["publish"], resources={"network": ResourcePolicy(max_concurrency=3)})
""",
    )
    status, plan, stderr = _run(capsys, ["plan", str(entry), "--format", "json"])
    assert status == 0
    assert stderr == ""
    _current_validator().validate(plan)
    assert plan == _current_golden("plan-resource.json")
    assert plan["data"]["resources"][0]["rate_limit"] is None


def test_plan_projects_retry_budget_and_classifier_without_callback_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "retry_flow.py",
        """
from kazeflow import Flow, ResourcePolicy, Retry, asset

def classify(error): return Retry(after_seconds=0.25, cooldown_resource=True)

@asset(resource="network")
def publish(): return None

flow = Flow(["publish"], resources={"network": ResourcePolicy(max_concurrency=2, max_attempts=4, classify_failure=classify)})
""",
    )
    status, plan, stderr = _run(capsys, ["plan", str(entry), "--format", "json"])
    assert status == 0
    assert stderr == ""
    _current_validator().validate(plan)
    assert plan["data"]["resources"] == [
        {
            "name": "network",
            "max_concurrency": 2,
            "rate_limit": None,
            "max_attempts": 4,
            "classifier_enabled": True,
        }
    ]
    assert "classify" not in json.dumps(plan)
    assert plan == _current_golden("plan-retry.json")


def test_retry_run_and_history_project_numbered_invocations(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "retry_flow.py",
        """
from kazeflow import Fail, Flow, ResourcePolicy, Retry, asset

calls = []
def classify(error):
    return Retry(after_seconds=0) if isinstance(error, TimeoutError) else Fail()

@asset(resource="network")
def publish():
    calls.append(len(calls) + 1)
    if len(calls) == 1: raise TimeoutError("temporary")
    return "done"

flow = Flow(["publish"], resources={"network": ResourcePolicy(max_concurrency=1, max_attempts=2, classify_failure=classify)})
""",
    )
    store = tmp_path / "runs.sqlite3"
    status, result, stderr = _run(
        capsys,
        ["run", str(entry), "--yes", "--store", str(store), "--format", "json"],
    )
    assert status == 0
    assert "Planned run:" in stderr
    _current_validator().validate(result)
    assert result["schema_version"] == 4
    assert result["data"]["record_schema_version"] == 4
    attempt = result["data"]["record"]["tasks"][0]["attempts"][0]
    assert [item["number"] for item in attempt["invocations"]] == [1, 2]
    assert attempt["invocations"][0]["status"] == "failed"
    assert attempt["invocations"][0]["retry_delay_seconds"] == 0
    assert attempt["invocations"][1]["status"] == "success"

    run_id = result["data"]["record"]["run_id"]
    status, shown, stderr = _run(
        capsys, ["runs", "show", run_id, "--store", str(store), "--format", "json"]
    )
    assert status == 0
    assert stderr == ""
    _current_validator().validate(shown)
    assert shown["schema_version"] == 4
    assert shown["data"]["record_schema_version"] == 4
    assert [
        item["number"]
        for item in shown["data"]["record"]["tasks"][0]["attempts"][0]["invocations"]
    ] == [1, 2]


def test_published_document_specific_schemas_resolve_the_shared_schema() -> None:
    shared = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    registry = Registry().with_resource(shared["$id"], Resource.from_contents(shared))
    fixtures = {
        "assets": "assets.json",
        "partitions": "partitions.json",
        "plan": "plan-keys.json",
        "run-result": "run-success.json",
        "run-declined": "run-declined.json",
        "runs-list": "runs-list.json",
        "runs-show": "runs-show.json",
        "runs-compare": "runs-compare.json",
    }

    for document_name, fixture_name in fixtures.items():
        wrapper_path = _SCHEMA_PATH.with_name(f"{document_name}.schema.json")
        wrapper = json.loads(wrapper_path.read_text(encoding="utf-8"))
        Draft202012Validator(wrapper, registry=registry).validate(_golden(fixture_name))

    for schema_path, golden_path in (
        (_PLAN_V2_SCHEMA_PATH, _golden_v2("plan-resource.json")),
        (_PLAN_V3_SCHEMA_PATH, _golden_v3("plan-rate.json")),
        (_PLAN_V4_SCHEMA_PATH, _current_golden("plan-rate.json")),
    ):
        wrapper = json.loads(
            (schema_path.parent / "plan.schema.json").read_text(encoding="utf-8")
        )
        shared_schema = json.loads(schema_path.read_text(encoding="utf-8"))
        registry = Registry().with_resource(
            shared_schema["$id"], Resource.from_contents(shared_schema)
        )
        Draft202012Validator(wrapper, registry=registry).validate(golden_path)


def test_history_schema_binds_record_shape_to_version_even_for_empty_selection() -> (
    None
):
    validator = _current_validator()
    empty_selection = copy.deepcopy(_current_golden("runs-show.json"))
    record = empty_selection["data"]["record"]
    record["tasks"][0].update(
        is_partitioned=True,
        status="skipped",
        reason="no_partition_keys",
        attempts=[],
    )
    validator.validate(empty_selection)

    mixed_compare = copy.deepcopy(_current_golden("runs-compare.json"))
    old_side = mixed_compare["data"]["left"]
    old_side["record_schema_version"] = 1
    for task in old_side["record"]["tasks"]:
        for attempt in task["attempts"]:
            attempt.pop("invocations")
            attempt.pop("resolution")
            attempt.pop("materialization_id")
    validator.validate(mixed_compare)

    mislabeled_v1 = copy.deepcopy(_current_golden("runs-show.json"))
    mislabeled_v1["data"]["record_schema_version"] = 1
    for task in mislabeled_v1["data"]["record"]["tasks"]:
        for attempt in task["attempts"]:
            attempt.pop("resolution")
            attempt.pop("materialization_id")
    with pytest.raises(Exception):
        validator.validate(mislabeled_v1)

    mislabeled_v2 = copy.deepcopy(_current_golden("runs-show.json"))
    attempt = mislabeled_v2["data"]["record"]["tasks"][0]["attempts"][0]
    attempt.pop("invocations")
    attempt.pop("resolution")
    attempt.pop("materialization_id")
    with pytest.raises(Exception):
        validator.validate(mislabeled_v2)


def test_circuit_reasons_are_record_v3_only_and_history_can_mix_versions() -> None:
    validator = _current_validator()
    circuit = copy.deepcopy(_current_golden("runs-show.json"))
    task = circuit["data"]["record"]["tasks"][0]
    attempt = task["attempts"][0]
    source = {"task": {"task_name": "source"}, "partition": {"present": False}}
    circuit_ref = [source]
    task.update(
        status="skipped", reason="resource_circuit_open", blocked_by=circuit_ref
    )
    attempt.update(
        status="skipped",
        reason="resource_circuit_open",
        blocked_by=circuit_ref,
        resolution=None,
        materialization_id=None,
    )
    validator.validate(circuit)

    for old_version in (1, 2):
        old = copy.deepcopy(circuit)
        old["data"]["record_schema_version"] = old_version
        for old_task in old["data"]["record"]["tasks"]:
            for old_attempt in old_task["attempts"]:
                old_attempt.pop("resolution")
                old_attempt.pop("materialization_id")
                if old_version == 1:
                    old_attempt.pop("invocations")
        with pytest.raises(Exception):
            validator.validate(old)

    invalid_attempt_reason = copy.deepcopy(circuit)
    invalid_attempt_reason["data"]["record"]["tasks"][0]["attempts"][0]["reason"] = (
        "mixed_blocked"
    )
    with pytest.raises(Exception):
        validator.validate(invalid_attempt_reason)

    summaries = copy.deepcopy(_current_golden("runs-list.json"))
    summaries["data"]["runs"].extend(
        [
            {
                "run_id": "LEGACY_V1",
                "record_schema_version": 1,
                "status": "success",
                "saved_at": "2026-08-12T00:00:02+00:00",
            },
            {
                "run_id": "LEGACY_V2",
                "record_schema_version": 2,
                "status": "success",
                "saved_at": "2026-08-12T00:00:02+00:00",
            },
        ]
    )
    validator.validate(summaries)

    mixed = copy.deepcopy(_current_golden("runs-compare.json"))
    left = mixed["data"]["left"]
    left["record_schema_version"] = 2
    for task in left["record"]["tasks"]:
        for attempt in task["attempts"]:
            attempt.pop("resolution")
            attempt.pop("materialization_id")
    right = mixed["data"]["right"]
    right["record_schema_version"] = 3
    right_task = right["record"]["tasks"][0]
    right_attempt = right_task["attempts"][0]
    right_attempt.pop("resolution")
    right_attempt.pop("materialization_id")
    right_task.update(status="skipped", reason="mixed_blocked")
    right_task["blocked_by"] = [
        {"task": {"task_name": "dependency"}, "partition": {"present": False}},
        {"task": {"task_name": "source"}, "partition": {"present": False}},
    ]
    right_attempt.update(
        status="skipped",
        reason="dependency_blocked",
        blocked_by=[
            {"task": {"task_name": "dependency"}, "partition": {"present": False}}
        ],
    )
    validator.validate(mixed)


def test_record_v4_provenance_schema_enforces_terminal_semantics() -> None:
    validator = _current_validator()
    valid = copy.deepcopy(_current_golden("runs-show.json"))
    attempt = valid["data"]["record"]["tasks"][0]["attempts"][0]
    attempt.update(resolution="materialized", materialization_id="adapter-id")
    validator.validate(valid)

    missing_materialization_id = copy.deepcopy(valid)
    missing_materialization_id["data"]["record"]["tasks"][0]["attempts"][0][
        "materialization_id"
    ] = None
    with pytest.raises(Exception):
        validator.validate(missing_materialization_id)

    id_without_resolution = copy.deepcopy(valid)
    attempt = id_without_resolution["data"]["record"]["tasks"][0]["attempts"][0]
    attempt.update(resolution=None, materialization_id="adapter-id")
    with pytest.raises(Exception):
        validator.validate(id_without_resolution)

    failed_with_provenance = copy.deepcopy(valid)
    task = failed_with_provenance["data"]["record"]["tasks"][0]
    task["status"] = "failed"
    attempt = task["attempts"][0]
    attempt.update(
        status="failed",
        resolution="executed",
        materialization_id="adapter-id",
    )
    with pytest.raises(Exception):
        validator.validate(failed_with_provenance)


def test_v1_schemas_reject_an_incomplete_completed_document() -> None:
    with pytest.raises(Exception):
        _validator().validate({"document_type": "kazeflow.plan", "data": {}})


def test_live_cli_documents_validate_and_match_normalized_goldens(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = _validator()
    plan_validator = _current_validator()
    entry = _script(
        tmp_path,
        "flow.py",
        """
from kazeflow import DatePartitionDef, Flow, asset

@asset
def extract():
    return "private output"

@asset(partition_def=DatePartitionDef())
def publish(extract):
    return extract

flow = Flow(["publish"])
""",
    )
    failed_entry = _script(
        tmp_path,
        "failed.py",
        """
from kazeflow import asset

@asset
def publish():
    raise RuntimeError("application failure")
""",
    )
    circuit_entry = _script(
        tmp_path,
        "circuit.py",
        """
from kazeflow import Flow, OpenCircuit, ResourcePolicy, asset

@asset(resource="network")
def a_source():
    raise PermissionError("resource unavailable")

@asset(resource="network")
def z_pending():
    return "must not run"

def classify(error): return OpenCircuit()

flow = Flow(["a_source", "z_pending"], resources={"network": ResourcePolicy(max_concurrency=1, classify_failure=classify)})
""",
    )

    status, assets, _ = _run(capsys, ["assets", str(entry), "--format", "json"])
    assert status == 0
    validator.validate(assets)
    assert assets == _golden("assets.json")

    status, partitions, _ = _run(capsys, ["partitions", str(entry), "--format", "json"])
    assert status == 0
    validator.validate(partitions)
    assert partitions == _golden("partitions.json")

    status, plan_keys, _ = _run(
        capsys,
        [
            "plan",
            str(entry),
            "--max-concurrency",
            "2",
            "--partition-key",
            "2026-08-10",
            "--partition-key",
            "2026-08-11",
            "--format",
            "json",
        ],
    )
    assert status == 0
    plan_validator.validate(plan_keys)
    assert plan_keys == _current_golden("plan-keys.json")

    status, plan_range, _ = _run(
        capsys,
        [
            "plan",
            str(entry),
            "--partition-range",
            "2026-08-10",
            "2026-08-12",
            "--format",
            "json",
        ],
    )
    assert status == 0
    plan_validator.validate(plan_range)
    assert plan_range == _current_golden("plan-range.json")

    status, plan_empty, _ = _run(
        capsys,
        ["plan", str(entry), "--empty-partitions", "--format", "json"],
    )
    assert status == 0
    plan_validator.validate(plan_empty)
    assert plan_empty == _current_golden("plan-empty.json")

    simple_entry = _script(
        tmp_path,
        "simple.py",
        """
from kazeflow import asset

@asset
def publish():
    return "private output"
""",
    )
    store = tmp_path / "runs.sqlite3"
    status, success, _ = _run(
        capsys,
        ["run", str(simple_entry), "--yes", "--store", str(store), "--format", "json"],
    )
    assert status == 0
    validator.validate(success)
    success_id = success["data"]["record"]["run_id"]
    assert isinstance(success_id, str)
    assert _normalized(success, run_ids={success_id: "RUN_ID"}) == _golden(
        "run-success.json"
    )

    status, failed, _ = _run(
        capsys,
        ["run", str(failed_entry), "--yes", "--store", str(store), "--format", "json"],
    )
    assert status == 1
    validator.validate(failed)
    failed_id = failed["data"]["record"]["run_id"]
    assert isinstance(failed_id, str)
    assert _normalized(failed, run_ids={failed_id: "RUN_ID"}) == _golden(
        "run-failed.json"
    )

    status, circuit, _ = _run(
        capsys,
        ["run", str(circuit_entry), "--yes", "--format", "json"],
    )
    assert status == 1
    validator.validate(circuit)
    circuit_tasks = circuit["data"]["record"]["tasks"]
    assert circuit_tasks[1]["reason"] == "resource_circuit_open"
    assert circuit_tasks[1]["blocked_by"] == [
        {
            "task": {"task_name": "a_source"},
            "partition": {"present": False},
        }
    ]
    assert _normalized(
        circuit, run_ids={circuit["data"]["record"]["run_id"]: "RUN_ID"}
    ) == _golden("run-circuit-open.json")

    stdin = _TTY("no\n")
    stderr = _TTY()
    monkeypatch.setattr(cli.sys, "stdin", stdin)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    status = main(["run", str(simple_entry), "--format", "json"])
    declined = json.loads(capsys.readouterr().out)
    assert status == 0
    validator.validate(declined)
    assert declined == _golden("run-declined.json")

    status, listing, _ = _run(
        capsys,
        ["runs", "list", "--limit", "1", "--store", str(store), "--format", "json"],
    )
    assert status == 0
    validator.validate(listing)
    assert _normalized(listing, run_ids={success_id: "RUN_ID"}) == _golden(
        "runs-list.json"
    )

    status, shown, _ = _run(
        capsys, ["runs", "show", success_id, "--store", str(store), "--format", "json"]
    )
    assert status == 0
    validator.validate(shown)
    assert _normalized(shown, run_ids={success_id: "RUN_ID"}) == _golden(
        "runs-show.json"
    )

    status, compared, _ = _run(
        capsys,
        [
            "runs",
            "compare",
            failed_id,
            success_id,
            "--store",
            str(store),
            "--format",
            "json",
        ],
    )
    assert status == 0
    validator.validate(compared)
    assert _normalized(
        compared, run_ids={failed_id: "LEFT_RUN_ID", success_id: "RIGHT_RUN_ID"}
    ) == _golden("runs-compare.json")
