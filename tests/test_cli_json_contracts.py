"""Executable evidence for the published version-1 CLI JSON contract."""

from __future__ import annotations

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
_SCHEMA_PATH = _ROOT / "docs/user/schemas/cli/v1/schema.json"
_GOLDENS = _ROOT / "tests/fixtures/cli-json/v1"


def _validator() -> Any:
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _golden(name: str) -> dict[str, Any]:
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

    assert len(golden_paths) == 11
    for path in golden_paths:
        validator.validate(json.loads(path.read_text(encoding="utf-8")))


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


def test_v1_schemas_reject_an_incomplete_completed_document() -> None:
    with pytest.raises(Exception):
        _validator().validate({"document_type": "kazeflow.plan", "data": {}})


def test_live_cli_documents_validate_and_match_normalized_goldens(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = _validator()
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
    validator.validate(plan_keys)
    assert plan_keys == _golden("plan-keys.json")

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
    validator.validate(plan_range)
    assert plan_range == _golden("plan-range.json")

    status, plan_empty, _ = _run(
        capsys,
        ["plan", str(entry), "--empty-partitions", "--format", "json"],
    )
    assert status == 0
    validator.validate(plan_empty)
    assert plan_empty == _golden("plan-empty.json")

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
