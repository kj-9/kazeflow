"""Integration coverage for the stdlib-only inspection CLI."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from kazeflow.cli import main


def _script(tmp_path: Path, name: str, source: str) -> Path:
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    return path


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    status = main(argv)
    captured = capsys.readouterr()
    return status, captured.out, captured.err


def test_assets_and_plan_use_declared_flow_and_emit_json_only_to_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "declared.py",
        """
from kazeflow import Flow, asset

@asset
def source():
    return 'source body must not run'

@asset
def publish(source):
    return 'publish body must not run'

@asset
def unrelated():
    return 'unrelated body must not run'

flow = Flow(['publish'])
""",
    )

    status, stdout, stderr = _run(capsys, ["assets", str(entry), "--format", "json"])

    assert status == 0
    assert stderr == ""
    assets = json.loads(stdout)
    assert assets["schema_version"] == 1
    assert assets["declared_flow"] is True
    assert [asset["name"] for asset in assets["assets"]] == [
        "publish",
        "source",
        "unrelated",
    ]

    status, stdout, stderr = _run(
        capsys,
        ["plan", str(entry), "--max-concurrency", "2", "--format", "json"],
    )

    assert status == 0
    assert stderr == ""
    plan = json.loads(stdout)
    assert plan["schema_version"] == 1
    assert plan["targets"] == ["publish"]
    assert plan["config"] == {"max_concurrency": 2, "partition_key_count": None}
    assert [task["name"] for task in plan["tasks"]] == ["source", "publish"]
    assert "unrelated" not in [task["name"] for task in plan["tasks"]]


def test_undeclared_script_lists_assets_and_plans_derived_terminal_target(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "undeclared.py",
        """
from kazeflow import asset

@asset
def extract():
    return 'not called'

@asset
def transform(extract):
    return 'not called'
""",
    )

    status, stdout, stderr = _run(capsys, ["assets", str(entry)])

    assert status == 0
    assert stderr == ""
    assert stdout.splitlines() == [
        "Assets:",
        "- extract (dependencies: none; partitioned: no)",
        "- transform (dependencies: extract; partitioned: no)",
    ]

    status, stdout, stderr = _run(capsys, ["plan", str(entry), "--format", "json"])

    assert status == 0
    assert stderr == ""
    plan = json.loads(stdout)
    assert plan["targets"] == ["transform"]
    assert [task["name"] for task in plan["tasks"]] == ["extract", "transform"]


def test_plan_combines_multiple_terminal_assets_and_honors_target_selection(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "multiple.py",
        """
from kazeflow import asset

@asset
def common():
    return 'not called'

@asset
def alpha(common):
    return 'not called'

@asset
def beta(common):
    return 'not called'
""",
    )

    status, stdout, stderr = _run(capsys, ["plan", str(entry), "--format", "json"])

    assert status == 0
    assert stderr == ""
    assert json.loads(stdout)["targets"] == ["alpha", "beta"]

    status, stdout, stderr = _run(
        capsys, ["plan", str(entry), "--target", "beta", "--format", "json"]
    )

    assert status == 0
    assert stderr == ""
    plan = json.loads(stdout)
    assert plan["targets"] == ["beta"]
    assert [task["name"] for task in plan["tasks"]] == ["common", "beta"]


def test_plan_loads_top_level_python_without_invoking_asset_body(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    marker = tmp_path / "effects.txt"
    entry = _script(
        tmp_path,
        "effects.py",
        f"""
from pathlib import Path
from kazeflow import asset

marker = Path({str(marker)!r})
marker.write_text('import\\n', encoding='utf-8')

@asset
def dangerous():
    marker.write_text(marker.read_text(encoding='utf-8') + 'asset-body\\n', encoding='utf-8')
""",
    )

    status, stdout, stderr = _run(capsys, ["plan", str(entry)])

    assert status == 0
    assert stderr == ""
    assert "dangerous" in stdout
    assert marker.read_text(encoding="utf-8") == "import\n"


def test_plan_projects_partition_selection_without_exposing_raw_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "partitioned.py",
        """
from kazeflow import DatePartitionDef, Flow, asset

@asset(partition_def=DatePartitionDef())
def publish():
    return 'not called'

flow = Flow(['publish'])
""",
    )

    status, stdout, stderr = _run(
        capsys,
        [
            "plan",
            str(entry),
            "--partition-key",
            "secret-east",
            "--partition-key",
            "secret-west",
            "--format",
            "json",
        ],
    )

    assert status == 0
    assert stderr == ""
    plan = json.loads(stdout)
    assert plan["config"]["partition_key_count"] == 2
    assert plan["tasks"] == [
        {"name": "publish", "dependencies": [], "partition_key_count": 2}
    ]
    assert "secret-east" not in stdout
    assert "secret-west" not in stdout


@pytest.mark.parametrize(
    ("argv", "expected_status"),
    [
        (["plan", "not-an-entry:"], 2),
        (["plan", "missing.py"], 3),
    ],
)
def test_invalid_entry_and_configuration_fail_without_json_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    expected_status: int,
) -> None:
    status, stdout, stderr = _run(capsys, [*argv, "--format", "json"])

    assert status == expected_status
    assert stdout == ""
    assert stderr

    entry = _script(
        tmp_path,
        "valid.py",
        """
from kazeflow import asset

@asset
def only_asset():
    return 'not called'
""",
    )
    status, stdout, stderr = _run(
        capsys,
        ["plan", str(entry), "--target", "unknown", "--format", "json"],
    )

    assert status == 2
    assert stdout == ""
    assert stderr

    status, stdout, stderr = _run(
        capsys,
        ["plan", str(entry), "--max-concurrency", "0", "--format", "json"],
    )

    assert status == 2
    assert stdout == ""
    assert stderr


def test_inspection_does_not_import_optional_tui_or_storage_modules(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sys.modules.pop("kazeflow.tui", None)
    sys.modules.pop("kazeflow.sqlite_store", None)
    entry = _script(
        tmp_path,
        "core_only.py",
        """
from kazeflow import asset

@asset
def inspectable():
    return 'not called'
""",
    )

    status, _stdout, stderr = _run(capsys, ["assets", str(entry)])

    assert status == 0
    assert stderr == ""
    assert "kazeflow.tui" not in sys.modules
    assert "kazeflow.sqlite_store" not in sys.modules
