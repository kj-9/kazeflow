"""Integration coverage for the stdlib-only inspection CLI."""

from __future__ import annotations

import json
import io
import sys
from pathlib import Path

import pytest

import kazeflow.cli as cli
from kazeflow.cli import main
from kazeflow.sqlite_store import SQLiteRunStore


def _script(tmp_path: Path, name: str, source: str) -> Path:
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    return path


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    status = main(argv)
    captured = capsys.readouterr()
    return status, captured.out, captured.err


class _TTY(io.StringIO):
    def isatty(self) -> bool:
        return True


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


def test_run_prompts_on_a_tty_then_executes_only_after_yes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "ran.txt"
    entry = _script(
        tmp_path,
        "approved.py",
        f"""
from pathlib import Path
from kazeflow import asset

marker = Path({str(marker)!r})

@asset
def publish():
    marker.write_text('ran', encoding='utf-8')
""",
    )
    stdin = _TTY("yes\n")
    stderr = _TTY()
    monkeypatch.setattr(cli.sys, "stdin", stdin)
    monkeypatch.setattr(cli.sys, "stderr", stderr)

    status = main(["run", str(entry)])
    stdout = capsys.readouterr().out

    assert status == 0
    assert marker.read_text(encoding="utf-8") == "ran"
    assert stdout.startswith("Run result:\n")
    assert "Planned run:" in stderr.getvalue()
    assert "Proceed? [y/N]" in stderr.getvalue()


def test_run_decline_is_a_successful_no_op_without_adapter_initialization(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = _script(
        tmp_path,
        "declined.py",
        """
from kazeflow import asset

@asset
def never_run():
    raise AssertionError('asset body must not run')
""",
    )
    stdin = _TTY("no\n")
    stderr = _TTY()
    monkeypatch.setattr(cli.sys, "stdin", stdin)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(cli, "_execute", lambda *_args, **_kwargs: pytest.fail("run"))
    monkeypatch.setattr(
        cli, "_save_result", lambda *_args, **_kwargs: pytest.fail("store")
    )

    status = main(["run", str(entry), "--tui", "--store", str(tmp_path / "run.db")])
    stdout = capsys.readouterr().out

    assert status == 0
    assert stdout == ""
    assert not (tmp_path / "run.db").exists()
    assert "run cancelled" in stderr.getvalue()


def test_run_requires_yes_without_a_terminal_and_does_not_invoke_assets(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    marker = tmp_path / "ran.txt"
    entry = _script(
        tmp_path,
        "noninteractive.py",
        f"""
from pathlib import Path
from kazeflow import asset

marker = Path({str(marker)!r})

@asset
def never_run():
    marker.write_text('ran', encoding='utf-8')
""",
    )

    status, stdout, stderr = _run(capsys, ["run", str(entry), "--format", "json"])

    assert status == 2
    assert stdout == ""
    assert not marker.exists()
    assert "Planned run:" in stderr
    assert "--yes is required" in stderr


def test_run_yes_emits_one_portable_json_record_after_preflight(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "json_run.py",
        """
from kazeflow import asset

@asset
def publish():
    return {'secret': 'raw output must not be serialized'}
""",
    )

    status, stdout, stderr = _run(
        capsys, ["run", str(entry), "--yes", "--format", "json"]
    )

    assert status == 0
    record = json.loads(stdout)
    assert record["status"] == "success"
    assert "raw output must not be serialized" not in stdout
    assert "Planned run:" in stderr
    assert "Run result:" not in stderr


def test_run_reports_asset_failure_as_completed_json_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = _script(
        tmp_path,
        "failed.py",
        """
from kazeflow import asset

@asset
def broken():
    raise RuntimeError('expected failure')
""",
    )

    status, stdout, stderr = _run(
        capsys, ["run", str(entry), "--yes", "--format", "json"]
    )

    assert status == 1
    assert json.loads(stdout)["status"] == "failed"
    assert "Planned run:" in stderr


def test_run_rejects_ambiguous_discovered_targets_without_invocation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    marker = tmp_path / "ran.txt"
    entry = _script(
        tmp_path,
        "ambiguous.py",
        f"""
from pathlib import Path
from kazeflow import asset

marker = Path({str(marker)!r})

@asset
def alpha():
    marker.write_text('alpha', encoding='utf-8')

@asset
def beta():
    marker.write_text('beta', encoding='utf-8')
""",
    )

    status, stdout, stderr = _run(
        capsys, ["run", str(entry), "--yes", "--format", "json"]
    )

    assert status == 2
    assert stdout == ""
    assert not marker.exists()
    assert "requires --target" in stderr


def test_run_saves_requested_result_and_store_failure_takes_precedence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    success_entry = _script(
        tmp_path,
        "stored.py",
        """
from kazeflow import asset

@asset
def publish():
    return 'stored'
""",
    )
    database = tmp_path / "runs.db"

    status, stdout, stderr = _run(
        capsys,
        [
            "run",
            str(success_entry),
            "--yes",
            "--store",
            str(database),
            "--format",
            "json",
        ],
    )

    assert status == 0
    assert "Planned run:" in stderr
    record = json.loads(stdout)
    with SQLiteRunStore(database) as store:
        assert store.load(record["run_id"]).record == record

    failed_entry = _script(
        tmp_path,
        "store_precedence.py",
        """
from kazeflow import asset

@asset
def broken():
    raise RuntimeError('asset failure')
""",
    )

    def fail_save(*_args: object, **_kwargs: object) -> None:
        raise cli._InfrastructureError("SQLite store failed: simulated")

    monkeypatch.setattr(cli, "_save_result", fail_save)
    status, stdout, stderr = _run(
        capsys,
        [
            "run",
            str(failed_entry),
            "--yes",
            "--store",
            str(database),
            "--format",
            "json",
        ],
    )

    assert status == 4
    assert stdout == ""
    assert "SQLite store failed: simulated" in stderr


def test_run_unavailable_tui_fails_before_invoking_an_asset(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "ran.txt"
    entry = _script(
        tmp_path,
        "tui_missing.py",
        f"""
from pathlib import Path
from kazeflow import asset

marker = Path({str(marker)!r})

@asset
def publish():
    marker.write_text('ran', encoding='utf-8')
""",
    )
    monkeypatch.setitem(sys.modules, "rich", None)
    monkeypatch.delitem(sys.modules, "rich.console", raising=False)

    status, stdout, stderr = _run(
        capsys, ["run", str(entry), "--yes", "--tui", "--format", "json"]
    )

    assert status == 4
    assert stdout == ""
    assert not marker.exists()
    assert "TUI adapter failed" in stderr


def test_runs_list_uses_project_default_store_and_limits_summaries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = _script(
        tmp_path,
        "stored_default.py",
        """
from kazeflow import asset

@asset
def publish():
    return 'private output'
""",
    )
    store_path = tmp_path / ".kazeflow" / "runs.sqlite3"
    store_path.parent.mkdir()
    status, stdout, _stderr = _run(
        capsys,
        ["run", str(entry), "--yes", "--store", str(store_path), "--format", "json"],
    )
    assert status == 0
    run_id = json.loads(stdout)["run_id"]

    monkeypatch.chdir(tmp_path)
    status, stdout, stderr = _run(
        capsys, ["runs", "list", "--limit", "1", "--format", "json"]
    )

    assert status == 0
    assert stderr == ""
    history = json.loads(stdout)
    assert history["schema_version"] == 1
    assert history["runs"][0]["run_id"] == run_id
    assert history["runs"][0]["schema_version"] == 1
    assert history["runs"][0]["status"] == "success"
    assert history["runs"][0]["saved_at"].endswith("+00:00")


def test_runs_show_and_compare_preserve_portable_boundaries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    success = _script(
        tmp_path,
        "history_success.py",
        """
from kazeflow import asset

@asset
def publish():
    return 'private output'
""",
    )
    failure = _script(
        tmp_path,
        "history_failure.py",
        """
from kazeflow import asset

@asset
def publish():
    raise RuntimeError('private failure details')
""",
    )
    database = tmp_path / "history.sqlite3"
    status, stdout, _stderr = _run(
        capsys,
        ["run", str(success), "--yes", "--store", str(database), "--format", "json"],
    )
    assert status == 0
    success_id = json.loads(stdout)["run_id"]
    status, stdout, _stderr = _run(
        capsys,
        ["run", str(failure), "--yes", "--store", str(database), "--format", "json"],
    )
    assert status == 1
    failure_id = json.loads(stdout)["run_id"]

    status, stdout, stderr = _run(
        capsys,
        ["runs", "show", success_id, "--store", str(database), "--format", "json"],
    )
    assert status == 0
    assert stderr == ""
    shown = json.loads(stdout)
    assert shown["run_id"] == success_id
    assert shown["record"]["tasks"][0]["attempts"][0]["attempt"]["partition"] == {
        "present": False
    }
    assert "private output" not in stdout

    status, stdout, stderr = _run(
        capsys,
        [
            "runs",
            "compare",
            failure_id,
            success_id,
            "--store",
            str(database),
            "--format",
            "json",
        ],
    )
    assert status == 0
    assert stderr == ""
    compared = json.loads(stdout)
    assert compared["left"]["run_id"] == failure_id
    assert compared["right"]["run_id"] == success_id
    task = compared["comparison"]["tasks"][0]
    assert task["task_name"] == "publish"
    assert task["left"]["failure_exception_types"] == ["RuntimeError"]
    assert "private failure details" not in json.dumps(compared["comparison"])


def test_runs_history_errors_do_not_create_a_store_or_emit_success_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    default_store = tmp_path / ".kazeflow" / "runs.sqlite3"

    status, stdout, stderr = _run(capsys, ["runs", "list", "--format", "json"])
    assert status == 4
    assert stdout == ""
    assert stderr
    assert not default_store.exists()

    database = tmp_path / "empty.sqlite3"
    with SQLiteRunStore(database):
        pass
    status, stdout, stderr = _run(
        capsys,
        ["runs", "show", "missing", "--store", str(database), "--format", "json"],
    )
    assert status == 2
    assert stdout == ""
    assert "run not found: missing" in stderr

    status, stdout, stderr = _run(
        capsys,
        ["runs", "list", "--store", str(database), "--limit", "-1", "--format", "json"],
    )
    assert status == 2
    assert stdout == ""
    assert "--limit" in stderr
