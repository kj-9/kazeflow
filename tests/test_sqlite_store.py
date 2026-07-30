from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import cast

import pytest

from kazeflow.results import (
    AttemptReference,
    AttemptResult,
    AttemptStatus,
    FailureInfo,
    FlowStatus,
    RunResult,
    SkipReason,
    TaskReference,
    TaskResult,
)
from kazeflow.sqlite_store import CURRENT_SCHEMA_VERSION, SQLiteRunStore


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
LATER = NOW + timedelta(seconds=2)
DURATION = timedelta(seconds=2)


class OpaqueValue:
    def __repr__(self) -> str:
        return "opaque-value-must-not-persist"


def _tasks(record: dict[str, object]) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], record["tasks"])


def _attempts(task: dict[str, object]) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], task["attempts"])


def _attempt(
    name: str,
    status: AttemptStatus,
    *,
    partition_key: object | None = None,
    output: object = None,
    exception: BaseException | None = None,
    failure: FailureInfo | None = None,
    reason: SkipReason | None = None,
    blocked_by: tuple[AttemptReference, ...] = (),
) -> AttemptResult:
    reference = AttemptReference(
        TaskReference(name),
        partition_key_present=partition_key is not None,
        partition_key=partition_key,
    )
    return AttemptResult(
        reference,
        status,
        NOW,
        LATER,
        DURATION,
        output=output,
        exception=exception,
        failure=failure,
        reason=reason,
        blocked_by=blocked_by,
    )


def _unpartitioned_result(
    run_id: str, status: AttemptStatus = AttemptStatus.SUCCESS
) -> RunResult:
    attempt = _attempt("plain", status, output=OpaqueValue())
    task = TaskResult(
        TaskReference("plain"), False, status, NOW, LATER, DURATION, (attempt,)
    )
    flow_status = (
        FlowStatus.SUCCESS if status is AttemptStatus.SUCCESS else FlowStatus.CANCELLED
    )
    return RunResult(run_id, flow_status, NOW, LATER, DURATION, (task,))


def _failed_partitioned_result(run_id: str) -> RunResult:
    source_ok = _attempt("source", AttemptStatus.SUCCESS, partition_key=0)
    source_failed = _attempt(
        "source",
        AttemptStatus.FAILED,
        partition_key="",
        output=OpaqueValue(),
        exception=RuntimeError("raw exception must not persist"),
        failure=FailureInfo("RuntimeError", "portable failure", "portable traceback"),
    )
    source = TaskResult(
        TaskReference("source"),
        True,
        AttemptStatus.FAILED,
        NOW,
        LATER,
        DURATION,
        (source_ok, source_failed),
    )
    target_ok = _attempt(
        "target", AttemptStatus.SUCCESS, partition_key=0, output=OpaqueValue()
    )
    target_blocked = _attempt(
        "target",
        AttemptStatus.SKIPPED,
        partition_key="",
        reason=SkipReason.DEPENDENCY_BLOCKED,
        blocked_by=(source_failed.attempt,),
    )
    target = TaskResult(
        TaskReference("target"),
        True,
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        (target_ok, target_blocked),
    )
    return RunResult(run_id, FlowStatus.FAILED, NOW, LATER, DURATION, (source, target))


def _no_work_result(run_id: str) -> RunResult:
    task = TaskResult(
        TaskReference("partitioned"),
        True,
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        reason=SkipReason.NO_PARTITION_KEYS,
    )
    return RunResult(run_id, FlowStatus.SUCCESS, NOW, LATER, DURATION, (task,))


def _falsey_partition_result(run_id: str, key: object) -> RunResult:
    attempt = _attempt("partitioned", AttemptStatus.SUCCESS, partition_key=key)
    task = TaskResult(
        TaskReference("partitioned"),
        True,
        AttemptStatus.SUCCESS,
        NOW,
        LATER,
        DURATION,
        (attempt,),
    )
    return RunResult(run_id, FlowStatus.SUCCESS, NOW, LATER, DURATION, (task,))


def test_save_load_preserves_the_exact_portable_failure_partition_record(
    tmp_path,
) -> None:
    result = _failed_partitioned_result("failed-partitioned")

    with SQLiteRunStore(tmp_path / "runs.sqlite3") as store:
        saved = store.save(result)
        loaded = store.load(result.run_id)

    assert saved.schema_version == CURRENT_SCHEMA_VERSION
    assert loaded.schema_version == CURRENT_SCHEMA_VERSION
    assert loaded.record == result.to_record()
    tasks = _tasks(loaded.record)
    assert [cast(dict[str, object], task["task"])["task_name"] for task in tasks] == [
        "source",
        "target",
    ]
    source_attempts = _attempts(tasks[0])
    target_attempts = _attempts(tasks[1])
    assert [attempt["status"] for attempt in source_attempts] == [
        "success",
        "failed",
    ]
    assert source_attempts[1]["failure"] == {
        "exception_type": "RuntimeError",
        "message": "portable failure",
        "traceback": "portable traceback",
    }
    assert target_attempts[1]["blocked_by"] == [
        {"task": {"task_name": "source"}, "partition": {"present": True}}
    ]


def test_loaded_record_decodes_a_fresh_independent_dictionary(tmp_path) -> None:
    result = _failed_partitioned_result("fresh-record")

    with SQLiteRunStore(tmp_path / "runs.sqlite3") as store:
        loaded = store.save(result)
        first = loaded.record
        _attempts(_tasks(first)[0])[0]["status"] = "tampered"

        assert loaded.record == result.to_record()


@pytest.mark.parametrize(
    "record_json",
    [
        "{not valid json",
        "[]",
        "{}",
    ],
    ids=["malformed-json", "non-object", "missing-required-top-level-fields"],
)
def test_load_rejects_malformed_or_invalid_record_json(
    tmp_path, record_json: str
) -> None:
    path = tmp_path / "corrupt-record.sqlite3"
    result = _unpartitioned_result("corrupt-record")

    with SQLiteRunStore(path) as store:
        store.save(result)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE runs SET record_json = ? WHERE run_id = ?",
                (record_json, result.run_id),
            )

        with pytest.raises(ValueError):
            store.load(result.run_id)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "another-run"),
        ("status", "failed"),
    ],
    ids=["run-id", "status"],
)
def test_load_rejects_record_envelope_mismatches(
    tmp_path, field: str, value: str
) -> None:
    path = tmp_path / "mismatched-record.sqlite3"
    result = _unpartitioned_result("matching-envelope")

    with SQLiteRunStore(path) as store:
        saved = store.save(result)
        record = saved.record
        record[field] = value
        record_json = json.dumps(record, sort_keys=True)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE runs SET record_json = ? WHERE run_id = ?",
                (record_json, result.run_id),
            )

        with pytest.raises(ValueError):
            store.load(result.run_id)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", ""),
        ("schema_version", "not-an-integer"),
        ("status", "not-a-terminal-status"),
        ("saved_at", "not-an-iso-timestamp"),
    ],
    ids=[
        "empty-run-id",
        "non-integer-schema-version",
        "invalid-status",
        "invalid-saved-at",
    ],
)
def test_list_runs_rejects_invalid_summary_rows(
    tmp_path, field: str, value: str
) -> None:
    path = tmp_path / "corrupt-summary.sqlite3"
    result = _unpartitioned_result("summary-row")

    with SQLiteRunStore(path) as store:
        store.save(result)
        with sqlite3.connect(path) as connection:
            connection.execute(
                f"UPDATE runs SET {field} = ? WHERE run_id = ?",
                (value, result.run_id),
            )

        with pytest.raises(ValueError):
            store.list_runs()


def test_save_load_list_and_reopen_cover_terminal_record_kinds(tmp_path) -> None:
    path = tmp_path / "history.sqlite3"
    results = (
        _unpartitioned_result("alpha"),
        _unpartitioned_result("beta", AttemptStatus.CANCELLED),
        _no_work_result("gamma"),
    )

    with SQLiteRunStore(path) as store:
        for result in results:
            store.save(result)
        summaries = store.list_runs()
        assert [summary.run_id for summary in summaries] == ["alpha", "beta", "gamma"]
        assert [summary.status for summary in summaries] == [
            "success",
            "cancelled",
            "success",
        ]
        assert all(
            summary.schema_version == CURRENT_SCHEMA_VERSION for summary in summaries
        )
        assert store.list_runs(limit=2) == summaries[:2]

    with SQLiteRunStore(path) as reopened:
        assert reopened.load("alpha").record == results[0].to_record()
        assert reopened.load("beta").record == results[1].to_record()
        assert reopened.load("gamma").record == results[2].to_record()


def test_duplicate_saves_and_missing_loads_do_not_modify_history(tmp_path) -> None:
    result = _unpartitioned_result("only-once")

    with SQLiteRunStore(tmp_path / "runs.sqlite3") as store:
        store.save(result)
        with pytest.raises(ValueError, match="already exists"):
            store.save(result)
        with pytest.raises(KeyError, match="missing"):
            store.load("missing")
        assert [summary.run_id for summary in store.list_runs()] == ["only-once"]


@pytest.mark.parametrize("key", [0, "", False, OpaqueValue()])
def test_portable_records_keep_partition_presence_without_raw_values(
    tmp_path, key
) -> None:
    result = _falsey_partition_result(f"partition-{type(key).__name__}", key)

    with SQLiteRunStore(tmp_path / "runs.sqlite3") as store:
        loaded = store.save(result)

    attempt = _attempts(_tasks(loaded.record)[0])[0]
    assert cast(dict[str, object], attempt["attempt"])["partition"] == {"present": True}
    assert "partition_key" not in loaded.record_json
    assert "opaque-value-must-not-persist" not in loaded.record_json
    assert loaded.record == result.to_record()


def test_nonserializable_outputs_and_exceptions_do_not_prevent_persistence(
    tmp_path,
) -> None:
    result = _failed_partitioned_result("nonserializable")

    with SQLiteRunStore(tmp_path / "runs.sqlite3") as store:
        loaded = store.save(result)

    assert "raw exception must not persist" not in loaded.record_json
    assert "opaque-value-must-not-persist" not in loaded.record_json
    assert "output" not in loaded.record_json
    assert '"exception":' not in loaded.record_json
