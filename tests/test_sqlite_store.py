from dataclasses import replace
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
from kazeflow.sqlite_store import (
    CURRENT_RECORD_SCHEMA_VERSION,
    CURRENT_SCHEMA_VERSION,
    SQLiteRunStore,
)


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

    assert saved.schema_version == CURRENT_RECORD_SCHEMA_VERSION
    assert loaded.schema_version == CURRENT_RECORD_SCHEMA_VERSION
    assert loaded.record == result.to_record(schema_version=4)
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

        assert loaded.record == result.to_record(schema_version=4)


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
            summary.schema_version == CURRENT_RECORD_SCHEMA_VERSION
            for summary in summaries
        )
        assert store.list_runs(limit=2) == summaries[:2]

    with SQLiteRunStore(path) as reopened:
        assert reopened.load("alpha").record == results[0].to_record(schema_version=4)
        assert reopened.load("beta").record == results[1].to_record(schema_version=4)
        assert reopened.load("gamma").record == results[2].to_record(schema_version=4)


def test_duplicate_saves_and_missing_loads_do_not_modify_history(tmp_path) -> None:
    result = _unpartitioned_result("only-once")

    with SQLiteRunStore(tmp_path / "runs.sqlite3") as store:
        store.save(result)
        with pytest.raises(ValueError, match="already exists"):
            store.save(result)
        with pytest.raises(KeyError, match="missing"):
            store.load("missing")
        assert [summary.run_id for summary in store.list_runs()] == ["only-once"]


@pytest.mark.parametrize("invalid_record", ["empty", "mismatched-envelope"])
def test_invalid_runresult_projection_does_not_persist_or_block_retry(
    tmp_path, invalid_record: str
) -> None:
    valid = _unpartitioned_result("retry-after-invalid-projection")

    class InvalidProjection(RunResult):
        def to_record(self, *, schema_version: int = 1) -> dict[str, object]:
            if invalid_record == "empty":
                return {}
            record = super().to_record(schema_version=schema_version)
            record["run_id"] = "different-run-id"
            record["status"] = "failed"
            return record

    invalid = InvalidProjection(
        valid.run_id,
        valid.status,
        valid.started_at,
        valid.ended_at,
        valid.duration,
        valid.tasks,
    )

    with SQLiteRunStore(tmp_path / "runs.sqlite3") as store:
        with pytest.raises(ValueError):
            store.save(invalid)
        assert store.list_runs() == ()

        saved = store.save(valid)
        assert saved.record == valid.to_record(schema_version=4)
        assert store.load(valid.run_id).record == valid.to_record(schema_version=4)


def test_constructor_rejects_invalid_paths_before_creating_a_database(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(TypeError, match="str or pathlib.Path"):
        SQLiteRunStore(None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must not be empty"):
        SQLiteRunStore("")

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("path_kind", ["string", "path"])
def test_constructor_accepts_string_and_path_database_paths(
    tmp_path, path_kind: str
) -> None:
    path = tmp_path / f"{path_kind}.sqlite3"
    database_path = str(path) if path_kind == "string" else path

    with SQLiteRunStore(database_path) as store:
        assert store.schema_version == CURRENT_SCHEMA_VERSION

    assert path.exists()


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
    assert loaded.record == result.to_record(schema_version=4)


def test_database_schema_one_reads_mixed_portable_record_versions_without_rewrite(
    tmp_path,
) -> None:
    path = tmp_path / "mixed-record-versions.sqlite3"
    legacy_result = _unpartitioned_result("legacy-v1")
    legacy_json = json.dumps(
        legacy_result.to_record(),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    version2_result = _unpartitioned_result("current-v2")
    version3_result = _unpartitioned_result("current-v3")
    current_result = _unpartitioned_result("current-v4")
    version2_json = json.dumps(
        version2_result.to_record(schema_version=2),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    version3_json = json.dumps(
        version3_result.to_record(schema_version=3),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    with SQLiteRunStore(path) as store:
        store.save(current_result)
        with sqlite3.connect(path) as connection:
            connection.execute(
                """
                INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
                VALUES (?, 1, ?, ?, ?)
                """,
                (
                    legacy_result.run_id,
                    legacy_result.status.value,
                    NOW.isoformat(),
                    legacy_json,
                ),
            )
            connection.execute(
                """
                INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
                VALUES (?, 3, ?, ?, ?)
                """,
                (
                    version3_result.run_id,
                    version3_result.status.value,
                    NOW.isoformat(),
                    version3_json,
                ),
            )
            connection.execute(
                """
                INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
                VALUES (?, 2, ?, ?, ?)
                """,
                (
                    version2_result.run_id,
                    version2_result.status.value,
                    NOW.isoformat(),
                    version2_json,
                ),
            )
        assert store.schema_version == 1
        legacy = store.load(legacy_result.run_id)
        version2 = store.load(version2_result.run_id)
        version3 = store.load(version3_result.run_id)
        current = store.load(current_result.run_id)
        assert legacy.schema_version == 1
        assert legacy.record == legacy_result.to_record()
        assert version2.schema_version == 2
        assert version2.record == version2_result.to_record(schema_version=2)
        assert version3.schema_version == 3
        assert version3.record == version3_result.to_record(schema_version=3)
        assert current.schema_version == 4
        assert current.record == current_result.to_record(schema_version=4)
        assert {entry.schema_version for entry in store.list_runs()} == {1, 2, 3, 4}

    with sqlite3.connect(path) as connection:
        unchanged = connection.execute(
            "SELECT record_json FROM runs WHERE run_id = ?",
            (legacy_result.run_id,),
        ).fetchone()[0]
    assert unchanged == legacy_json
    with sqlite3.connect(path) as connection:
        unchanged_v2 = connection.execute(
            "SELECT record_json FROM runs WHERE run_id = ?",
            (version2_result.run_id,),
        ).fetchone()[0]
    assert unchanged_v2 == version2_json
    with sqlite3.connect(path) as connection:
        unchanged_v3 = connection.execute(
            "SELECT record_json FROM runs WHERE run_id = ?",
            (version3_result.run_id,),
        ).fetchone()[0]
    assert unchanged_v3 == version3_json
    assert CURRENT_SCHEMA_VERSION == 1


def test_v4_store_round_trip_preserves_executed_and_materialized_provenance(
    tmp_path,
) -> None:
    result = _unpartitioned_result("provenance-v4")
    source_attempt = result.tasks[0].attempts[0]
    committed = replace(
        source_attempt, resolution="executed", materialization_id="artifact-17"
    )
    reused = replace(
        source_attempt, resolution="materialized", materialization_id="artifact-17"
    )
    committed_result = replace(
        result,
        run_id="committed-v4",
        tasks=(replace(result.tasks[0], attempts=(committed,)),),
    )
    reused_result = replace(
        result,
        run_id="reused-v4",
        tasks=(replace(result.tasks[0], attempts=(reused,)),),
    )

    with SQLiteRunStore(tmp_path / "provenance-v4.sqlite3") as store:
        committed_record = store.save(committed_result)
        reused_record = store.save(reused_result)
        assert committed_record.schema_version == 4
        assert reused_record.schema_version == 4
        for stored in (committed_record, reused_record):
            attempt = _attempts(_tasks(stored.record)[0])[0]
            assert attempt["resolution"] in {"executed", "materialized"}
            assert attempt["materialization_id"] == "artifact-17"
        assert store.load("committed-v4").record == committed_result.to_record(
            schema_version=4
        )
        assert store.load("reused-v4").record == reused_result.to_record(
            schema_version=4
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("resolution", None, "materialization_id requires a resolution"),
        ("resolution", "other", "invalid resolution"),
        ("resolution", [], "invalid resolution"),
        ("resolution", "materialized", "require a materialization_id"),
        ("materialization_id", "", "must be a non-empty string"),
        ("materialization_id", 3, "must be a non-empty string"),
    ],
)
def test_v4_store_rejects_invalid_provenance_values(
    tmp_path, field: str, value: object, message: str
) -> None:
    path = tmp_path / "invalid-provenance.sqlite3"
    result = _unpartitioned_result("invalid-provenance")
    with SQLiteRunStore(path) as store:
        store.save(result)
        record = result.to_record(schema_version=4)
        attempt = _attempts(_tasks(record)[0])[0]
        if field == "materialization_id":
            attempt["resolution"] = "executed"
        elif value is None:
            attempt["materialization_id"] = "orphaned-id"
        attempt[field] = value
        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE runs SET record_json = ? WHERE run_id = ?",
                (
                    json.dumps(record, allow_nan=False, separators=(",", ":")),
                    result.run_id,
                ),
            )
        with pytest.raises(ValueError, match=message):
            store.load(result.run_id)


@pytest.mark.parametrize("field", ["resolution", "materialization_id"])
def test_v4_store_requires_both_provenance_fields(tmp_path, field: str) -> None:
    path = tmp_path / "missing-provenance.sqlite3"
    result = _unpartitioned_result("missing-provenance")
    with SQLiteRunStore(path) as store:
        store.save(result)
        record = result.to_record(schema_version=4)
        _attempts(_tasks(record)[0])[0].pop(field)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE runs SET record_json = ? WHERE run_id = ?",
                (
                    json.dumps(record, allow_nan=False, separators=(",", ":")),
                    result.run_id,
                ),
            )
        with pytest.raises(ValueError, match="require provenance fields"):
            store.load(result.run_id)


@pytest.mark.parametrize("version", [1, 2, 3])
def test_old_record_versions_reject_provenance_without_rewriting_rows(
    tmp_path, version: int
) -> None:
    path = tmp_path / f"old-v{version}-provenance.sqlite3"
    result = _unpartitioned_result(f"old-v{version}-provenance")
    record = result.to_record(schema_version=version)
    attempt = _attempts(_tasks(record)[0])[0]
    attempt["resolution"] = "materialized"
    attempt["materialization_id"] = "must-not-appear-in-v1-v3"
    record_json = json.dumps(
        record, allow_nan=False, separators=(",", ":"), sort_keys=True
    )
    with SQLiteRunStore(path):
        pass
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                result.run_id,
                version,
                result.status.value,
                NOW.isoformat(),
                record_json,
            ),
        )
    with SQLiteRunStore(path) as store:
        with pytest.raises(ValueError, match="cannot contain provenance"):
            store.load(result.run_id)
    with sqlite3.connect(path) as connection:
        unchanged = connection.execute(
            "SELECT record_json FROM runs WHERE run_id = ?", (result.run_id,)
        ).fetchone()[0]
    assert unchanged == record_json


def test_v4_store_rejects_provenance_on_non_success_attempt(tmp_path) -> None:
    path = tmp_path / "failed-provenance.sqlite3"
    result = _unpartitioned_result("failed-provenance")
    with SQLiteRunStore(path) as store:
        store.save(result)
        record = result.to_record(schema_version=4)
        attempt = _attempts(_tasks(record)[0])[0]
        attempt["status"] = "failed"
        attempt["resolution"] = "executed"
        attempt["materialization_id"] = "must-not-exist"
        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE runs SET record_json = ? WHERE run_id = ?",
                (
                    json.dumps(record, allow_nan=False, separators=(",", ":")),
                    result.run_id,
                ),
            )
        with pytest.raises(ValueError, match="non-success attempts"):
            store.load(result.run_id)


def test_circuit_reasons_are_written_as_v4_and_rejected_in_old_record_versions(
    tmp_path,
) -> None:
    source_failure = FailureInfo("RuntimeError", "closed", "traceback")
    source_attempt = _attempt(
        "source", AttemptStatus.FAILED, partition_key=0, failure=source_failure
    )
    source_task = TaskResult(
        TaskReference("source"),
        True,
        AttemptStatus.FAILED,
        NOW,
        LATER,
        DURATION,
        (source_attempt,),
    )
    target_attempt = _attempt(
        "target",
        AttemptStatus.SKIPPED,
        partition_key="",
        reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
        blocked_by=(source_attempt.attempt,),
    )
    target_task = TaskResult(
        TaskReference("target"),
        True,
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        (target_attempt,),
        SkipReason.RESOURCE_CIRCUIT_OPEN,
        (source_attempt.attempt,),
    )
    result = RunResult(
        "circuit-record",
        FlowStatus.FAILED,
        NOW,
        LATER,
        DURATION,
        (source_task, target_task),
    )
    path = tmp_path / "circuit-records.sqlite3"

    with SQLiteRunStore(path) as store:
        saved = store.save(result)
        assert saved.schema_version == 4
        assert store.load(result.run_id).record == result.to_record(schema_version=4)
        new_record = result.to_record(schema_version=3)

        for location, reason, status in (
            ("task", "resource_circuit_open", "success"),
            ("task", "mixed_blocked", "failed"),
            ("attempt", "resource_circuit_open", "success"),
        ):
            corrupt = json.loads(json.dumps(new_record))
            target_task = corrupt["tasks"][1]
            if location == "task":
                target_task["reason"] = reason
                target_task["status"] = status
            else:
                target_task["attempts"][0]["reason"] = reason
                target_task["attempts"][0]["status"] = status
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "UPDATE runs SET record_json = ? WHERE run_id = ?",
                    (
                        json.dumps(
                            corrupt,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                        result.run_id,
                    ),
                )
            with pytest.raises(
                ValueError, match="must have skipped status|must be skipped"
            ):
                store.load(result.run_id)
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "UPDATE runs SET record_json = ? WHERE run_id = ?",
                    (saved.record_json, result.run_id),
                )

        for version in (1, 2):
            historical = json.loads(json.dumps(new_record))
            if version == 1:
                for task in historical["tasks"]:
                    for attempt in task["attempts"]:
                        attempt.pop("invocations")
            record_json = json.dumps(
                historical, allow_nan=False, separators=(",", ":"), sort_keys=True
            )
            run_id = f"circuit-v{version}"
            historical["run_id"] = run_id
            record_json = json.dumps(
                historical, allow_nan=False, separators=(",", ":"), sort_keys=True
            )
            with sqlite3.connect(path) as connection:
                connection.execute(
                    """
                    INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        version,
                        result.status.value,
                        NOW.isoformat(),
                        record_json,
                    ),
                )
            with pytest.raises(ValueError, match="invalid .* reason"):
                store.load(run_id)


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
