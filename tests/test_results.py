"""Tests for immutable, portable execution result values."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timedelta, timezone
import inspect
import json
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


NOW = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
LATER = NOW + timedelta(seconds=2)
DURATION = timedelta(seconds=1)


def successful_attempt(
    task_name: str = "task", partition_key: object | None = None
) -> AttemptResult:
    present = partition_key is not None
    return AttemptResult(
        attempt=AttemptReference(
            TaskReference(task_name),
            partition_key_present=present,
            partition_key=partition_key,
        ),
        status=AttemptStatus.SUCCESS,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
    )


def blocked_attempt(
    task_name: str = "task", partition_key: object = 0
) -> AttemptResult:
    blocker = AttemptReference(TaskReference("upstream"))
    return AttemptResult(
        attempt=AttemptReference(
            TaskReference(task_name),
            partition_key_present=True,
            partition_key=partition_key,
        ),
        status=AttemptStatus.SKIPPED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        reason=SkipReason.DEPENDENCY_BLOCKED,
        blocked_by=(blocker,),
    )


def test_exact_enums_fields_and_defaults() -> None:
    assert {status.name: status.value for status in FlowStatus} == {
        "PENDING": "pending",
        "RUNNING": "running",
        "SUCCESS": "success",
        "FAILED": "failed",
        "CANCELLED": "cancelled",
    }
    assert {status.name: status.value for status in AttemptStatus} == {
        "PENDING": "pending",
        "RUNNING": "running",
        "SUCCESS": "success",
        "FAILED": "failed",
        "SKIPPED": "skipped",
        "CANCELLED": "cancelled",
    }
    assert {reason.name: reason.value for reason in SkipReason} == {
        "DEPENDENCY_BLOCKED": "dependency_blocked",
        "NO_PARTITION_KEYS": "no_partition_keys",
    }
    assert [field.name for field in fields(AttemptReference)] == [
        "task",
        "partition_key_present",
        "partition_key",
    ]
    assert [field.name for field in fields(AttemptResult)] == [
        "attempt",
        "status",
        "started_at",
        "ended_at",
        "duration",
        "output",
        "exception",
        "failure",
        "reason",
        "blocked_by",
    ]
    assert [field.name for field in fields(TaskResult)] == [
        "task",
        "is_partitioned",
        "status",
        "started_at",
        "ended_at",
        "duration",
        "attempts",
        "reason",
        "blocked_by",
    ]
    assert [field.name for field in fields(RunResult)] == [
        "run_id",
        "status",
        "started_at",
        "ended_at",
        "duration",
        "tasks",
    ]
    assert (
        inspect.signature(AttemptReference).parameters["partition_key_present"].default
        is False
    )
    assert (
        inspect.signature(AttemptReference).parameters["partition_key"].default is None
    )
    assert inspect.signature(AttemptResult).parameters["blocked_by"].default == ()
    assert inspect.signature(TaskResult).parameters["attempts"].default == ()
    assert inspect.signature(RunResult).parameters["tasks"].default == ()


@pytest.mark.parametrize("key", [0, "", False])
def test_falsey_partition_keys_remain_present(key: object) -> None:
    reference = AttemptReference(
        TaskReference("partitioned"), partition_key_present=True, partition_key=key
    )

    assert reference.partition_key_present is True
    assert reference.partition_key is key
    assert reference.to_record() == {
        "task": {"task_name": "partitioned"},
        "partition": {"present": True},
    }


@pytest.mark.parametrize(
    "present,key",
    [(False, 0), (True, None)],
)
def test_invalid_partition_references_are_rejected(
    present: bool, key: object | None
) -> None:
    with pytest.raises(ValueError):
        AttemptReference(TaskReference("task"), present, key)


def test_envelopes_are_frozen_and_collections_must_be_tuples() -> None:
    reference = TaskReference("task")
    with pytest.raises(FrozenInstanceError):
        setattr(reference, "task_name", "other")

    attempt = successful_attempt()
    with pytest.raises(ValueError, match="attempts"):
        TaskResult(
            task=reference,
            is_partitioned=False,
            status=AttemptStatus.SUCCESS,
            started_at=NOW,
            ended_at=LATER,
            duration=DURATION,
            attempts=cast(tuple[AttemptResult, ...], [attempt]),
        )


@pytest.mark.parametrize(
    "started,ended,duration,status",
    [
        (NOW.replace(tzinfo=None), LATER, DURATION, AttemptStatus.SUCCESS),
        (
            NOW,
            LATER.astimezone(timezone(timedelta(hours=1))),
            DURATION,
            AttemptStatus.SUCCESS,
        ),
        (LATER, NOW, DURATION, AttemptStatus.SUCCESS),
        (NOW, LATER, timedelta(seconds=-1), AttemptStatus.SUCCESS),
        (NOW, LATER, DURATION, AttemptStatus.RUNNING),
    ],
)
def test_attempt_result_rejects_malformed_completed_snapshot(
    started: datetime, ended: datetime, duration: timedelta, status: AttemptStatus
) -> None:
    with pytest.raises(ValueError):
        AttemptResult(
            attempt=AttemptReference(TaskReference("task")),
            status=status,
            started_at=started,
            ended_at=ended,
            duration=duration,
        )


def test_failure_and_skip_invariants() -> None:
    failure = FailureInfo("ValueError", "bad", "trace")
    error = ValueError("bad")
    failed = AttemptResult(
        attempt=AttemptReference(TaskReference("task")),
        status=AttemptStatus.FAILED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        exception=error,
        failure=failure,
    )
    assert failed.exception is error
    assert failed.failure is failure

    with pytest.raises(ValueError, match="strings"):
        FailureInfo("ValueError", "bad", cast(str, object()))

    with pytest.raises(ValueError, match="requires FailureInfo"):
        AttemptResult(
            AttemptReference(TaskReference("task")),
            AttemptStatus.FAILED,
            NOW,
            LATER,
            DURATION,
        )
    with pytest.raises(ValueError, match="only a failed"):
        AttemptResult(
            AttemptReference(TaskReference("task")),
            AttemptStatus.SUCCESS,
            NOW,
            LATER,
            DURATION,
            exception=error,
        )
    with pytest.raises(ValueError, match="dependency_blocked"):
        AttemptResult(
            AttemptReference(TaskReference("task")),
            AttemptStatus.SKIPPED,
            NOW,
            LATER,
            DURATION,
            reason=SkipReason.NO_PARTITION_KEYS,
        )


def test_unpartitioned_aggregate_must_equal_its_attempt() -> None:
    attempt = successful_attempt()
    result = TaskResult(
        task=TaskReference("task"),
        is_partitioned=False,
        status=AttemptStatus.SUCCESS,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        attempts=(attempt,),
    )
    assert result.attempts == (attempt,)

    with pytest.raises(ValueError, match="aggregate"):
        TaskResult(
            task=TaskReference("task"),
            is_partitioned=False,
            status=AttemptStatus.CANCELLED,
            started_at=NOW,
            ended_at=LATER,
            duration=DURATION,
            attempts=(attempt,),
        )


def test_partitioned_aggregate_preserves_order_and_mixed_skip_is_unattributed() -> None:
    first = successful_attempt("partitioned", 0)
    second = blocked_attempt("partitioned", "")
    third = successful_attempt("partitioned", 1)
    result = TaskResult(
        task=TaskReference("partitioned"),
        is_partitioned=True,
        status=AttemptStatus.SKIPPED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        attempts=(first, second, third),
    )

    assert tuple(attempt.attempt.partition_key for attempt in result.attempts) == (
        0,
        "",
        1,
    )
    assert result.reason is None
    assert result.blocked_by == ()
    attempt_records = cast(list[dict[str, object]], result.to_record()["attempts"])
    assert [
        cast(dict[str, object], attempt["attempt"])["partition"]
        for attempt in attempt_records
    ] == [
        {"present": True},
        {"present": True},
        {"present": True},
    ]


def test_no_work_partitioned_task_and_dependency_blocked_aggregate() -> None:
    task = TaskReference("partitioned")
    no_work = TaskResult(
        task=task,
        is_partitioned=True,
        status=AttemptStatus.SKIPPED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        reason=SkipReason.NO_PARTITION_KEYS,
    )
    assert no_work.attempts == ()

    first = blocked_attempt("partitioned", 0)
    second = blocked_attempt("partitioned", 1)
    blocked = TaskResult(
        task=task,
        is_partitioned=True,
        status=AttemptStatus.SKIPPED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        attempts=(first, second),
        reason=SkipReason.DEPENDENCY_BLOCKED,
        blocked_by=first.blocked_by + second.blocked_by,
    )
    assert blocked.blocked_by == first.blocked_by + second.blocked_by

    with pytest.raises(ValueError, match="zero-attempt"):
        TaskResult(
            task=task,
            is_partitioned=True,
            status=AttemptStatus.SUCCESS,
            started_at=NOW,
            ended_at=LATER,
            duration=DURATION,
        )


def test_run_aggregate_is_validated_and_task_names_are_unique() -> None:
    success = TaskResult(
        task=TaskReference("success"),
        is_partitioned=False,
        status=AttemptStatus.SUCCESS,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        attempts=(successful_attempt("success"),),
    )
    result = RunResult("run", FlowStatus.SUCCESS, NOW, LATER, DURATION, (success,))
    assert result.tasks == (success,)

    with pytest.raises(ValueError, match="status"):
        RunResult("run", FlowStatus.FAILED, NOW, LATER, DURATION, (success,))
    with pytest.raises(ValueError, match="unique"):
        RunResult("run", FlowStatus.SUCCESS, NOW, LATER, DURATION, (success, success))


def test_record_schemas_are_lossy_new_and_json_compatible() -> None:
    raw_output = object()
    failure = FailureInfo("ValueError", "bad", "trace")
    failed = AttemptResult(
        attempt=AttemptReference(TaskReference("failed")),
        status=AttemptStatus.FAILED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        output=raw_output,
        exception=ValueError("bad"),
        failure=failure,
    )
    task = TaskResult(
        task=TaskReference("failed"),
        is_partitioned=False,
        status=AttemptStatus.FAILED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        attempts=(failed,),
    )
    record = RunResult(
        "run", FlowStatus.FAILED, NOW, LATER, DURATION, (task,)
    ).to_record()

    assert set(record) == {
        "run_id",
        "status",
        "started_at",
        "ended_at",
        "duration_seconds",
        "tasks",
    }
    task_records = cast(list[dict[str, object]], record["tasks"])
    failed_attempt_records = cast(list[dict[str, object]], task_records[0]["attempts"])
    attempt_record = failed_attempt_records[0]
    assert set(attempt_record) == {
        "attempt",
        "status",
        "started_at",
        "ended_at",
        "duration_seconds",
        "reason",
        "blocked_by",
        "failure",
    }
    assert "output" not in attempt_record
    assert "exception" not in attempt_record
    assert "partition_key" not in cast(dict[str, object], attempt_record["attempt"])
    assert attempt_record["failure"] == failure.to_record()
    json.dumps(record)

    task_records.clear()
    assert (
        len(
            cast(
                list[dict[str, object]],
                RunResult(
                    "run", FlowStatus.FAILED, NOW, LATER, DURATION, (task,)
                ).to_record()["tasks"],
            )
        )
        == 1
    )


def test_records_preserve_task_order_and_include_optional_nulls() -> None:
    first = TaskResult(
        task=TaskReference("first"),
        is_partitioned=False,
        status=AttemptStatus.SUCCESS,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        attempts=(successful_attempt("first"),),
    )
    second = TaskResult(
        task=TaskReference("second"),
        is_partitioned=True,
        status=AttemptStatus.SKIPPED,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        reason=SkipReason.NO_PARTITION_KEYS,
    )
    record = RunResult(
        "run", FlowStatus.SUCCESS, NOW, LATER, DURATION, (first, second)
    ).to_record()

    task_records = cast(list[dict[str, object]], record["tasks"])
    assert [
        cast(dict[str, object], item["task"])["task_name"] for item in task_records
    ] == [
        "first",
        "second",
    ]
    assert first.to_record()["reason"] is None
    assert first.to_record()["blocked_by"] == []
    assert second.to_record()["attempts"] == []
