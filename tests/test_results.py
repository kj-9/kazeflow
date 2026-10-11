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
    InvocationResult,
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
        "RESOURCE_CIRCUIT_OPEN": "resource_circuit_open",
        "MIXED_BLOCKED": "mixed_blocked",
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
        "resolution",
        "materialization_id",
        "invocations",
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
    assert inspect.signature(AttemptResult).parameters["invocations"].default == ()


def test_invocation_history_is_validated_and_projected_only_in_version_two() -> None:
    failed = InvocationResult(
        1,
        AttemptStatus.FAILED,
        NOW,
        NOW + timedelta(seconds=1),
        timedelta(seconds=1),
        FailureInfo("ValueError", "temporary", "trace"),
        retry_delay_seconds=0.25,
        cooldown_seconds=1.5,
    )
    succeeded = InvocationResult(
        2,
        AttemptStatus.SUCCESS,
        NOW + timedelta(seconds=1),
        LATER,
        timedelta(seconds=1),
    )
    attempt = AttemptResult(
        AttemptReference(TaskReference("task")),
        AttemptStatus.SUCCESS,
        NOW,
        LATER,
        DURATION,
        output=object(),
        invocations=(failed, succeeded),
    )
    task = TaskResult(
        TaskReference("task"),
        False,
        AttemptStatus.SUCCESS,
        NOW,
        LATER,
        DURATION,
        (attempt,),
    )
    result = RunResult("retry-run", FlowStatus.SUCCESS, NOW, LATER, DURATION, (task,))

    assert "invocations" not in attempt.to_record()
    attempt_v2 = cast(list[object], result.to_record(schema_version=2)["tasks"])
    projected_task = cast(dict[str, object], attempt_v2[0])
    projected_attempts = cast(list[object], projected_task["attempts"])
    projected_attempt = cast(dict[str, object], projected_attempts[0])
    assert projected_attempt["invocations"] == [
        failed.to_record(),
        succeeded.to_record(),
    ]
    assert result.to_record(schema_version=3) == result.to_record(schema_version=2)
    assert "output" not in json.dumps(result.to_record(schema_version=2))


@pytest.mark.parametrize("schema_version", [0, 5, True, "2"])
def test_result_projection_rejects_unknown_or_boolean_versions(schema_version) -> None:
    with pytest.raises(ValueError, match="schema_version"):
        successful_attempt().to_record(schema_version=schema_version)


@pytest.mark.parametrize(
    "seconds",
    [-1, float("nan"), float("inf"), True, "1"],
)
def test_invocation_rejects_invalid_retry_seconds(seconds) -> None:
    with pytest.raises(ValueError):
        InvocationResult(
            1,
            AttemptStatus.FAILED,
            NOW,
            LATER,
            DURATION,
            FailureInfo("ValueError", "failed", "trace"),
            retry_delay_seconds=seconds,
        )


def test_attempt_rejects_noncontiguous_or_nonretryable_invocation_history() -> None:
    success = InvocationResult(1, AttemptStatus.SUCCESS, NOW, LATER, DURATION)
    failed_without_delay = InvocationResult(
        1,
        AttemptStatus.FAILED,
        NOW,
        LATER,
        DURATION,
        FailureInfo("ValueError", "failed", "trace"),
    )
    with pytest.raises(ValueError, match="contiguous"):
        AttemptResult(
            AttemptReference(TaskReference("task")),
            AttemptStatus.SUCCESS,
            NOW,
            LATER,
            DURATION,
            invocations=(
                InvocationResult(2, AttemptStatus.SUCCESS, NOW, LATER, DURATION),
            ),
        )
    with pytest.raises(ValueError, match="only failed"):
        AttemptResult(
            AttemptReference(TaskReference("task")),
            AttemptStatus.SUCCESS,
            NOW,
            LATER,
            DURATION,
            invocations=(
                success,
                InvocationResult(2, AttemptStatus.SUCCESS, NOW, LATER, DURATION),
            ),
        )
    with pytest.raises(ValueError, match="retry delay"):
        AttemptResult(
            AttemptReference(TaskReference("task")),
            AttemptStatus.SUCCESS,
            NOW,
            LATER,
            DURATION,
            invocations=(
                failed_without_delay,
                InvocationResult(2, AttemptStatus.SUCCESS, NOW, LATER, DURATION),
            ),
        )


@pytest.mark.parametrize(
    ("status", "invocation", "failure"),
    [
        (
            AttemptStatus.SUCCESS,
            InvocationResult(
                1,
                AttemptStatus.FAILED,
                NOW,
                LATER,
                DURATION,
                FailureInfo("ValueError", "failed", "trace"),
            ),
            None,
        ),
        (
            AttemptStatus.FAILED,
            InvocationResult(1, AttemptStatus.SUCCESS, NOW, LATER, DURATION),
            FailureInfo("ValueError", "failed", "trace"),
        ),
        (
            AttemptStatus.FAILED,
            InvocationResult(
                1,
                AttemptStatus.FAILED,
                NOW,
                LATER,
                DURATION,
                FailureInfo("ValueError", "failed", "trace"),
                retry_delay_seconds=0.2,
            ),
            FailureInfo("ValueError", "failed", "trace"),
        ),
    ],
    ids=["success-after-failed-invocation", "failed-after-success", "unconsumed-retry"],
)
def test_terminal_attempt_must_match_final_invocation(
    status: AttemptStatus,
    invocation: InvocationResult,
    failure: FailureInfo | None,
) -> None:
    with pytest.raises(ValueError, match="last invocation|scheduled retry"):
        AttemptResult(
            AttemptReference(TaskReference("task")),
            status,
            NOW,
            LATER,
            DURATION,
            failure=failure,
            invocations=(invocation,),
        )


def test_cancelled_attempt_may_end_while_retry_is_scheduled() -> None:
    failed = InvocationResult(
        1,
        AttemptStatus.FAILED,
        NOW,
        LATER,
        DURATION,
        FailureInfo("ValueError", "failed", "trace"),
        retry_delay_seconds=0.2,
    )

    result = AttemptResult(
        AttemptReference(TaskReference("task")),
        AttemptStatus.CANCELLED,
        NOW,
        LATER,
        DURATION,
        invocations=(failed,),
    )
    assert result.status is AttemptStatus.CANCELLED


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


def test_executed_materialized_attempt_can_carry_committed_id() -> None:
    result = AttemptResult(
        attempt=AttemptReference(
            TaskReference("asset"), partition_key_present=True, partition_key=0
        ),
        status=AttemptStatus.SUCCESS,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        resolution="executed",
        materialization_id="committed-1",
    )

    assert result.resolution == "executed"
    assert result.materialization_id == "committed-1"
    assert "materialization_id" not in result.to_record()


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


def test_circuit_and_mixed_blocked_partition_aggregates_are_attributed() -> None:
    task = TaskReference("partitioned")
    source = AttemptReference(TaskReference("source"))
    dependency = AttemptReference(TaskReference("dependency"))
    circuit_attempt = AttemptResult(
        AttemptReference(task, True, ""),
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
        blocked_by=(source,),
    )
    dependency_attempt = AttemptResult(
        AttemptReference(task, True, 0),
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        reason=SkipReason.DEPENDENCY_BLOCKED,
        blocked_by=(dependency,),
    )
    circuit_only = TaskResult(
        task,
        True,
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        (circuit_attempt,),
        SkipReason.RESOURCE_CIRCUIT_OPEN,
        (source,),
    )
    mixed = TaskResult(
        task,
        True,
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        (circuit_attempt, dependency_attempt),
        SkipReason.MIXED_BLOCKED,
        (source, dependency),
    )
    assert circuit_only.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
    assert mixed.blocked_by == (source, dependency)
    assert mixed.to_record(schema_version=3)["reason"] == "mixed_blocked"
    with pytest.raises(ValueError, match="schema version 2"):
        mixed.to_record(schema_version=2)

    mixed_outcome = TaskResult(
        task,
        True,
        AttemptStatus.SKIPPED,
        NOW,
        LATER,
        DURATION,
        (successful_attempt("partitioned", 2), circuit_attempt),
    )
    assert mixed_outcome.reason is None
    for version in (1, 2):
        with pytest.raises(ValueError, match="schema version"):
            mixed_outcome.to_record(schema_version=version)
    with pytest.raises(ValueError, match="one blocker"):
        AttemptResult(
            AttemptReference(task, True, 1),
            AttemptStatus.SKIPPED,
            NOW,
            LATER,
            DURATION,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(source, dependency),
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
