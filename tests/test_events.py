from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timezone
from typing import Any

import pytest

from kazeflow.events import (
    EventKind,
    ExecutionEvent,
    ExecutionEventConsumer,
    validate_event_sequence,
)
from kazeflow.results import (
    AttemptReference,
    AttemptStatus,
    FailureInfo,
    FlowStatus,
    SkipReason,
    TaskReference,
)


NOW = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
TASK = TaskReference("extract")
ATTEMPT = AttemptReference(TASK, partition_key_present=True, partition_key=0)
BLOCKER = AttemptReference(TaskReference("upstream"))
FAILURE = FailureInfo("ValueError", "bad input", "traceback")


def event(sequence: int, kind: EventKind, **kwargs: Any) -> ExecutionEvent:
    return ExecutionEvent("run-1", sequence, NOW, kind, **kwargs)


def test_event_signature_defaults_and_immutability() -> None:
    assert [field.name for field in fields(ExecutionEvent)] == [
        "run_id",
        "sequence",
        "occurred_at",
        "kind",
        "task",
        "attempt",
        "status",
        "reason",
        "blocked_by",
        "failure",
        "invocation_number",
        "retry_delay_seconds",
        "cooldown_seconds",
        "resolution",
        "materialization_id",
    ]
    assert ExecutionEvent.__dataclass_fields__["task"].default is None
    assert ExecutionEvent.__dataclass_fields__["blocked_by"].default == ()

    value = event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING)
    with pytest.raises(FrozenInstanceError):
        setattr(value, "sequence", 2)


def test_event_kind_values_and_consumer_protocol_shape() -> None:
    assert {kind.value for kind in EventKind} == {
        "flow_started",
        "task_started",
        "attempt_started",
        "attempt_finished",
        "task_finished",
        "flow_finished",
        "invocation_started",
        "invocation_finished",
    }
    assert "on_event" in ExecutionEventConsumer.__dict__


@pytest.mark.parametrize(
    ("kind", "kwargs"),
    [
        (EventKind.FLOW_STARTED, {"status": FlowStatus.SUCCESS}),
        (EventKind.FLOW_FINISHED, {"status": FlowStatus.RUNNING}),
        (EventKind.TASK_STARTED, {"status": AttemptStatus.RUNNING}),
        (EventKind.TASK_FINISHED, {"task": TASK, "status": AttemptStatus.RUNNING}),
        (EventKind.ATTEMPT_STARTED, {"status": AttemptStatus.RUNNING}),
        (
            EventKind.ATTEMPT_FINISHED,
            {"attempt": ATTEMPT, "status": AttemptStatus.FAILED},
        ),
    ],
)
def test_event_kind_payload_rules_are_enforced(
    kind: EventKind, kwargs: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        event(1, kind, **kwargs)


def test_skipped_and_failed_event_payload_rules() -> None:
    blocked_task = event(
        1,
        EventKind.TASK_FINISHED,
        task=TASK,
        status=AttemptStatus.SKIPPED,
        reason=SkipReason.DEPENDENCY_BLOCKED,
        blocked_by=(BLOCKER,),
    )
    assert blocked_task.reason is SkipReason.DEPENDENCY_BLOCKED

    no_work_task = event(
        1,
        EventKind.TASK_FINISHED,
        task=TASK,
        status=AttemptStatus.SKIPPED,
        reason=SkipReason.NO_PARTITION_KEYS,
    )
    assert no_work_task.attempt is None

    failed = event(
        1,
        EventKind.ATTEMPT_FINISHED,
        attempt=ATTEMPT,
        status=AttemptStatus.FAILED,
        failure=FAILURE,
    )
    assert failed.failure == FAILURE

    with pytest.raises(ValueError):
        event(
            1,
            EventKind.ATTEMPT_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.NO_PARTITION_KEYS,
        )
    with pytest.raises(ValueError):
        event(
            1,
            EventKind.TASK_FINISHED,
            task=TASK,
            status=AttemptStatus.FAILED,
            failure=FAILURE,
        )


def test_event_rejects_invalid_identifiers_and_time() -> None:
    with pytest.raises(ValueError):
        ExecutionEvent("", 1, NOW, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING)
    with pytest.raises(ValueError):
        ExecutionEvent(
            "run-1", 0, NOW, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING
        )
    with pytest.raises(ValueError):
        ExecutionEvent(
            "run-1",
            1,
            datetime(2026, 7, 19, 12, 0),
            EventKind.FLOW_STARTED,
            status=FlowStatus.RUNNING,
        )


def test_event_record_is_fixed_and_lossy() -> None:
    value = event(
        4,
        EventKind.ATTEMPT_FINISHED,
        attempt=ATTEMPT,
        status=AttemptStatus.FAILED,
        failure=FAILURE,
    )

    legacy = {
        "run_id": "run-1",
        "sequence": 4,
        "occurred_at": "2026-07-19T12:00:00+00:00",
        "kind": "attempt_finished",
        "task": None,
        "attempt": {"task": {"task_name": "extract"}, "partition": {"present": True}},
        "status": "failed",
        "reason": None,
        "blocked_by": [],
        "failure": {
            "exception_type": "ValueError",
            "message": "bad input",
            "traceback": "traceback",
        },
    }
    assert value.to_record(schema_version=1) == legacy
    current = value.to_record()
    assert current == {
        **legacy,
        "schema_version": 4,
        "invocation_number": None,
        "retry_delay_seconds": None,
        "cooldown_seconds": None,
        "resolution": None,
        "materialization_id": None,
    }
    assert "partition_key" not in str(current)
    assert not hasattr(value, "output")
    assert not hasattr(value, "exception")


def test_invocation_event_payloads_and_versioned_records() -> None:
    started = event(
        1,
        EventKind.INVOCATION_STARTED,
        attempt=ATTEMPT,
        status=AttemptStatus.RUNNING,
        invocation_number=1,
    )
    finished = event(
        2,
        EventKind.INVOCATION_FINISHED,
        attempt=ATTEMPT,
        status=AttemptStatus.FAILED,
        invocation_number=1,
        failure=FAILURE,
        retry_delay_seconds=0.25,
        cooldown_seconds=1.5,
    )

    assert started.to_record()["invocation_number"] == 1
    assert finished.to_record()["schema_version"] == 4
    assert finished.to_record(schema_version=2)["schema_version"] == 2
    assert finished.to_record()["retry_delay_seconds"] == 0.25
    with pytest.raises(ValueError, match="cannot be projected"):
        started.to_record(schema_version=1)
    with pytest.raises(ValueError, match="positive integer"):
        event(
            1,
            EventKind.INVOCATION_STARTED,
            attempt=ATTEMPT,
            status=AttemptStatus.RUNNING,
            invocation_number=True,
        )
    with pytest.raises(ValueError, match="finite non-negative"):
        event(
            1,
            EventKind.INVOCATION_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.FAILED,
            invocation_number=1,
            failure=FAILURE,
            retry_delay_seconds=float("nan"),
        )


def test_invocation_event_stream_requires_contiguous_retry_causality() -> None:
    events = (
        event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
        event(2, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
        event(
            3, EventKind.ATTEMPT_STARTED, attempt=ATTEMPT, status=AttemptStatus.RUNNING
        ),
        event(
            4,
            EventKind.INVOCATION_STARTED,
            attempt=ATTEMPT,
            status=AttemptStatus.RUNNING,
            invocation_number=1,
        ),
        event(
            5,
            EventKind.INVOCATION_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.FAILED,
            invocation_number=1,
            failure=FAILURE,
            retry_delay_seconds=0,
        ),
        event(
            6,
            EventKind.INVOCATION_STARTED,
            attempt=ATTEMPT,
            status=AttemptStatus.RUNNING,
            invocation_number=2,
        ),
        event(
            7,
            EventKind.INVOCATION_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.SUCCESS,
            invocation_number=2,
        ),
        event(
            8, EventKind.ATTEMPT_FINISHED, attempt=ATTEMPT, status=AttemptStatus.SUCCESS
        ),
        event(9, EventKind.TASK_FINISHED, task=TASK, status=AttemptStatus.SUCCESS),
        event(10, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
    )
    validate_event_sequence(events)

    skipped_number = events[:5] + (
        replace(events[5], invocation_number=3),
        *events[6:],
    )
    with pytest.raises(ValueError, match="contiguous"):
        validate_event_sequence(skipped_number)


def test_logical_finish_must_match_invocation_and_consume_retry() -> None:
    def stream(
        invocation_status: AttemptStatus,
        logical_status: AttemptStatus,
        retry_delay_seconds: float | None = None,
    ) -> tuple[ExecutionEvent, ...]:
        failure = FAILURE if invocation_status is AttemptStatus.FAILED else None
        logical_failure = FAILURE if logical_status is AttemptStatus.FAILED else None
        flow_status = (
            FlowStatus.SUCCESS
            if logical_status is AttemptStatus.SUCCESS
            else FlowStatus.CANCELLED
            if logical_status is AttemptStatus.CANCELLED
            else FlowStatus.FAILED
        )
        return (
            event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
            event(2, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
            event(
                3,
                EventKind.ATTEMPT_STARTED,
                attempt=ATTEMPT,
                status=AttemptStatus.RUNNING,
            ),
            event(
                4,
                EventKind.INVOCATION_STARTED,
                attempt=ATTEMPT,
                status=AttemptStatus.RUNNING,
                invocation_number=1,
            ),
            event(
                5,
                EventKind.INVOCATION_FINISHED,
                attempt=ATTEMPT,
                status=invocation_status,
                invocation_number=1,
                failure=failure,
                retry_delay_seconds=retry_delay_seconds,
            ),
            event(
                6,
                EventKind.ATTEMPT_FINISHED,
                attempt=ATTEMPT,
                status=logical_status,
                failure=logical_failure,
            ),
            event(7, EventKind.TASK_FINISHED, task=TASK, status=logical_status),
            event(8, EventKind.FLOW_FINISHED, status=flow_status),
        )

    with pytest.raises(ValueError, match="last invocation status"):
        validate_event_sequence(stream(AttemptStatus.FAILED, AttemptStatus.SUCCESS))
    with pytest.raises(ValueError, match="last invocation status"):
        validate_event_sequence(stream(AttemptStatus.SUCCESS, AttemptStatus.FAILED))
    with pytest.raises(ValueError, match="scheduled retry"):
        validate_event_sequence(stream(AttemptStatus.FAILED, AttemptStatus.FAILED, 0.2))

    # Cancellation while waiting may preserve a scheduled retry without inventing it.
    validate_event_sequence(stream(AttemptStatus.FAILED, AttemptStatus.CANCELLED, 0.2))


def test_validate_event_sequence_accepts_complete_causal_stream() -> None:
    events = (
        event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
        event(2, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
        event(
            3, EventKind.ATTEMPT_STARTED, attempt=ATTEMPT, status=AttemptStatus.RUNNING
        ),
        event(
            4,
            EventKind.ATTEMPT_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.SUCCESS,
        ),
        event(5, EventKind.TASK_FINISHED, task=TASK, status=AttemptStatus.SUCCESS),
        event(6, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
    )

    validate_event_sequence(events)


def test_validate_event_sequence_rejects_attempt_lifecycle_before_task_start() -> None:
    events = (
        event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
        event(
            2, EventKind.ATTEMPT_STARTED, attempt=ATTEMPT, status=AttemptStatus.RUNNING
        ),
        event(
            3,
            EventKind.ATTEMPT_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.SUCCESS,
        ),
        event(4, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
        event(5, EventKind.TASK_FINISHED, task=TASK, status=AttemptStatus.SUCCESS),
        event(6, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
    )

    with pytest.raises(ValueError, match="before its task started"):
        validate_event_sequence(events)


def test_blocked_attempt_may_finish_without_start_after_parent_task_starts() -> None:
    blocked_attempt = AttemptReference(
        TASK, partition_key_present=True, partition_key=1
    )
    events = (
        event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
        event(2, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
        event(
            3,
            EventKind.ATTEMPT_FINISHED,
            attempt=blocked_attempt,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.DEPENDENCY_BLOCKED,
            blocked_by=(BLOCKER,),
        ),
        event(
            4,
            EventKind.TASK_FINISHED,
            task=TASK,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.DEPENDENCY_BLOCKED,
            blocked_by=(BLOCKER,),
        ),
        event(5, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
    )

    validate_event_sequence(events)


@pytest.mark.parametrize(
    "events",
    [
        (),
        (
            event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
            event(3, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
        ),
        (
            event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
            event(2, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
            event(3, EventKind.TASK_FINISHED, task=TASK, status=AttemptStatus.SUCCESS),
            event(
                4,
                EventKind.ATTEMPT_FINISHED,
                attempt=ATTEMPT,
                status=AttemptStatus.SUCCESS,
            ),
            event(5, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
        ),
    ],
)
def test_validate_event_sequence_rejects_invalid_streams(
    events: tuple[ExecutionEvent, ...],
) -> None:
    with pytest.raises(ValueError):
        validate_event_sequence(events)


def test_validate_event_sequence_allows_no_work_and_blocked_tasks_without_starts() -> (
    None
):
    no_work = TaskReference("no-work")
    blocked = TaskReference("blocked")
    events = (
        event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
        event(
            2,
            EventKind.TASK_FINISHED,
            task=no_work,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.NO_PARTITION_KEYS,
        ),
        event(
            3,
            EventKind.TASK_FINISHED,
            task=blocked,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.DEPENDENCY_BLOCKED,
            blocked_by=(BLOCKER,),
        ),
        event(4, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
    )

    validate_event_sequence(events)


def test_circuit_skip_requires_an_earlier_failed_source_and_allows_retry_history() -> (
    None
):
    source_task = BLOCKER.task
    events = (
        event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
        event(
            2, EventKind.TASK_STARTED, task=source_task, status=AttemptStatus.RUNNING
        ),
        event(
            3, EventKind.ATTEMPT_STARTED, attempt=BLOCKER, status=AttemptStatus.RUNNING
        ),
        event(
            4,
            EventKind.ATTEMPT_FINISHED,
            attempt=BLOCKER,
            status=AttemptStatus.FAILED,
            failure=FAILURE,
        ),
        event(
            5, EventKind.TASK_FINISHED, task=source_task, status=AttemptStatus.FAILED
        ),
        event(6, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
        event(
            7, EventKind.ATTEMPT_STARTED, attempt=ATTEMPT, status=AttemptStatus.RUNNING
        ),
        event(
            8,
            EventKind.INVOCATION_STARTED,
            attempt=ATTEMPT,
            status=AttemptStatus.RUNNING,
            invocation_number=1,
        ),
        event(
            9,
            EventKind.INVOCATION_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.FAILED,
            invocation_number=1,
            failure=FAILURE,
            retry_delay_seconds=2.0,
        ),
        event(
            10,
            EventKind.ATTEMPT_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(BLOCKER,),
        ),
        event(
            11,
            EventKind.TASK_FINISHED,
            task=TASK,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(BLOCKER,),
        ),
        event(12, EventKind.FLOW_FINISHED, status=FlowStatus.FAILED),
    )
    validate_event_sequence(events)
    assert events[9].to_record()["schema_version"] == 4
    for version in (1, 2):
        with pytest.raises(ValueError, match="cannot be projected"):
            events[9].to_record(schema_version=version)


def test_circuit_skip_without_source_finish_is_rejected() -> None:
    events = (
        event(1, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING),
        event(2, EventKind.TASK_STARTED, task=TASK, status=AttemptStatus.RUNNING),
        event(
            3,
            EventKind.ATTEMPT_FINISHED,
            attempt=ATTEMPT,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(BLOCKER,),
        ),
        event(
            4,
            EventKind.TASK_FINISHED,
            task=TASK,
            status=AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(BLOCKER,),
        ),
        event(5, EventKind.FLOW_FINISHED, status=FlowStatus.SUCCESS),
    )
    with pytest.raises(ValueError, match="earlier failed"):
        validate_event_sequence(events)
