from dataclasses import FrozenInstanceError, fields
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

    assert value.to_record() == {
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
    assert "partition_key" not in str(value.to_record())
    assert not hasattr(value, "output")
    assert not hasattr(value, "exception")


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
