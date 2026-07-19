"""Neutral, passive execution lifecycle event values."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, Sequence

from .results import (
    AttemptReference,
    AttemptStatus,
    FailureInfo,
    FlowStatus,
    SkipReason,
    TaskReference,
)


class EventKind(str, Enum):
    """The fixed lifecycle event kinds emitted for one flow execution."""

    FLOW_STARTED = "flow_started"
    TASK_STARTED = "task_started"
    ATTEMPT_STARTED = "attempt_started"
    ATTEMPT_FINISHED = "attempt_finished"
    TASK_FINISHED = "task_finished"
    FLOW_FINISHED = "flow_finished"


_TERMINAL_FLOW_STATUSES = {
    FlowStatus.SUCCESS,
    FlowStatus.FAILED,
    FlowStatus.CANCELLED,
}
_TERMINAL_ATTEMPT_STATUSES = {
    AttemptStatus.SUCCESS,
    AttemptStatus.FAILED,
    AttemptStatus.SKIPPED,
    AttemptStatus.CANCELLED,
}


def _is_aware_utc(value: datetime) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
    )


def _validate_skipped_payload(
    *,
    kind: EventKind,
    status: AttemptStatus,
    reason: SkipReason | None,
    blocked_by: tuple[AttemptReference, ...],
) -> None:
    if status is not AttemptStatus.SKIPPED:
        if reason is not None or blocked_by:
            raise ValueError("reason and blocked_by are only valid for skipped events")
        return

    if reason is None:
        raise ValueError("skipped events require a reason")
    if reason is SkipReason.DEPENDENCY_BLOCKED:
        if not blocked_by:
            raise ValueError("dependency-blocked events require blockers")
        return
    if reason is SkipReason.NO_PARTITION_KEYS:
        if kind is not EventKind.TASK_FINISHED or blocked_by:
            raise ValueError(
                "no_partition_keys is valid only for task-finished events without blockers"
            )
        return
    raise ValueError("unknown skip reason")


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """One validated, serializable lifecycle observation for a flow run."""

    run_id: str
    sequence: int
    occurred_at: datetime
    kind: EventKind
    task: TaskReference | None = None
    attempt: AttemptReference | None = None
    status: FlowStatus | AttemptStatus | None = None
    reason: SkipReason | None = None
    blocked_by: tuple[AttemptReference, ...] = ()
    failure: FailureInfo | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("run_id must be a non-empty string")
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 1
        ):
            raise ValueError("sequence must be a positive integer")
        if not _is_aware_utc(self.occurred_at):
            raise ValueError("occurred_at must be an aware UTC datetime")
        if not isinstance(self.kind, EventKind):
            raise ValueError("kind must be an EventKind")
        if not isinstance(self.blocked_by, tuple) or not all(
            isinstance(blocker, AttemptReference) for blocker in self.blocked_by
        ):
            raise ValueError("blocked_by must be a tuple of AttemptReference values")
        if self.reason is not None and not isinstance(self.reason, SkipReason):
            raise ValueError("reason must be a SkipReason or None")
        if self.failure is not None and not isinstance(self.failure, FailureInfo):
            raise ValueError("failure must be a FailureInfo or None")

        if self.kind is EventKind.FLOW_STARTED:
            self._validate_flow_started()
        elif self.kind is EventKind.FLOW_FINISHED:
            self._validate_flow_finished()
        elif self.kind is EventKind.TASK_STARTED:
            self._validate_task_started()
        elif self.kind is EventKind.TASK_FINISHED:
            self._validate_task_finished()
        elif self.kind is EventKind.ATTEMPT_STARTED:
            self._validate_attempt_started()
        else:
            self._validate_attempt_finished()

    def _validate_flow_started(self) -> None:
        if self.task is not None or self.attempt is not None:
            raise ValueError("flow events cannot carry task or attempt identities")
        if self.status is not FlowStatus.RUNNING:
            raise ValueError("flow_started requires FlowStatus.RUNNING")
        self._validate_no_skip_or_failure()

    def _validate_flow_finished(self) -> None:
        if self.task is not None or self.attempt is not None:
            raise ValueError("flow events cannot carry task or attempt identities")
        if (
            not isinstance(self.status, FlowStatus)
            or self.status not in _TERMINAL_FLOW_STATUSES
        ):
            raise ValueError("flow_finished requires a terminal FlowStatus")
        self._validate_no_skip_or_failure()

    def _validate_task_started(self) -> None:
        if not isinstance(self.task, TaskReference) or self.attempt is not None:
            raise ValueError("task_started requires task and forbids attempt")
        if self.status is not AttemptStatus.RUNNING:
            raise ValueError("task_started requires AttemptStatus.RUNNING")
        self._validate_no_skip_or_failure()

    def _validate_task_finished(self) -> None:
        if not isinstance(self.task, TaskReference) or self.attempt is not None:
            raise ValueError("task_finished requires task and forbids attempt")
        if (
            not isinstance(self.status, AttemptStatus)
            or self.status not in _TERMINAL_ATTEMPT_STATUSES
        ):
            raise ValueError("task_finished requires a terminal AttemptStatus")
        _validate_skipped_payload(
            kind=self.kind,
            status=self.status,
            reason=self.reason,
            blocked_by=self.blocked_by,
        )
        if self.failure is not None:
            raise ValueError("only failed attempt-finished events may carry failure")

    def _validate_attempt_started(self) -> None:
        if self.task is not None or not isinstance(self.attempt, AttemptReference):
            raise ValueError("attempt_started requires attempt and forbids task")
        if self.status is not AttemptStatus.RUNNING:
            raise ValueError("attempt_started requires AttemptStatus.RUNNING")
        self._validate_no_skip_or_failure()

    def _validate_attempt_finished(self) -> None:
        if self.task is not None or not isinstance(self.attempt, AttemptReference):
            raise ValueError("attempt_finished requires attempt and forbids task")
        if (
            not isinstance(self.status, AttemptStatus)
            or self.status not in _TERMINAL_ATTEMPT_STATUSES
        ):
            raise ValueError("attempt_finished requires a terminal AttemptStatus")
        _validate_skipped_payload(
            kind=self.kind,
            status=self.status,
            reason=self.reason,
            blocked_by=self.blocked_by,
        )
        if self.status is AttemptStatus.FAILED:
            if self.failure is None:
                raise ValueError("failed attempt-finished events require failure")
        elif self.failure is not None:
            raise ValueError("only failed attempt-finished events may carry failure")

    def _validate_no_skip_or_failure(self) -> None:
        if self.reason is not None or self.blocked_by:
            raise ValueError("reason and blocked_by are only valid for skipped events")
        if self.failure is not None:
            raise ValueError("only failed attempt-finished events may carry failure")

    def to_record(self) -> dict[str, object]:
        """Return the fixed, intentionally lossy event record projection."""
        return {
            "run_id": self.run_id,
            "sequence": self.sequence,
            "occurred_at": self.occurred_at.isoformat(),
            "kind": self.kind.value,
            "task": self.task.to_record() if self.task is not None else None,
            "attempt": self.attempt.to_record() if self.attempt is not None else None,
            "status": self.status.value if self.status is not None else None,
            "reason": self.reason.value if self.reason is not None else None,
            "blocked_by": [blocker.to_record() for blocker in self.blocked_by],
            "failure": self.failure.to_record() if self.failure is not None else None,
        }


class ExecutionEventConsumer(Protocol):
    """A passive observer interface; dispatch policy belongs to later integration."""

    def on_event(self, event: ExecutionEvent) -> None: ...


@dataclass(slots=True)
class _LifecycleState:
    started: bool = False
    finished: bool = False
    finish_status: AttemptStatus | None = None
    finish_reason: SkipReason | None = None


def _attempt_state(
    states: list[tuple[AttemptReference, _LifecycleState]],
    attempt: AttemptReference,
) -> _LifecycleState:
    for known_attempt, state in states:
        if known_attempt == attempt:
            return state
    state = _LifecycleState()
    states.append((attempt, state))
    return state


def _finish_without_start_is_allowed(state: _LifecycleState) -> bool:
    return state.finish_status is AttemptStatus.SKIPPED and state.finish_reason in {
        SkipReason.DEPENDENCY_BLOCKED,
        SkipReason.NO_PARTITION_KEYS,
    }


def validate_event_sequence(events: Sequence[ExecutionEvent]) -> None:
    """Validate the complete causal lifecycle stream for one execution."""
    if not events:
        raise ValueError("event stream must be non-empty")
    if not all(isinstance(event, ExecutionEvent) for event in events):
        raise ValueError("event stream must contain ExecutionEvent values")

    run_id = events[0].run_id
    for expected_sequence, event in enumerate(events, start=1):
        if event.run_id != run_id:
            raise ValueError("event stream must contain exactly one run_id")
        if event.sequence != expected_sequence:
            raise ValueError("event sequences must begin at 1 and increment by one")

    if events[0].kind is not EventKind.FLOW_STARTED:
        raise ValueError("event stream must start with flow_started")
    if events[-1].kind is not EventKind.FLOW_FINISHED:
        raise ValueError("event stream must end with flow_finished")
    if sum(event.kind is EventKind.FLOW_STARTED for event in events) != 1:
        raise ValueError("event stream must contain exactly one flow_started")
    if sum(event.kind is EventKind.FLOW_FINISHED for event in events) != 1:
        raise ValueError("event stream must contain exactly one flow_finished")

    task_states: dict[TaskReference, _LifecycleState] = {}
    attempt_states: list[tuple[AttemptReference, _LifecycleState]] = []

    for event in events:
        if event.kind is EventKind.TASK_STARTED:
            assert event.task is not None
            state = task_states.setdefault(event.task, _LifecycleState())
            if state.started or state.finished:
                raise ValueError("task lifecycle has an invalid start")
            state.started = True
        elif event.kind is EventKind.TASK_FINISHED:
            assert event.task is not None
            state = task_states.setdefault(event.task, _LifecycleState())
            if state.finished:
                raise ValueError("task lifecycle has more than one finish")
            for attempt, attempt_state in attempt_states:
                if attempt.task == event.task and not attempt_state.finished:
                    raise ValueError("task finished before one of its attempts")
            state.finished = True
            assert isinstance(event.status, AttemptStatus)
            state.finish_status = event.status
            state.finish_reason = event.reason
        elif event.kind is EventKind.ATTEMPT_STARTED:
            assert event.attempt is not None
            task_state = task_states.get(event.attempt.task)
            if task_state is None or not task_state.started:
                raise ValueError("attempt started before its task started")
            if task_state.finished:
                raise ValueError("attempt started after its task finished")
            state = _attempt_state(attempt_states, event.attempt)
            if state.started or state.finished:
                raise ValueError("attempt lifecycle has an invalid start")
            state.started = True
        elif event.kind is EventKind.ATTEMPT_FINISHED:
            assert event.attempt is not None
            task_state = task_states.get(event.attempt.task)
            if task_state is None or not task_state.started:
                raise ValueError("attempt finished before its task started")
            if task_state.finished:
                raise ValueError("attempt finished after its task finished")
            state = _attempt_state(attempt_states, event.attempt)
            if state.finished:
                raise ValueError("attempt lifecycle has more than one finish")
            state.finished = True
            assert isinstance(event.status, AttemptStatus)
            state.finish_status = event.status
            state.finish_reason = event.reason

    for task, state in task_states.items():
        if not state.finished:
            raise ValueError("every task must have exactly one finish")
        if not state.started and not _finish_without_start_is_allowed(state):
            raise ValueError("only skipped tasks may finish without starting")

    for attempt, state in attempt_states:
        if not state.finished:
            raise ValueError("every attempt must have exactly one finish")
        if not state.started and not _finish_without_start_is_allowed(state):
            raise ValueError(
                "only dependency-blocked attempts may finish without starting"
            )
        task_state = task_states.get(attempt.task)
        if task_state is None or not task_state.finished:
            raise ValueError("every attempt must belong to a finished task")
