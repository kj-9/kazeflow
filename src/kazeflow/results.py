"""Immutable, presentation-neutral execution result values."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


class FlowStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AttemptStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class SkipReason(str, Enum):
    DEPENDENCY_BLOCKED = "dependency_blocked"
    NO_PARTITION_KEYS = "no_partition_keys"
    RESOURCE_CIRCUIT_OPEN = "resource_circuit_open"
    MIXED_BLOCKED = "mixed_blocked"


_TERMINAL_ATTEMPT_STATUSES = frozenset(
    {
        AttemptStatus.SUCCESS,
        AttemptStatus.FAILED,
        AttemptStatus.SKIPPED,
        AttemptStatus.CANCELLED,
    }
)
_TERMINAL_FLOW_STATUSES = frozenset(
    {FlowStatus.SUCCESS, FlowStatus.FAILED, FlowStatus.CANCELLED}
)


def _validate_utc_timestamp(value: datetime, field_name: str) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError(f"{field_name} must be an aware UTC datetime")


def _validate_completed_timing(
    started_at: datetime, ended_at: datetime, duration: timedelta
) -> None:
    _validate_utc_timestamp(started_at, "started_at")
    _validate_utc_timestamp(ended_at, "ended_at")
    if ended_at < started_at:
        raise ValueError("ended_at must not precede started_at")
    if not isinstance(duration, timedelta) or duration < timedelta(0):
        raise ValueError("duration must be a non-negative timedelta")


def _validate_blocked_by(blocked_by: tuple[AttemptReference, ...]) -> None:
    if not isinstance(blocked_by, tuple) or not all(
        isinstance(blocker, AttemptReference) for blocker in blocked_by
    ):
        raise ValueError("blocked_by must be a tuple of AttemptReference values")


def _validate_record_version(schema_version: int) -> None:
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version not in (1, 2, 3, 4)
    ):
        raise ValueError("schema_version must be 1, 2, 3, or 4")


def _validate_reason_version(reason: SkipReason | None, schema_version: int) -> None:
    if (
        schema_version in (1, 2)
        and reason is not None
        and reason
        in {
            SkipReason.RESOURCE_CIRCUIT_OPEN,
            SkipReason.MIXED_BLOCKED,
        }
    ):
        raise ValueError(
            f"{reason.value} cannot be projected as schema version {schema_version}"
        )


def _validate_optional_seconds(value: float | None, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be finite non-negative seconds or None")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite or value < 0:
        raise ValueError(f"{field_name} must be finite non-negative seconds or None")


@dataclass(frozen=True, slots=True)
class TaskReference:
    task_name: str

    def __post_init__(self) -> None:
        if not isinstance(self.task_name, str) or not self.task_name:
            raise ValueError("task_name must be a non-empty string")

    def to_record(self) -> dict[str, object]:
        return {"task_name": self.task_name}


@dataclass(frozen=True, slots=True)
class AttemptReference:
    task: TaskReference
    partition_key_present: bool = False
    partition_key: object | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.task, TaskReference):
            raise ValueError("task must be a TaskReference")
        if not isinstance(self.partition_key_present, bool):
            raise ValueError("partition_key_present must be a bool")
        if self.partition_key_present != (self.partition_key is not None):
            raise ValueError(
                "partition_key_present and partition_key must agree about presence"
            )

    def to_record(self) -> dict[str, object]:
        return {
            "task": self.task.to_record(),
            "partition": {"present": self.partition_key_present},
        }


@dataclass(frozen=True, slots=True)
class FailureInfo:
    exception_type: str
    message: str
    traceback: str

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str)
            for value in (self.exception_type, self.message, self.traceback)
        ):
            raise ValueError("failure metadata fields must be strings")

    def to_record(self) -> dict[str, object]:
        return {
            "exception_type": self.exception_type,
            "message": self.message,
            "traceback": self.traceback,
        }


@dataclass(frozen=True, slots=True)
class InvocationResult:
    """One numbered body-plus-commit operation within a logical attempt."""

    number: int
    status: AttemptStatus
    started_at: datetime
    ended_at: datetime
    duration: timedelta
    failure: FailureInfo | None = None
    retry_delay_seconds: float | None = None
    cooldown_seconds: float | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.number, bool)
            or not isinstance(self.number, int)
            or self.number < 1
        ):
            raise ValueError("number must be a positive integer")
        if not isinstance(self.status, AttemptStatus) or self.status not in (
            AttemptStatus.SUCCESS,
            AttemptStatus.FAILED,
        ):
            raise ValueError("invocation status must be success or failed")
        _validate_completed_timing(self.started_at, self.ended_at, self.duration)
        _validate_optional_seconds(self.retry_delay_seconds, "retry_delay_seconds")
        _validate_optional_seconds(self.cooldown_seconds, "cooldown_seconds")
        if self.status is AttemptStatus.FAILED:
            if not isinstance(self.failure, FailureInfo):
                raise ValueError("a failed invocation requires FailureInfo")
        elif self.failure is not None:
            raise ValueError("a successful invocation cannot carry failure")
        if self.status is AttemptStatus.SUCCESS and (
            self.retry_delay_seconds is not None or self.cooldown_seconds is not None
        ):
            raise ValueError("retry delays are only valid for failed invocations")

    def to_record(self) -> dict[str, object]:
        return {
            "number": self.number,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": self.duration.total_seconds(),
            "failure": None if self.failure is None else self.failure.to_record(),
            "retry_delay_seconds": self.retry_delay_seconds,
            "cooldown_seconds": self.cooldown_seconds,
        }


@dataclass(frozen=True, slots=True)
class AttemptResult:
    attempt: AttemptReference
    status: AttemptStatus
    started_at: datetime
    ended_at: datetime
    duration: timedelta
    output: object = None
    exception: BaseException | None = None
    failure: FailureInfo | None = None
    reason: SkipReason | None = None
    blocked_by: tuple[AttemptReference, ...] = ()
    resolution: str | None = None
    materialization_id: str | None = None
    invocations: tuple[InvocationResult, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.attempt, AttemptReference):
            raise ValueError("attempt must be an AttemptReference")
        if (
            not isinstance(self.status, AttemptStatus)
            or self.status not in _TERMINAL_ATTEMPT_STATUSES
        ):
            raise ValueError("status must be a terminal AttemptStatus")
        _validate_completed_timing(self.started_at, self.ended_at, self.duration)
        _validate_blocked_by(self.blocked_by)
        if not isinstance(self.invocations, tuple) or not all(
            isinstance(invocation, InvocationResult) for invocation in self.invocations
        ):
            raise ValueError("invocations must be a tuple of InvocationResult values")
        for index, invocation in enumerate(self.invocations, start=1):
            if invocation.number != index:
                raise ValueError("invocation numbers must be contiguous and one-based")
            if index < len(self.invocations):
                if invocation.status is not AttemptStatus.FAILED:
                    raise ValueError("only failed invocations may precede another")
                if invocation.retry_delay_seconds is None:
                    raise ValueError("a retried invocation requires a retry delay")
        if self.invocations and self.status in {
            AttemptStatus.SUCCESS,
            AttemptStatus.FAILED,
        }:
            last_invocation = self.invocations[-1]
            if last_invocation.status is not self.status:
                raise ValueError(
                    "the last invocation status must match the logical attempt status"
                )
            if last_invocation.retry_delay_seconds is not None:
                raise ValueError(
                    "a scheduled retry cannot terminate a successful or failed attempt"
                )

        if self.status is AttemptStatus.SUCCESS:
            if self.resolution not in (None, "executed", "materialized"):
                raise ValueError("resolution must be executed, materialized, or None")
            if self.materialization_id is not None and (
                not isinstance(self.materialization_id, str)
                or not self.materialization_id
            ):
                raise ValueError("materialization_id must be a non-empty string")
            if self.resolution == "materialized" and self.materialization_id is None:
                raise ValueError("a materialized attempt requires a materialization_id")
            if self.resolution is None and self.materialization_id is not None:
                raise ValueError("materialization_id requires materialized resolution")
        elif self.resolution is not None or self.materialization_id is not None:
            raise ValueError(
                "only a successful attempt may have materialization resolution data"
            )

        if self.status is AttemptStatus.FAILED:
            if not isinstance(self.failure, FailureInfo):
                raise ValueError("a failed attempt requires FailureInfo")
        elif self.failure is not None or self.exception is not None:
            raise ValueError("only a failed attempt may retain failure information")

        if self.status is AttemptStatus.SKIPPED:
            if self.reason not in {
                SkipReason.DEPENDENCY_BLOCKED,
                SkipReason.RESOURCE_CIRCUIT_OPEN,
            }:
                raise ValueError(
                    "a skipped attempt must be dependency_blocked or resource_circuit_open"
                )
            if not self.blocked_by:
                raise ValueError("a blocked attempt requires blockers")
            if (
                self.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
                and len(self.blocked_by) != 1
            ):
                raise ValueError(
                    "a circuit-blocked attempt requires exactly one blocker"
                )
        elif self.reason is not None or self.blocked_by:
            raise ValueError("only a skipped attempt may have a reason or blockers")

    def to_record(self, *, schema_version: int = 1) -> dict[str, object]:
        _validate_record_version(schema_version)
        _validate_reason_version(self.reason, schema_version)
        record: dict[str, object] = {
            "attempt": self.attempt.to_record(),
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": self.duration.total_seconds(),
            "reason": None if self.reason is None else self.reason.value,
            "blocked_by": [blocker.to_record() for blocker in self.blocked_by],
            "failure": None if self.failure is None else self.failure.to_record(),
        }
        if schema_version in (2, 3, 4):
            record["invocations"] = [
                invocation.to_record() for invocation in self.invocations
            ]
        if schema_version == 4:
            record["resolution"] = self.resolution
            record["materialization_id"] = self.materialization_id
        return record


@dataclass(frozen=True, slots=True)
class TaskResult:
    task: TaskReference
    is_partitioned: bool
    status: AttemptStatus
    started_at: datetime
    ended_at: datetime
    duration: timedelta
    attempts: tuple[AttemptResult, ...] = ()
    reason: SkipReason | None = None
    blocked_by: tuple[AttemptReference, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.task, TaskReference):
            raise ValueError("task must be a TaskReference")
        if not isinstance(self.is_partitioned, bool):
            raise ValueError("is_partitioned must be a bool")
        if (
            not isinstance(self.status, AttemptStatus)
            or self.status not in _TERMINAL_ATTEMPT_STATUSES
        ):
            raise ValueError("status must be a terminal AttemptStatus")
        _validate_completed_timing(self.started_at, self.ended_at, self.duration)
        if not isinstance(self.attempts, tuple) or not all(
            isinstance(attempt, AttemptResult) for attempt in self.attempts
        ):
            raise ValueError("attempts must be a tuple of AttemptResult values")
        _validate_blocked_by(self.blocked_by)

        if self.is_partitioned:
            self._validate_partitioned()
        else:
            self._validate_unpartitioned()

    def _validate_unpartitioned(self) -> None:
        if len(self.attempts) != 1:
            raise ValueError("an unpartitioned task must have exactly one attempt")
        attempt = self.attempts[0]
        if attempt.attempt.task != self.task or attempt.attempt.partition_key_present:
            raise ValueError(
                "an unpartitioned attempt must use this task and no partition"
            )
        if (
            self.status is not attempt.status
            or self.reason is not attempt.reason
            or self.blocked_by != attempt.blocked_by
        ):
            raise ValueError("an unpartitioned task aggregate must equal its attempt")

    def _validate_partitioned(self) -> None:
        if not self.attempts:
            if (
                self.status is not AttemptStatus.SKIPPED
                or self.reason is not SkipReason.NO_PARTITION_KEYS
                or self.blocked_by
            ):
                raise ValueError(
                    "a zero-attempt partitioned task must be no_partition_keys"
                )
            return

        if any(
            attempt.attempt.task != self.task
            or not attempt.attempt.partition_key_present
            for attempt in self.attempts
        ):
            raise ValueError("partitioned attempts must use this task and a partition")

        if any(attempt.status is AttemptStatus.FAILED for attempt in self.attempts):
            expected_status = AttemptStatus.FAILED
        elif any(
            attempt.status is AttemptStatus.CANCELLED for attempt in self.attempts
        ):
            expected_status = AttemptStatus.CANCELLED
        elif any(attempt.status is AttemptStatus.SKIPPED for attempt in self.attempts):
            expected_status = AttemptStatus.SKIPPED
        else:
            expected_status = AttemptStatus.SUCCESS

        if self.status is not expected_status:
            raise ValueError("partitioned task status must match its attempt outcomes")

        all_dependency_blocked = all(
            attempt.status is AttemptStatus.SKIPPED
            and attempt.reason is SkipReason.DEPENDENCY_BLOCKED
            for attempt in self.attempts
        )
        all_resource_circuit_blocked = all(
            attempt.status is AttemptStatus.SKIPPED
            and attempt.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
            for attempt in self.attempts
        )
        all_blocked = all(
            attempt.status is AttemptStatus.SKIPPED
            and attempt.reason
            in {SkipReason.DEPENDENCY_BLOCKED, SkipReason.RESOURCE_CIRCUIT_OPEN}
            for attempt in self.attempts
        )
        if all_blocked:
            expected_blocked_by = tuple(
                blocker for attempt in self.attempts for blocker in attempt.blocked_by
            )
            if all_dependency_blocked:
                expected_reason = SkipReason.DEPENDENCY_BLOCKED
            elif all_resource_circuit_blocked:
                expected_reason = SkipReason.RESOURCE_CIRCUIT_OPEN
            else:
                expected_reason = SkipReason.MIXED_BLOCKED
            if (
                self.reason is not expected_reason
                or self.blocked_by != expected_blocked_by
            ):
                raise ValueError(
                    "a blocked task aggregate must retain its reason and blockers"
                )
        elif self.reason is not None or self.blocked_by:
            raise ValueError(
                "a mixed or non-skipped partitioned aggregate has no reason or blockers"
            )

    def to_record(self, *, schema_version: int = 1) -> dict[str, object]:
        _validate_record_version(schema_version)
        _validate_reason_version(self.reason, schema_version)
        return {
            "task": self.task.to_record(),
            "is_partitioned": self.is_partitioned,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": self.duration.total_seconds(),
            "reason": None if self.reason is None else self.reason.value,
            "blocked_by": [blocker.to_record() for blocker in self.blocked_by],
            "attempts": [
                attempt.to_record(schema_version=schema_version)
                for attempt in self.attempts
            ],
        }


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: str
    status: FlowStatus
    started_at: datetime
    ended_at: datetime
    duration: timedelta
    tasks: tuple[TaskResult, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("run_id must be a non-empty string")
        if (
            not isinstance(self.status, FlowStatus)
            or self.status not in _TERMINAL_FLOW_STATUSES
        ):
            raise ValueError("status must be a terminal FlowStatus")
        _validate_completed_timing(self.started_at, self.ended_at, self.duration)
        if not isinstance(self.tasks, tuple) or not all(
            isinstance(task, TaskResult) for task in self.tasks
        ):
            raise ValueError("tasks must be a tuple of TaskResult values")

        task_names: set[str] = set()
        for task in self.tasks:
            if task.task.task_name in task_names:
                raise ValueError("run tasks must have unique task references")
            task_names.add(task.task.task_name)

        if any(task.status is AttemptStatus.FAILED for task in self.tasks):
            expected_status = FlowStatus.FAILED
        elif any(task.status is AttemptStatus.CANCELLED for task in self.tasks):
            expected_status = FlowStatus.CANCELLED
        else:
            expected_status = FlowStatus.SUCCESS
        if self.status is not expected_status:
            raise ValueError("run status must match its task outcomes")

    def _partitioned_task(self, task_name: str) -> TaskResult:
        if not isinstance(task_name, str):
            raise TypeError("task_name must be a string")
        for task in self.tasks:
            if task.task.task_name == task_name:
                if not task.is_partitioned:
                    raise ValueError(f"task {task_name!r} is not partitioned")
                return task
        raise KeyError(task_name)

    def failed_partition_keys(self, task_name: str) -> tuple[object, ...]:
        """Return original keys of terminal failures in attempt order.

        Recovered retries, skips and cancellations are excluded. Keys are live,
        application-owned objects; portable records cannot reconstruct them.
        Raises TypeError for non-string names, KeyError for missing tasks and
        ValueError for unpartitioned tasks.
        """
        task = self._partitioned_task(task_name)
        return tuple(
            attempt.attempt.partition_key
            for attempt in task.attempts
            if attempt.status is AttemptStatus.FAILED
        )

    def circuit_skipped_partition_keys(self, task_name: str) -> tuple[object, ...]:
        """Return original keys skipped by an open circuit in attempt order.

        Dependency skips and cancellations are excluded. Keys retain their live
        object identity. Lookup errors match failed_partition_keys.
        """
        task = self._partitioned_task(task_name)
        return tuple(
            attempt.attempt.partition_key
            for attempt in task.attempts
            if attempt.status is AttemptStatus.SKIPPED
            and attempt.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
        )

    def to_record(self, *, schema_version: int = 1) -> dict[str, object]:
        _validate_record_version(schema_version)
        for task in self.tasks:
            _validate_reason_version(task.reason, schema_version)
            for attempt in task.attempts:
                _validate_reason_version(attempt.reason, schema_version)
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_seconds": self.duration.total_seconds(),
            "tasks": [
                task.to_record(schema_version=schema_version) for task in self.tasks
            ],
        }
