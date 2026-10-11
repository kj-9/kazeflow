"""Behavioral tests for live partition outcome queries."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from kazeflow import OpenCircuit
from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.flow import Flow
from kazeflow.partition import PartitionDef
from kazeflow.resources import ResourcePolicy
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


NOW = datetime(2026, 10, 12, tzinfo=timezone.utc)
LATER = NOW + timedelta(seconds=1)
DURATION = timedelta(seconds=1)
FAILURE = FailureInfo("RuntimeError", "failed", "traceback")


def _attempt(
    task_name: str,
    key: object,
    status: AttemptStatus,
    *,
    reason: SkipReason | None = None,
    blocked_by: tuple[AttemptReference, ...] = (),
    invocations: tuple[InvocationResult, ...] = (),
) -> AttemptResult:
    return AttemptResult(
        attempt=AttemptReference(
            TaskReference(task_name), partition_key_present=True, partition_key=key
        ),
        status=status,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        failure=FAILURE if status is AttemptStatus.FAILED else None,
        reason=reason,
        blocked_by=blocked_by,
        invocations=invocations,
    )


def _task(
    attempts: tuple[AttemptResult, ...],
    *,
    task_name: str = "partitioned",
    is_partitioned: bool = True,
    status: AttemptStatus | None = None,
    reason: SkipReason | None = None,
    blocked_by: tuple[AttemptReference, ...] = (),
) -> TaskResult:
    if status is None:
        if any(item.status is AttemptStatus.FAILED for item in attempts):
            status = AttemptStatus.FAILED
        elif any(item.status is AttemptStatus.CANCELLED for item in attempts):
            status = AttemptStatus.CANCELLED
        elif any(item.status is AttemptStatus.SKIPPED for item in attempts):
            status = AttemptStatus.SKIPPED
        else:
            status = AttemptStatus.SUCCESS
    if attempts and all(
        item.status is AttemptStatus.SKIPPED
        and item.reason
        in {SkipReason.DEPENDENCY_BLOCKED, SkipReason.RESOURCE_CIRCUIT_OPEN}
        for item in attempts
    ):
        if reason is None:
            if all(item.reason is SkipReason.DEPENDENCY_BLOCKED for item in attempts):
                reason = SkipReason.DEPENDENCY_BLOCKED
            elif all(
                item.reason is SkipReason.RESOURCE_CIRCUIT_OPEN for item in attempts
            ):
                reason = SkipReason.RESOURCE_CIRCUIT_OPEN
            else:
                reason = SkipReason.MIXED_BLOCKED
        if not blocked_by:
            blocked_by = tuple(
                blocker for item in attempts for blocker in item.blocked_by
            )
    return TaskResult(
        task=TaskReference(task_name),
        is_partitioned=is_partitioned,
        status=status,
        started_at=NOW,
        ended_at=LATER,
        duration=DURATION,
        attempts=attempts,
        reason=reason,
        blocked_by=blocked_by,
    )


def _run(*tasks: TaskResult, status: FlowStatus | None = None) -> RunResult:
    if status is None:
        if any(task.status is AttemptStatus.FAILED for task in tasks):
            status = FlowStatus.FAILED
        elif any(task.status is AttemptStatus.CANCELLED for task in tasks):
            status = FlowStatus.CANCELLED
        else:
            status = FlowStatus.SUCCESS
    return RunResult("query-test", status, NOW, LATER, DURATION, tuple(tasks))


class OpaqueKey:
    __hash__ = None

    def __bool__(self) -> bool:
        raise AssertionError("query must not test partition-key truthiness")

    def __eq__(self, other: object) -> bool:
        raise AssertionError("query must not compare partition keys")


def test_queries_preserve_falsey_opaque_keys_and_attempt_order() -> None:
    opaque = OpaqueKey()
    list_key = ["mutable", "key"]
    circuit_blocker = AttemptReference(TaskReference("source"))
    failed_keys = [0, False, list_key]
    circuit_keys = ["", opaque, opaque]
    attempts = (
        _attempt("partitioned", failed_keys[0], AttemptStatus.FAILED),
        _attempt(
            "partitioned",
            circuit_keys[0],
            AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(circuit_blocker,),
        ),
        _attempt("partitioned", failed_keys[1], AttemptStatus.FAILED),
        _attempt(
            "partitioned",
            circuit_keys[1],
            AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(circuit_blocker,),
        ),
        _attempt(
            "partitioned",
            circuit_keys[2],
            AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(circuit_blocker,),
        ),
        _attempt("partitioned", failed_keys[2], AttemptStatus.FAILED),
    )
    result = _run(_task(attempts))

    failed = result.failed_partition_keys("partitioned")
    circuit = result.circuit_skipped_partition_keys("partitioned")
    assert isinstance(failed, tuple)
    assert all(
        actual is expected for actual, expected in zip(failed, failed_keys, strict=True)
    )
    assert all(
        actual is expected
        for actual, expected in zip(circuit, circuit_keys, strict=True)
    )


def test_queries_filter_each_terminal_attempt_in_mixed_failed_aggregate() -> None:
    source = AttemptReference(TaskReference("source"))
    dependency = AttemptReference(TaskReference("dependency"))
    recovered = (
        InvocationResult(
            1,
            AttemptStatus.FAILED,
            NOW,
            LATER,
            DURATION,
            failure=FAILURE,
            retry_delay_seconds=0,
        ),
        InvocationResult(2, AttemptStatus.SUCCESS, NOW, LATER, DURATION),
    )
    circuit_after_failure = (
        InvocationResult(
            1,
            AttemptStatus.FAILED,
            NOW,
            LATER,
            DURATION,
            failure=FAILURE,
            retry_delay_seconds=1,
        ),
    )
    failed_key = object()
    recovered_key = object()
    dependency_key = object()
    circuit_key = object()
    cancelled_key = object()
    attempts = (
        _attempt("partitioned", failed_key, AttemptStatus.FAILED),
        _attempt(
            "partitioned", recovered_key, AttemptStatus.SUCCESS, invocations=recovered
        ),
        _attempt(
            "partitioned",
            dependency_key,
            AttemptStatus.SKIPPED,
            reason=SkipReason.DEPENDENCY_BLOCKED,
            blocked_by=(dependency,),
        ),
        _attempt(
            "partitioned",
            circuit_key,
            AttemptStatus.SKIPPED,
            reason=SkipReason.RESOURCE_CIRCUIT_OPEN,
            blocked_by=(source,),
            invocations=circuit_after_failure,
        ),
        _attempt("partitioned", cancelled_key, AttemptStatus.CANCELLED),
    )
    result = _run(_task(attempts))

    failed = result.failed_partition_keys("partitioned")
    circuit = result.circuit_skipped_partition_keys("partitioned")
    assert len(failed) == 1 and failed[0] is failed_key
    assert len(circuit) == 1 and circuit[0] is circuit_key
    assert result.tasks[0].status is AttemptStatus.FAILED
    assert result.tasks[0].reason is None


def test_no_outcomes_and_explicit_empty_partition_work_return_empty_tuples() -> None:
    successful = _task((_attempt("partitioned", "ok", AttemptStatus.SUCCESS),))
    empty = _task(
        (),
        task_name="empty",
        status=AttemptStatus.SKIPPED,
        reason=SkipReason.NO_PARTITION_KEYS,
    )
    result = _run(successful, empty)

    assert result.failed_partition_keys("partitioned") == ()
    assert result.circuit_skipped_partition_keys("partitioned") == ()
    assert result.failed_partition_keys("empty") == ()
    assert result.circuit_skipped_partition_keys("empty") == ()


@pytest.mark.parametrize(
    "method_name", ["failed_partition_keys", "circuit_skipped_partition_keys"]
)
def test_query_rejects_non_string_names(method_name: str) -> None:
    result = _run(
        _task((), status=AttemptStatus.SKIPPED, reason=SkipReason.NO_PARTITION_KEYS)
    )

    with pytest.raises(TypeError):
        getattr(result, method_name)(None)


@pytest.mark.parametrize(
    "method_name", ["failed_partition_keys", "circuit_skipped_partition_keys"]
)
@pytest.mark.parametrize("missing_name", ["unknown", ""])
def test_query_key_error_carries_exact_missing_name(
    method_name: str, missing_name: str
) -> None:
    result = _run(
        _task((), status=AttemptStatus.SKIPPED, reason=SkipReason.NO_PARTITION_KEYS)
    )

    with pytest.raises(KeyError) as error:
        getattr(result, method_name)(missing_name)
    assert error.value.args == (missing_name,)


@pytest.mark.parametrize(
    "method_name", ["failed_partition_keys", "circuit_skipped_partition_keys"]
)
def test_query_rejects_known_unpartitioned_task_even_without_outcomes(
    method_name: str,
) -> None:
    unpartitioned_attempt = AttemptResult(
        AttemptReference(TaskReference("plain")),
        AttemptStatus.SUCCESS,
        NOW,
        LATER,
        DURATION,
    )
    unpartitioned = _task(
        (unpartitioned_attempt,),
        task_name="plain",
        is_partitioned=False,
    )
    result = _run(unpartitioned)

    with pytest.raises(ValueError, match="plain.*partition"):
        getattr(result, method_name)("plain")


class Keys(PartitionDef):
    def range(self, start, end):
        raise AssertionError("range is not used")


@pytest.mark.asyncio
async def test_live_flow_queries_support_explicit_failed_key_rerun() -> None:
    registry = AssetRegistry()
    should_fail = True
    calls: list[object] = []

    async def work(context: AssetContext) -> object:
        nonlocal should_fail
        key = context.partition_key
        calls.append(key)
        if key == "source" and should_fail:
            raise RuntimeError("temporary failure")
        return key

    registry.register(work, partition_def=Keys(), resource="service")
    flow = Flow(
        ["work"],
        registry=registry,
        resources={
            "service": ResourcePolicy(1, classify_failure=lambda error: OpenCircuit())
        },
    )
    first = await flow.run_async(
        {"partition_keys": ["source", "pending"], "max_concurrency": 2}
    )

    assert first.failed_partition_keys("work") == ("source",)
    assert first.circuit_skipped_partition_keys("work") == ("pending",)
    assert first.tasks[0].status is AttemptStatus.FAILED

    should_fail = False
    # The caller chooses and passes the returned keys explicitly.
    retry_keys = first.failed_partition_keys("work")
    second = await flow.run_async({"partition_keys": retry_keys})
    assert second.tasks[0].attempts[0].status is AttemptStatus.SUCCESS
    assert calls == ["source", "source"]


def test_query_does_not_change_portable_record_shapes_or_values() -> None:
    opaque = OpaqueKey()
    failed = _attempt("partitioned", opaque, AttemptStatus.FAILED)
    result = _run(_task((failed,)))
    before = {
        version: result.to_record(schema_version=version) for version in (1, 2, 3)
    }

    result.failed_partition_keys("partitioned")
    result.circuit_skipped_partition_keys("partitioned")

    after = {version: result.to_record(schema_version=version) for version in (1, 2, 3)}
    assert after == before
    for record in after.values():
        for task in record["tasks"]:
            for attempt in task["attempts"]:
                assert "partition_key" not in attempt["attempt"]
