"""Portable and observed materialization provenance at the commit boundary."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.events import EventKind, ExecutionEvent, validate_event_sequence
from kazeflow.file_materialization import FileMaterialization, JsonCodec
from kazeflow.flow import Flow
from kazeflow.partition import PartitionDef
from kazeflow.resources import ResourcePolicy, Retry
from kazeflow.results import (
    AttemptReference,
    AttemptResult,
    AttemptStatus,
    FlowStatus,
    TaskReference,
)


NOW = datetime(2026, 10, 12, tzinfo=timezone.utc)
REF = AttemptReference(TaskReference("work"), True, 0)


@pytest.mark.parametrize(
    "resolution,identifier",
    [(None, None), ("executed", None), ("executed", "id"), ("materialized", "id")],
)
def test_explicit_result_and_event_projection_versions(resolution, identifier):
    result = AttemptResult(
        REF,
        AttemptStatus.SUCCESS,
        NOW,
        NOW,
        timedelta(),
        output=object(),
        resolution=resolution,
        materialization_id=identifier,
    )
    event = ExecutionEvent(
        "run",
        1,
        NOW,
        EventKind.ATTEMPT_FINISHED,
        attempt=REF,
        status=AttemptStatus.SUCCESS,
        resolution=resolution,
        materialization_id=identifier,
    )
    projected = result.to_record(schema_version=4)
    assert projected["resolution"] == resolution
    assert projected["materialization_id"] == identifier
    for version in (1, 2, 3):
        for old in (
            result.to_record(schema_version=version),
            event.to_record(schema_version=version),
        ):
            assert "resolution" not in old and "materialization_id" not in old
    assert result.to_record() == result.to_record(schema_version=1)
    assert event.to_record()["schema_version"] == 4
    assert event.to_record()["resolution"] == resolution
    assert event.to_record()["materialization_id"] == identifier
    assert "output" not in projected
    reference = projected["attempt"]
    assert isinstance(reference, dict)
    assert reference["partition"] == {"present": True}


@pytest.mark.parametrize(
    "resolution,identifier",
    [
        ("unknown", None),
        ("materialized", None),
        (None, "id"),
        ("executed", ""),
        ("executed", 42),
    ],
)
def test_invalid_success_event_provenance_rejected(resolution, identifier):
    with pytest.raises(ValueError):
        ExecutionEvent(
            "run",
            1,
            NOW,
            EventKind.ATTEMPT_FINISHED,
            attempt=REF,
            status=AttemptStatus.SUCCESS,
            resolution=resolution,
            materialization_id=identifier,
        )


@pytest.mark.parametrize(
    "kind,status",
    [
        (EventKind.ATTEMPT_STARTED, AttemptStatus.RUNNING),
        (EventKind.INVOCATION_FINISHED, AttemptStatus.SUCCESS),
        (EventKind.ATTEMPT_FINISHED, AttemptStatus.CANCELLED),
    ],
)
def test_provenance_is_invalid_on_other_lifecycle_events(kind, status):
    with pytest.raises(ValueError, match="provenance"):
        ExecutionEvent(
            "run", 1, NOW, kind, attempt=REF, status=status, resolution="executed"
        )


@pytest.mark.parametrize("version", [0, 5, True, "4"])
def test_event_projection_rejects_invalid_version(version):
    event = ExecutionEvent(
        "run", 1, NOW, EventKind.FLOW_STARTED, status=FlowStatus.RUNNING
    )
    with pytest.raises(ValueError, match="schema_version"):
        event.to_record(schema_version=version)


class Keys(PartitionDef):
    def range(self, start, end):
        raise AssertionError("not used")


class Consumer:
    def __init__(self):
        self.events = []

    def on_event(self, event):
        self.events.append(event)


@pytest.mark.asyncio
async def test_committed_retry_and_reuse_provenance_match_terminal_events(tmp_path):
    adapter = FileMaterialization(tmp_path / "store", codec=JsonCodec())
    registry = AssetRegistry()
    calls = []

    def work(context: AssetContext):
        calls.append(context.partition_key)
        if len(calls) == 1:
            raise OSError("temporary body failure")
        return {"value": 42}

    registry.register(
        work, partition_def=Keys(), materialization=adapter, version="1", resource="api"
    )
    flow = Flow(
        ["work"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1, max_attempts=2, classify_failure=lambda error: Retry(0)
            )
        },
    )
    first_observer = Consumer()
    first = await flow.run_async({"partition_keys": [0]}, event_consumer=first_observer)
    second_observer = Consumer()
    second = await flow.run_async(
        {"partition_keys": [0]}, event_consumer=second_observer
    )
    assert calls == [0, 0]
    executed = first.tasks[0].attempts[0]
    reused = second.tasks[0].attempts[0]
    assert executed.resolution == "executed"
    assert reused.resolution == "materialized"
    assert executed.materialization_id == reused.materialization_id
    assert executed.materialization_id
    assert [item.status for item in executed.invocations] == [
        AttemptStatus.FAILED,
        AttemptStatus.SUCCESS,
    ]
    assert reused.invocations == ()
    for result, observer in ((first, first_observer), (second, second_observer)):
        validate_event_sequence(observer.events)
        finish = [
            event
            for event in observer.events
            if event.kind is EventKind.ATTEMPT_FINISHED
        ]
        assert len(finish) == 1
        attempt = result.tasks[0].attempts[0]
        assert finish[0].resolution == attempt.resolution
        assert finish[0].materialization_id == attempt.materialization_id
        record = attempt.to_record(schema_version=4)
        assert record["resolution"] == attempt.resolution
        assert record["materialization_id"] == attempt.materialization_id
        assert all(
            event.resolution is None
            for event in observer.events
            if event.kind is not EventKind.ATTEMPT_FINISHED
        )
    # Failure/cancellation cannot acquire successful provenance via construction.
    with pytest.raises(ValueError):
        replace(executed, status=AttemptStatus.CANCELLED)


@pytest.mark.asyncio
async def test_commit_failure_has_null_provenance_and_blocks_downstream(tmp_path):
    class FailedCommit(FileMaterialization):
        def commit(self, context, output):
            raise OSError("commit failed")

    registry = AssetRegistry()
    calls = []

    def source(context: AssetContext):
        calls.append("source")
        return 42

    def child(source, context: AssetContext):
        calls.append("child")
        return source[context.partition_key]

    registry.register(
        source,
        partition_def=Keys(),
        materialization=FailedCommit(tmp_path / "source", codec=JsonCodec()),
        version="1",
    )
    registry.register(
        child,
        partition_def=Keys(),
        materialization=FileMaterialization(tmp_path / "child", codec=JsonCodec()),
        version="1",
    )
    observer = Consumer()
    result = await Flow(["child"], registry=registry).run_async(
        {"partition_keys": [0]}, event_consumer=observer
    )
    assert calls == ["source"]
    assert [task.status for task in result.tasks] == [
        AttemptStatus.FAILED,
        AttemptStatus.SKIPPED,
    ]
    for task in result.tasks:
        record = task.attempts[0].to_record(schema_version=4)
        assert record["resolution"] is None
        assert record["materialization_id"] is None
    finishes = [
        event for event in observer.events if event.kind is EventKind.ATTEMPT_FINISHED
    ]
    assert len(finishes) == 2
    assert all(
        event.resolution is None and event.materialization_id is None
        for event in finishes
    )
    validate_event_sequence(observer.events)
