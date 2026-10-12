"""Actual body/commit statistics independent of optional invocation history."""

from datetime import timedelta

import pytest

from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.file_materialization import FileMaterialization, JsonCodec
from kazeflow.flow import Flow
from kazeflow.partition import PartitionDef
from kazeflow.resources import ResourcePolicy, Retry
from kazeflow.results import AttemptStatus, FlowStatus


class Keys(PartitionDef):
    def range(self, start, end):
        raise AssertionError("not used")


@pytest.mark.asyncio
async def test_actual_operations_count_without_invocation_history_and_reset():
    registry = AssetRegistry()

    def work(context: AssetContext):
        if context.partition_key == "failed":
            raise OSError("failure")
        return 42

    registry.register(work, partition_def=Keys(), resource="api")
    flow = Flow(
        ["work"],
        registry=registry,
        resources={"api": ResourcePolicy(1), "unused": ResourcePolicy(1)},
    )
    first = await flow.run_async(
        {"partition_keys": ["ok", "failed"], "max_concurrency": 1}
    )
    stats = first.resource_statistics("api")
    assert (stats.invocations, stats.succeeded, stats.failed, stats.retries) == (
        2,
        1,
        1,
        0,
    )
    assert stats.invocation_duration >= timedelta()
    assert stats.wait_count == 0
    assert stats.wait_duration == timedelta()
    assert all(attempt.invocations == () for attempt in first.tasks[0].attempts)
    assert first.status is FlowStatus.FAILED
    assert [item.name for item in first.resources] == ["api"]
    with pytest.raises(KeyError):
        first.resource_statistics("unused")
    second = await flow.run_async({"partition_keys": ["ok"]})
    assert second.resource_statistics("api").invocations == 1
    assert first.resource_statistics("api") is stats
    empty = await flow.run_async({"partition_keys": []})
    assert empty.resource_statistics("api").invocations == 0
    assert empty.resource_statistics("api").wait_duration == timedelta()


@pytest.mark.asyncio
async def test_actual_retry_count_and_recovered_result():
    registry = AssetRegistry()
    calls = 0

    def work():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("retry")
        return 42

    registry.register(work, resource="api")
    flow = Flow(
        ["work"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1, max_attempts=2, classify_failure=lambda error: Retry(0)
            )
        },
    )
    result = await flow.run_async()
    stats = result.resource_statistics("api")
    assert (stats.invocations, stats.succeeded, stats.failed, stats.retries) == (
        2,
        1,
        1,
        1,
    )
    assert result.status is FlowStatus.SUCCESS
    assert result.tasks[0].attempts[0].status is AttemptStatus.SUCCESS


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["resolve", "load", "commit"])
async def test_adapter_phase_failure_counts_only_started_body_operations(
    tmp_path, phase
):
    class FailingAdapter(FileMaterialization):
        fail = False

        def resolve(self, context):
            if self.fail and phase == "resolve":
                raise OSError("resolve failed")
            return super().resolve(context)

        def load(self, record):
            if self.fail and phase == "load":
                raise OSError("load failed")
            return super().load(record)

        def commit(self, context, output):
            if self.fail and phase == "commit":
                raise OSError("commit failed")
            return super().commit(context, output)

    adapter = FailingAdapter(tmp_path / "store", codec=JsonCodec())
    registry = AssetRegistry()
    calls = []

    def work(context: AssetContext):
        calls.append(context.partition_key)
        return 42

    registry.register(
        work, partition_def=Keys(), materialization=adapter, version="1", resource="api"
    )
    flow = Flow(["work"], registry=registry, resources={"api": ResourcePolicy(1)})
    if phase == "load":
        await flow.run_async({"partition_keys": [0]})
        calls.clear()
    adapter.fail = True
    result = await flow.run_async({"partition_keys": [0]})
    stats = result.resource_statistics("api")
    assert result.status is FlowStatus.FAILED
    assert stats.invocations == (1 if phase == "commit" else 0)
    assert stats.failed == stats.invocations
    assert stats.succeeded == 0
    assert stats.retries == 0
    assert calls == ([0] if phase == "commit" else [])
    if phase != "commit":
        assert stats.invocation_duration == timedelta()


@pytest.mark.asyncio
async def test_materialized_success_has_zero_operations_and_preserves_projection_shapes(
    tmp_path,
):
    registry = AssetRegistry()
    adapter = FileMaterialization(tmp_path / "store", codec=JsonCodec())
    calls = []

    def work(context: AssetContext):
        calls.append(context.partition_key)
        return 42

    registry.register(
        work, partition_def=Keys(), materialization=adapter, version="1", resource="api"
    )
    flow = Flow(["work"], registry=registry, resources={"api": ResourcePolicy(1)})
    first = await flow.run_async({"partition_keys": [0]})
    second = await flow.run_async({"partition_keys": [0]})
    assert first.resource_statistics("api").invocations == 1
    stats = second.resource_statistics("api")
    assert (
        stats.invocations,
        stats.succeeded,
        stats.failed,
        stats.retries,
        stats.wait_count,
    ) == (0, 0, 0, 0, 0)
    assert stats.wait_duration == stats.invocation_duration == timedelta()
    assert calls == [0]
    assert second.tasks[0].attempts[0].resolution == "materialized"
    for version in (1, 2, 3, 4):
        record = second.to_record(schema_version=version)
        assert set(record) == {
            "run_id",
            "status",
            "started_at",
            "ended_at",
            "duration_seconds",
            "tasks",
        }
        assert "resource_statistics" not in record and "resources" not in record


@pytest.mark.asyncio
async def test_exhausted_retry_decision_does_not_count_as_a_retry_start():
    registry = AssetRegistry()

    def work():
        raise OSError("retry denied by exhausted budget")

    registry.register(work, resource="api")
    result = await Flow(
        ["work"],
        registry=registry,
        resources={"api": ResourcePolicy(1, classify_failure=lambda error: Retry(1))},
    ).run_async()
    stats = result.resource_statistics("api")
    assert stats.invocations == stats.failed == 1
    assert stats.succeeded == stats.retries == stats.wait_count == 0


@pytest.mark.asyncio
async def test_resource_summaries_follow_plan_resource_order_and_unresourced_work_is_absent():
    registry = AssetRegistry()

    def first():
        return "first"

    def second():
        return "second"

    def plain():
        return "plain"

    registry.register(first, resource="zeta")
    registry.register(second, resource="alpha")
    registry.register(plain)
    flow = Flow(
        ["first", "second", "plain"],
        registry=registry,
        resources={"zeta": ResourcePolicy(1), "alpha": ResourcePolicy(1)},
    )
    result = await flow.run_async()
    assert [stat.name for stat in result.resources] == ["alpha", "zeta"]
    assert all(stat.invocations == stat.succeeded == 1 for stat in result.resources)
    plain_result = await Flow(["plain"], registry=registry).run_async()
    assert plain_result.resources == ()
