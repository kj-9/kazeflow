"""Focused contracts for run-local rolling resource start limits."""

import asyncio
from dataclasses import FrozenInstanceError
import math
import time

import pytest

from kazeflow import RateLimit
from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.flow import Flow
from kazeflow.materialization import MaterializationRecord
from kazeflow.partition import PartitionDef
from kazeflow.resources import ResourcePolicy, _RateWindow
from kazeflow.results import AttemptStatus, FlowStatus, SkipReason


class IdentityPartitions(PartitionDef):
    def range(self, start, end):
        raise AssertionError("range is not used")


@pytest.mark.parametrize(
    ("requests", "per_seconds"),
    [
        (0, 1),
        (-1, 1),
        (True, 1),
        (1.5, 1),
        (1, 0),
        (1, -1),
        (1, True),
        (1, math.inf),
        (1, -math.inf),
        (1, math.nan),
        (1, "1"),
    ],
)
def test_rate_limit_rejects_invalid_values(requests, per_seconds):
    with pytest.raises(ValueError):
        RateLimit(requests, per_seconds)


def test_rate_limit_and_resource_plan_are_immutable_and_selected_only():
    registry = AssetRegistry()
    called = False

    def selected():
        nonlocal called
        called = True

    registry.register(selected, resource="api")
    registry.register(lambda: None, resource="unused")
    policy = ResourcePolicy(3, RateLimit(2, 0.5))
    flow = Flow(
        ["selected"],
        registry=registry,
        resources={"api": policy, "unused": ResourcePolicy(4, RateLimit(9, 3))},
    )

    plan = flow.plan()

    assert len(plan.resources) == 1
    assert plan.resources[0].name == "api"
    assert plan.resources[0].max_concurrency == 3
    assert plan.resources[0].rate_limit == RateLimit(2, 0.5)
    assert called is False
    with pytest.raises(FrozenInstanceError):
        setattr(plan.resources[0], "rate_limit", None)


def test_invalid_rate_policy_fails_preflight_without_run_events_or_body_call():
    registry = AssetRegistry()
    calls = []
    registry.register(lambda: calls.append("body"), resource="api")
    malformed = object.__new__(RateLimit)
    object.__setattr__(malformed, "requests", True)
    object.__setattr__(malformed, "per_seconds", 1.0)
    policy = object.__new__(ResourcePolicy)
    object.__setattr__(policy, "max_concurrency", 1)
    object.__setattr__(policy, "rate_limit", malformed)

    with pytest.raises(ValueError, match="requests"):
        Flow(["<lambda>"], registry=registry, resources={"api": policy}).plan()
    assert calls == []


def test_rate_window_expires_at_exact_boundary_and_retains_failed_starts():
    window = _RateWindow(RateLimit(2, 5))
    window.reserve(10)
    window.record_start(10)
    window.reserve(11)
    window.record_start(11)

    assert window.ready(14.999) is False
    assert window.deadline(12) == 15
    assert window.ready(15) is True
    assert list(window.starts) == [11]
    # A failed body still consumed the reservation and its start remains in-window.
    window.reserve(15)
    window.record_start(15)
    assert window.ready(15.999) is False
    assert window.ready(16) is True


def test_rate_windows_have_independent_state_and_runs_start_fresh():
    first = _RateWindow(RateLimit(1, 5))
    second = _RateWindow(RateLimit(1, 5))
    first.reserve(0)
    first.record_start(0)
    assert first.ready(1) is False
    assert second.ready(1) is True


@pytest.mark.asyncio
async def test_shared_rolling_limit_counts_assets_and_falsey_partition_keys():
    registry = AssetRegistry()
    starts = []

    async def alpha(context: AssetContext):
        starts.append(("alpha", context.partition_key, time.monotonic()))

    async def beta(context: AssetContext):
        starts.append(("beta", context.partition_key, time.monotonic()))

    for body in (alpha, beta):
        registry.register(body, partition_def=IdentityPartitions(), resource="api")

    result = await asyncio.wait_for(
        Flow(
            ["alpha", "beta"],
            registry=registry,
            resources={"api": ResourcePolicy(3, RateLimit(2, 0.04))},
        ).run_async({"partition_keys": [0, "", ()], "max_concurrency": 6}),
        timeout=2,
    )

    assert result.status is FlowStatus.SUCCESS
    assert len(starts) == 6
    # Every rolling interval has at most two recorded starts. The small tolerance
    # avoids relying on exact event-loop wakeup timing at expiry boundaries.
    times = sorted(item[2] for item in starts)
    assert all(times[i + 2] - times[i] >= 0.035 for i in range(len(times) - 2))
    assert [item[1] for item in starts].count(0) == 2
    assert [item[1] for item in starts].count("") == 2
    assert [item[1] for item in starts].count(()) == 2


@pytest.mark.asyncio
async def test_expired_permit_starts_while_prior_body_is_still_running():
    registry = AssetRegistry()
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release_first = asyncio.Event()
    starts = []

    async def body(context: AssetContext):
        starts.append((context.partition_key, time.monotonic()))
        if context.partition_key == "first":
            first_started.set()
            await release_first.wait()
        else:
            second_started.set()

    registry.register(body, partition_def=IdentityPartitions(), resource="api")
    run_task = asyncio.create_task(
        Flow(
            ["body"],
            registry=registry,
            resources={"api": ResourcePolicy(2, RateLimit(1, 0.05))},
        ).run_async({"partition_keys": ["first", "second"], "max_concurrency": 2})
    )
    try:
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await asyncio.wait_for(second_started.wait(), timeout=0.5)
        assert starts[1][1] - starts[0][1] >= 0.04
        assert not release_first.is_set()
    finally:
        release_first.set()
    result = await asyncio.wait_for(run_task, timeout=1)
    assert result.status is FlowStatus.SUCCESS


@pytest.mark.asyncio
async def test_rate_wait_uses_no_invocation_slot_and_resumes_with_no_running_body():
    registry = AssetRegistry()
    first_started = asyncio.Event()
    unrelated_started = asyncio.Event()
    calls = []
    active = 0
    peak_active = 0

    async def limited(context: AssetContext):
        nonlocal active, peak_active
        active += 1
        peak_active = max(peak_active, active)
        calls.append(("limited", context.partition_key))
        if context.partition_key == 0:
            first_started.set()
        await asyncio.sleep(0)
        active -= 1

    async def unrelated():
        nonlocal active, peak_active
        active += 1
        peak_active = max(peak_active, active)
        calls.append(("unrelated", None))
        unrelated_started.set()
        await asyncio.sleep(0)
        active -= 1

    registry.register(limited, partition_def=IdentityPartitions(), resource="api")
    registry.register(unrelated)
    result = await asyncio.wait_for(
        Flow(
            ["limited", "unrelated"],
            registry=registry,
            resources={"api": ResourcePolicy(1, RateLimit(1, 0.04))},
        ).run_async({"partition_keys": [0, ""], "max_concurrency": 1}),
        timeout=2,
    )

    assert first_started.is_set() and unrelated_started.is_set()
    assert calls[:2] == [("limited", 0), ("unrelated", None)]
    assert calls[2] == ("limited", "")
    assert peak_active == 1
    assert result.status is FlowStatus.SUCCESS
    assert [a.attempt.partition_key for a in result.tasks[0].attempts] == [0, ""]


@pytest.mark.asyncio
async def test_failed_invocation_consumes_start_until_window_expires():
    registry = AssetRegistry()
    starts = []

    async def failing(context: AssetContext):
        starts.append((context.partition_key, time.monotonic()))
        if context.partition_key == "first":
            raise RuntimeError("expected failure")

    registry.register(failing, partition_def=IdentityPartitions(), resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["failing"],
            registry=registry,
            resources={"api": ResourcePolicy(1, RateLimit(1, 0.04))},
        ).run_async({"partition_keys": ["first", "second"]}),
        timeout=2,
    )

    assert result.status is FlowStatus.FAILED
    assert [a.status for a in result.tasks[0].attempts] == [
        AttemptStatus.FAILED,
        AttemptStatus.SUCCESS,
    ]
    assert len(starts) == 2
    assert starts[1][1] - starts[0][1] >= 0.035


@pytest.mark.asyncio
async def test_separate_runs_have_independent_rate_state():
    registry = AssetRegistry()
    barrier = asyncio.Event()
    starts = 0

    async def body():
        nonlocal starts
        starts += 1
        if starts == 2:
            barrier.set()
        await asyncio.wait_for(barrier.wait(), timeout=0.5)

    registry.register(body, resource="api")
    resources = {"api": ResourcePolicy(1, RateLimit(1, 1))}
    flows = [Flow(["body"], registry=registry, resources=resources) for _ in range(2)]
    results = await asyncio.wait_for(
        asyncio.gather(*(flow.run_async() for flow in flows)), timeout=1
    )

    assert starts == 2
    assert all(result.status is FlowStatus.SUCCESS for result in results)


@pytest.mark.asyncio
async def test_cancel_during_rate_wait_propagates_without_starting_pending_body():
    registry = AssetRegistry()
    first_started = asyncio.Event()
    calls = []

    async def body(context: AssetContext):
        calls.append(context.partition_key)
        if context.partition_key == "first":
            first_started.set()

    registry.register(body, partition_def=IdentityPartitions(), resource="api")
    flow = Flow(
        ["body"],
        registry=registry,
        resources={"api": ResourcePolicy(1, RateLimit(1, 0.5))},
    )
    running = asyncio.create_task(
        flow.run_async({"partition_keys": ["first", "pending"]})
    )
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await asyncio.sleep(0.02)
    assert calls == ["first"]
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert calls == ["first"]


@pytest.mark.asyncio
async def test_blocked_later_key_does_not_terminalize_an_earlier_waiting_key():
    registry = AssetRegistry()
    release = asyncio.Event()
    calls = []

    async def source(context: AssetContext):
        if context.partition_key == "failed":
            raise RuntimeError("one key failed")
        await release.wait()
        return "ready"

    async def child(source, context: AssetContext):
        calls.append(context.partition_key)
        return source[context.partition_key]

    async def zz_release():
        release.set()

    registry.register(source, partition_def=IdentityPartitions(), resource="api")
    registry.register(child, partition_def=IdentityPartitions())
    registry.register(zz_release)
    result = await asyncio.wait_for(
        Flow(
            ["child", "zz_release"],
            registry=registry,
            resources={"api": ResourcePolicy(2, RateLimit(2, 0.04))},
        ).run_async({"partition_keys": ["waiting", "failed"], "max_concurrency": 2}),
        timeout=1,
    )
    child_result = next(task for task in result.tasks if task.task.task_name == "child")
    assert calls == ["waiting"]
    assert [attempt.status for attempt in child_result.attempts] == [
        AttemptStatus.SUCCESS,
        AttemptStatus.SKIPPED,
    ]
    assert child_result.attempts[1].blocked_by[0].partition_key == "failed"


class MemoryAdapter:
    def __init__(self):
        self.records = {}
        self.values = {}
        self.fail = None
        self.events = []

    async def resolve(self, context):
        self.events.append(("resolve", context.partition_key))
        if self.fail == "resolve":
            raise RuntimeError("resolve failed")
        return self.records.get(context.partition_key)

    async def load(self, record):
        self.events.append(("load", record.partition_key))
        if self.fail == "load":
            raise RuntimeError("load failed")
        return self.values[record.materialization_id]

    async def commit(self, context, output):
        self.events.append(("commit", context.partition_key))
        if self.fail == "commit":
            raise RuntimeError("commit failed")
        record = MaterializationRecord(
            f"id:{context.partition_key}",
            context.asset_name,
            context.version,
            context.partition_domain,
            context.partition_key_present,
            context.partition_key,
            context.input_materialization_ids,
            True,
        )
        self.records[context.partition_key] = record
        self.values[record.materialization_id] = output
        return record


@pytest.mark.asyncio
async def test_materialization_hit_uses_no_start_permit_but_miss_does():
    registry = AssetRegistry()
    adapter = MemoryAdapter()
    starts = []

    async def body(context: AssetContext):
        starts.append(context.partition_key)
        return f"value:{context.partition_key}"

    registry.register(
        body,
        partition_def=IdentityPartitions(),
        resource="api",
        materialization=adapter,
        version="v1",
    )
    record = MaterializationRecord(
        "hit:zero", "body", "v1", IdentityPartitions().domain, True, 0, (), True
    )
    adapter.records[0] = record
    adapter.values[record.materialization_id] = "cached"

    result = await asyncio.wait_for(
        Flow(
            ["body"],
            registry=registry,
            resources={"api": ResourcePolicy(1, RateLimit(1, 0.3))},
        ).run_async({"partition_keys": [0, ""]}),
        timeout=1,
    )

    assert result.status is FlowStatus.SUCCESS
    assert starts == [""]
    assert [attempt.resolution for attempt in result.tasks[0].attempts] == [
        "materialized",
        "executed",
    ]
    assert result.tasks[0].attempts[0].output == "cached"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["resolve", "load", "commit"])
async def test_materialization_failure_does_not_run_body_or_dependent(failure):
    registry = AssetRegistry()
    adapter = MemoryAdapter()
    adapter.fail = failure
    starts = []

    async def body(context: AssetContext):
        starts.append(context.partition_key)
        return "unexpected"

    async def child(body, context: AssetContext):
        starts.append("child")
        return body

    registry.register(
        body,
        partition_def=IdentityPartitions(),
        resource="api",
        materialization=adapter,
        version="v1",
    )
    registry.register(child, partition_def=IdentityPartitions())
    if failure == "load":
        adapter.records[0] = MaterializationRecord(
            "existing", "body", "v1", IdentityPartitions().domain, True, 0, (), True
        )
        adapter.values["existing"] = "cached"

    result = await Flow(
        ["child"],
        registry=registry,
        resources={"api": ResourcePolicy(1, RateLimit(1, 0.05))},
    ).run_async({"partition_keys": [0]})

    assert result.status is FlowStatus.FAILED
    assert result.tasks[0].attempts[0].status is AttemptStatus.FAILED
    assert result.tasks[1].attempts[0].status is AttemptStatus.SKIPPED
    assert result.tasks[1].attempts[0].reason is SkipReason.DEPENDENCY_BLOCKED
    assert starts == ([0] if failure == "commit" else [])


@pytest.mark.asyncio
async def test_materialization_commit_completes_before_downstream_body_starts():
    registry = AssetRegistry()
    events = []

    class RecordingAdapter(MemoryAdapter):
        async def commit(self, context, output):
            events.append("commit begin")
            record = await super().commit(context, output)
            events.append("commit end")
            return record

    adapter = RecordingAdapter()

    async def source(context: AssetContext):
        events.append("source body")
        return "source-value"

    async def child(source, context: AssetContext):
        events.append("child body")
        return source[context.partition_key]

    registry.register(
        source,
        partition_def=IdentityPartitions(),
        materialization=adapter,
        version="v1",
    )
    registry.register(child, partition_def=IdentityPartitions())

    result = await Flow(["child"], registry=registry).run_async({"partition_keys": [0]})

    assert result.status is FlowStatus.SUCCESS
    assert events == ["source body", "commit begin", "commit end", "child body"]
