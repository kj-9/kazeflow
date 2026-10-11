import asyncio
from dataclasses import FrozenInstanceError

import pytest

from kazeflow.assets import AssetRegistry
from kazeflow.flow import Flow
from kazeflow.partition import PartitionDef
from kazeflow.plan import ResourcePlan
from kazeflow.resources import ResourcePolicy


def test_policy_and_plan_are_immutable_and_selected_only() -> None:
    registry = AssetRegistry()
    called = False

    def shared() -> None:
        nonlocal called
        called = True

    registry.register(shared, resource="api")
    registry.register(lambda: None, resource="undeclared_unused")
    policies = {"api": ResourcePolicy(2), "unused": ResourcePolicy(4)}
    flow = Flow(["shared"], registry=registry, resources=policies)
    policies.clear()

    plan = flow.plan()
    assert plan.resources == (ResourcePlan("api", 2),)
    assert plan.tasks[0].resource_name == "api"
    assert called is False
    with pytest.raises(FrozenInstanceError):
        setattr(plan.resources[0], "max_concurrency", 9)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_resource_policy_rejects_invalid_limits(limit: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        ResourcePolicy(limit)  # type: ignore[arg-type]


def test_planning_rechecks_malformed_resource_policy_instances() -> None:
    registry = AssetRegistry()
    registry.register(lambda: None, resource="api")
    policy = object.__new__(ResourcePolicy)
    object.__setattr__(policy, "max_concurrency", True)

    with pytest.raises(ValueError, match="positive integer"):
        Flow(["<lambda>"], registry=registry, resources={"api": policy}).plan()


def test_missing_resource_reference_fails_preflight_but_unselected_does_not() -> None:
    registry = AssetRegistry()
    invoked: list[str] = []

    def selected() -> None:
        invoked.append("selected")

    def unused() -> None:
        invoked.append("unused")

    registry.register(selected, resource="absent")
    registry.register(unused, resource="also_absent")
    flow = Flow(["selected"], registry=registry, resources={})
    with pytest.raises(ValueError, match="undeclared resource 'absent'"):
        flow.plan()
    assert invoked == []

    registry = AssetRegistry()
    registry.register(selected)
    registry.register(unused, resource="absent")
    assert Flow(["selected"], registry=registry).plan().resources == ()


@pytest.mark.asyncio
async def test_resource_saturation_does_not_consume_global_slot() -> None:
    registry = AssetRegistry()
    release = asyncio.Event()
    unrelated_started = asyncio.Event()
    active = 0
    peak = 0
    calls: list[str] = []

    async def api_one() -> None:
        nonlocal active, peak
        calls.append("api_one")
        active += 1
        peak = max(peak, active)
        await release.wait()
        active -= 1

    async def api_two() -> None:
        nonlocal active, peak
        calls.append("api_two")
        active += 1
        peak = max(peak, active)
        active -= 1

    async def unrelated() -> None:
        calls.append("unrelated")
        unrelated_started.set()
        release.set()

    registry.register(api_one, resource="api")
    registry.register(api_two, resource="api")
    registry.register(unrelated)
    flow = Flow(
        ["api_one", "api_two", "unrelated"],
        registry=registry,
        resources={"api": ResourcePolicy(1)},
    )
    result = await asyncio.wait_for(flow.run_async({"max_concurrency": 2}), timeout=1)

    assert unrelated_started.is_set()
    assert peak == 1
    assert calls.count("api_one") == calls.count("api_two") == 1
    assert len(result.tasks) == 3


class IdentityPartitions(PartitionDef):
    def range(self, start: object, end: object) -> list[object]:
        raise AssertionError("range not used")


@pytest.mark.asyncio
async def test_resource_limit_is_shared_across_assets_and_falsey_partitions() -> None:
    registry = AssetRegistry()
    active = 0
    peak = 0
    calls: list[tuple[str, object]] = []

    async def alpha(context) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        calls.append(("alpha", context.partition_key))
        await asyncio.sleep(0)
        active -= 1

    async def beta(context) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        calls.append(("beta", context.partition_key))
        await asyncio.sleep(0)
        active -= 1

    registry.register(alpha, partition_def=IdentityPartitions(), resource="shared")
    registry.register(beta, partition_def=IdentityPartitions(), resource="shared")
    result = await Flow(
        ["alpha", "beta"],
        registry=registry,
        resources={"shared": ResourcePolicy(1)},
    ).run_async({"partition_keys": [0, "", 1], "max_concurrency": 4})

    assert peak == 1
    assert calls == [
        ("alpha", 0),
        ("alpha", ""),
        ("alpha", 1),
        ("beta", 0),
        ("beta", ""),
        ("beta", 1),
    ]
    assert [task.task.task_name for task in result.tasks] == ["alpha", "beta"]
    assert all(len(task.attempts) == 3 for task in result.tasks)
    assert [
        attempt.attempt.partition_key
        for task in result.tasks
        for attempt in task.attempts
    ] == [0, "", 1, 0, "", 1]
    assert len(calls) == len(set(calls)) == 6


@pytest.mark.asyncio
async def test_separate_runs_do_not_coordinate_resource_capacity() -> None:
    registry = AssetRegistry()
    both_started = asyncio.Event()
    started = 0

    async def invoke() -> None:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=1)

    registry.register(invoke, resource="shared")
    resources = {"shared": ResourcePolicy(1)}
    first = Flow(["invoke"], registry=registry, resources=resources)
    second = Flow(["invoke"], registry=registry, resources=resources)

    results = await asyncio.wait_for(
        asyncio.gather(first.run_async(), second.run_async()), timeout=2
    )

    assert started == 2
    assert len(results) == 2
    assert all(
        result.tasks[0].attempts[0].status.value == "success" for result in results
    )


@pytest.mark.asyncio
async def test_failed_resource_attempt_releases_capacity_and_cancellation_propagates() -> (
    None
):
    registry = AssetRegistry()
    calls: list[str] = []

    async def failure() -> None:
        calls.append("failure")
        raise RuntimeError("expected")

    async def later() -> None:
        calls.append("later")

    registry.register(failure, resource="api")
    registry.register(later, resource="api")
    result = await Flow(
        ["failure", "later"],
        registry=registry,
        resources={"api": ResourcePolicy(1)},
    ).run_async()
    assert calls == ["failure", "later"]
    assert [task.status.value for task in result.tasks] == ["failed", "success"]

    started = asyncio.Event()
    cancelled: list[str] = []
    registry = AssetRegistry()

    async def blocking() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append("blocking")

    registry.register(blocking, resource="api")
    flow = Flow(["blocking"], registry=registry, resources={"api": ResourcePolicy(1)})
    run = asyncio.create_task(flow.run_async())
    await started.wait()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert cancelled == ["blocking"]
