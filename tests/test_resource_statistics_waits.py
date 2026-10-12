"""Behavioral coverage for run-local resource accounting and waits."""

import asyncio

import pytest

from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.flow import Flow
from kazeflow.results import AttemptStatus, FlowStatus, SkipReason
from kazeflow.resources import OpenCircuit, RateLimit, ResourcePolicy, Retry


@pytest.mark.asyncio
async def test_concurrency_wait_is_counted_once_and_keeps_global_progress() -> None:
    registry = AssetRegistry()
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    unrelated_started = asyncio.Event()
    active = 0
    peak = 0
    calls: list[str] = []

    async def first() -> None:
        nonlocal active, peak
        calls.append("first")
        active += 1
        peak = max(peak, active)
        first_started.set()
        await release_first.wait()
        active -= 1

    async def waiter() -> None:
        nonlocal active, peak
        calls.append("waiter")
        active += 1
        peak = max(peak, active)
        active -= 1

    async def unrelated() -> None:
        calls.append("unrelated")
        unrelated_started.set()

    registry.register(first, resource="api")
    registry.register(waiter, resource="api")
    registry.register(unrelated)
    run = asyncio.create_task(
        Flow(
            ["first", "waiter", "unrelated"],
            registry=registry,
            resources={"api": ResourcePolicy(1)},
        ).run_async({"max_concurrency": 2})
    )
    try:
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await asyncio.wait_for(unrelated_started.wait(), timeout=1)
        # Keep the admission denied across repeated scheduler checks.
        await asyncio.sleep(0.01)
        release_first.set()
        result = await asyncio.wait_for(run, timeout=1)
    finally:
        release_first.set()
        if not run.done():
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

    stats = result.resource_statistics("api")
    assert calls.count("waiter") == 1
    assert calls.index("unrelated") < calls.index("waiter")
    assert peak == 1
    assert stats.invocations == stats.succeeded == 2
    assert stats.wait_count == 1
    assert stats.wait_duration.total_seconds() > 0


@pytest.mark.asyncio
async def test_rate_limit_admission_delay_is_an_observed_wait() -> None:
    registry = AssetRegistry()
    calls: list[str] = []

    async def first() -> None:
        calls.append("first")

    async def second() -> None:
        calls.append("second")

    registry.register(first, resource="api")
    registry.register(second, resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["first", "second"],
            registry=registry,
            resources={"api": ResourcePolicy(2, RateLimit(1, 0.04))},
        ).run_async({"max_concurrency": 2}),
        timeout=1,
    )

    stats = result.resource_statistics("api")
    assert calls == ["first", "second"]
    assert stats.invocations == stats.succeeded == 2
    assert stats.wait_count == 1
    assert stats.wait_duration.total_seconds() > 0


@pytest.mark.asyncio
async def test_retry_delay_and_shared_cooldown_are_counted_per_waiting_operation() -> (
    None
):
    registry = AssetRegistry()
    calls: list[tuple[str, int | None]] = []

    async def flaky(context: AssetContext) -> None:
        calls.append(("flaky", context.invocation_number))
        if context.invocation_number == 1:
            raise TimeoutError("retry once")

    async def peer() -> None:
        calls.append(("peer", None))

    def classify(error: Exception) -> Retry:
        return Retry(after_seconds=0.04, cooldown_resource=True)

    registry.register(flaky, resource="api")
    registry.register(peer, resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["flaky", "peer"],
            registry=registry,
            resources={
                "api": ResourcePolicy(1, max_attempts=2, classify_failure=classify)
            },
        ).run_async({"max_concurrency": 2}),
        timeout=1,
    )

    stats = result.resource_statistics("api")
    assert calls.count(("flaky", 1)) == calls.count(("flaky", 2)) == 1
    assert calls.count(("peer", None)) == 1
    assert stats.invocations == 3
    assert stats.succeeded == 2
    assert stats.failed == 1
    assert stats.retries == 1
    # The retry and the peer each have one observed cooldown admission episode.
    assert stats.wait_count == 2
    assert stats.wait_duration.total_seconds() > 0
    assert stats.invocation_duration.total_seconds() > 0


@pytest.mark.asyncio
async def test_circuit_skip_closes_existing_wait_without_body_or_retry() -> None:
    registry = AssetRegistry()
    source_started = asyncio.Event()
    release_source = asyncio.Event()
    calls: list[str] = []

    async def source() -> None:
        calls.append("source")
        source_started.set()
        await release_source.wait()
        raise RuntimeError("open circuit")

    async def waiting() -> None:
        calls.append("waiting")

    registry.register(source, resource="api")
    registry.register(waiting, resource="api")
    run = asyncio.create_task(
        Flow(
            ["source", "waiting"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    1,
                    classify_failure=lambda error: OpenCircuit(),
                )
            },
        ).run_async({"max_concurrency": 2})
    )
    try:
        await asyncio.wait_for(source_started.wait(), timeout=1)
        await asyncio.sleep(0.01)
        release_source.set()
        result = await asyncio.wait_for(run, timeout=1)
    finally:
        release_source.set()
        if not run.done():
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

    stats = result.resource_statistics("api")
    waiting_attempt = result.tasks[1].attempts[0]
    assert result.status is FlowStatus.FAILED
    assert calls == ["source"]
    assert waiting_attempt.status is AttemptStatus.SKIPPED
    assert waiting_attempt.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
    assert stats.invocations == 1
    assert stats.failed == 1
    assert stats.retries == 0
    assert stats.wait_count == 1
    assert stats.wait_duration.total_seconds() > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_kind", ["dependency", "global"])
async def test_non_resource_queue_delays_do_not_start_observed_wait(
    wait_kind: str,
) -> None:
    registry = AssetRegistry()
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()
    calls: list[str] = []

    async def blocker() -> None:
        calls.append("blocker")
        blocker_started.set()
        await release_blocker.wait()

    async def selected() -> None:
        calls.append("selected")

    if wait_kind == "dependency":

        async def selected_child(blocker) -> None:
            calls.append("selected_child")
            return None

        registry.register(blocker)
        registry.register(selected_child, resource="api")
        targets = ["selected_child"]
    else:
        registry.register(blocker)
        registry.register(selected, resource="api")
        targets = ["blocker", "selected"]

    run = asyncio.create_task(
        Flow(
            targets,
            registry=registry,
            resources={"api": ResourcePolicy(1)},
        ).run_async({"max_concurrency": 1})
    )
    try:
        await asyncio.wait_for(blocker_started.wait(), timeout=1)
        await asyncio.sleep(0.01)
        release_blocker.set()
        result = await asyncio.wait_for(run, timeout=1)
    finally:
        release_blocker.set()
        if not run.done():
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

    stats = result.resource_statistics("api")
    assert calls.count("selected") + calls.count("selected_child") == 1
    assert stats.invocations == stats.succeeded == 1
    assert stats.wait_count == 0
    assert stats.wait_duration.total_seconds() == 0
