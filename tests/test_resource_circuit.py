"""Behavioral contracts for explicit, run-local resource circuit breaking."""

import asyncio

import pytest

from kazeflow import OpenCircuit
from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.events import EventKind, validate_event_sequence
from kazeflow.flow import Flow
from kazeflow.materialization import MaterializationRecord
from kazeflow.partition import PartitionDef
from kazeflow.resources import Fail, RateLimit, ResourcePolicy, Retry
from kazeflow.results import AttemptStatus, FlowStatus, SkipReason


class Keys(PartitionDef):
    def range(self, start, end):
        raise AssertionError("range is not used")


class Events:
    def __init__(self, callback=None):
        self.items = []
        self.callback = callback

    def on_event(self, event):
        self.items.append(event)
        if self.callback is not None:
            self.callback(event)


def attempts_by_key(result):
    return {
        attempt.attempt.partition_key: attempt for attempt in result.tasks[0].attempts
    }


@pytest.mark.asyncio
async def test_plain_fail_does_not_open_resource_and_independent_resource_runs():
    registry = AssetRegistry()
    calls = []

    async def ordinary_failure():
        calls.append("failure")
        raise RuntimeError("ordinary")

    async def peer():
        calls.append("peer")
        return "available"

    registry.register(ordinary_failure, resource="api")
    registry.register(peer, resource="api")
    result = await Flow(
        ["ordinary_failure", "peer"],
        registry=registry,
        resources={"api": ResourcePolicy(1, classify_failure=lambda error: Fail())},
    ).run_async()

    assert calls == ["failure", "peer"]
    assert result.status is FlowStatus.FAILED
    assert result.tasks[1].attempts[0].status is AttemptStatus.SUCCESS


@pytest.mark.asyncio
async def test_first_observed_open_wins_running_peer_completes_and_pending_skips():
    registry = AssetRegistry()
    both_started = asyncio.Event()
    release_running = asyncio.Event()
    started = set()
    calls = []
    commit_calls = []
    events = Events()

    async def source(context: AssetContext):
        key = context.partition_key
        calls.append(key)
        started.add(key)
        if len(started) == 2:
            both_started.set()
        if key == "source":
            await both_started.wait()
            raise OSError("resource is unavailable")
        if key == "running":
            await both_started.wait()
            await release_running.wait()
            return "committed"
        raise AssertionError("pending work must never start")

    class RecordingAdapter:
        async def resolve(self, context):
            return None

        async def load(self, record):
            raise AssertionError("no materialization should exist")

        async def commit(self, context, output):
            commit_calls.append((context.partition_key, output))
            return MaterializationRecord(
                f"id:{context.partition_key}",
                context.asset_name,
                context.version,
                context.partition_domain,
                context.partition_key_present,
                context.partition_key,
                context.input_materialization_ids,
                True,
            )

    adapter = RecordingAdapter()
    registry.register(
        source,
        partition_def=Keys(),
        resource="api",
        materialization=adapter,
        version="v1",
    )

    def classifier(error):
        return OpenCircuit()

    run = asyncio.create_task(
        Flow(
            ["source"],
            registry=registry,
            resources={"api": ResourcePolicy(2, classify_failure=classifier)},
        ).run_async(
            {"partition_keys": ["source", "running", "pending"], "max_concurrency": 3},
            event_consumer=events,
        )
    )
    try:
        await asyncio.wait_for(both_started.wait(), timeout=1)
        # The source's failure is observed before the already admitted peer is
        # released. Its opening must not cancel that in-flight operation.
        for _ in range(100):
            if any(
                event.kind is EventKind.ATTEMPT_FINISHED
                and event.attempt is not None
                and event.attempt.partition_key == "source"
                for event in events.items
            ):
                break
            await asyncio.sleep(0)
        else:
            pytest.fail("source did not finish and open the circuit")
        release_running.set()
        result = await asyncio.wait_for(run, timeout=1)
    finally:
        if not run.done():
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

    by_key = attempts_by_key(result)
    assert calls == ["source", "running"]
    assert commit_calls == [("running", "committed")]
    assert by_key["source"].status is AttemptStatus.FAILED
    assert by_key["running"].status is AttemptStatus.SUCCESS
    assert by_key["running"].resolution == "executed"
    assert by_key["pending"].status is AttemptStatus.SKIPPED
    assert by_key["pending"].reason is SkipReason.RESOURCE_CIRCUIT_OPEN
    assert by_key["pending"].blocked_by == (by_key["source"].attempt,)
    assert (
        sum(
            event.kind is EventKind.ATTEMPT_FINISHED
            and event.attempt == by_key["source"].attempt
            for event in events.items
        )
        == 1
    )
    validate_event_sequence(events.items)


@pytest.mark.asyncio
async def test_first_observed_source_wins_when_two_running_calls_open_together():
    registry = AssetRegistry()
    started = asyncio.Event()
    release = {"early": asyncio.Event(), "late": asyncio.Event()}
    classifiers = []
    events = Events()

    # Distinct events let the test force which admitted failure is observed
    # first without relying on scheduler timing.
    started_events = [asyncio.Event(), asyncio.Event()]

    async def controlled(context: AssetContext):
        key = context.partition_key
        if key in {"early", "late"}:
            started_events[0 if key == "early" else 1].set()
            if all(event.is_set() for event in started_events):
                started.set()
            await release[key].wait()
            raise OSError(key)
        raise AssertionError("third call must be skipped")

    registry.register(controlled, partition_def=Keys(), resource="api")

    def classifier(error):
        classifiers.append(str(error))
        return OpenCircuit()

    run = asyncio.create_task(
        Flow(
            ["controlled"],
            registry=registry,
            resources={"api": ResourcePolicy(2, classify_failure=classifier)},
        ).run_async(
            {"partition_keys": ["early", "late", "pending"], "max_concurrency": 3},
            event_consumer=events,
        )
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        release["late"].set()
        source_finished = asyncio.Event()

        def watch_source(event):
            if (
                event.kind is EventKind.ATTEMPT_FINISHED
                and event.attempt is not None
                and event.attempt.partition_key == "late"
            ):
                source_finished.set()

        events.callback = watch_source
        await asyncio.wait_for(source_finished.wait(), timeout=1)
        release["early"].set()
        result = await asyncio.wait_for(run, timeout=1)
    finally:
        if not run.done():
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

    by_key = attempts_by_key(result)
    assert classifiers == ["late", "early"]
    assert by_key["pending"].blocked_by == (by_key["late"].attempt,)
    assert by_key["pending"].reason is SkipReason.RESOURCE_CIRCUIT_OPEN
    validate_event_sequence(events.items)


@pytest.mark.asyncio
async def test_open_circuit_skips_pending_retry_and_keeps_invocation_history():
    registry = AssetRegistry()
    retry_finished = asyncio.Event()
    source_finished = asyncio.Event()
    calls = []

    async def retrying(context: AssetContext):
        calls.append(("retrying", context.invocation_number))
        raise TimeoutError("retry later")

    async def breaker():
        await retry_finished.wait()
        calls.append(("breaker", 1))
        raise OSError("fatal")

    registry.register(retrying, resource="api")
    registry.register(breaker, resource="api")

    def on_event(event):
        if event.kind is EventKind.INVOCATION_FINISHED:
            if event.attempt.task.task_name == "retrying":
                retry_finished.set()
        if (
            event.kind is EventKind.ATTEMPT_FINISHED
            and event.attempt is not None
            and event.attempt.task.task_name == "breaker"
        ):
            source_finished.set()

    events = Events(on_event)

    def classifier(error):
        if isinstance(error, TimeoutError):
            return Retry(after_seconds=30)
        return OpenCircuit()

    run = asyncio.create_task(
        Flow(
            ["retrying", "breaker"],
            registry=registry,
            resources={
                "api": ResourcePolicy(2, max_attempts=3, classify_failure=classifier)
            },
        ).run_async({"max_concurrency": 2}, event_consumer=events)
    )
    try:
        await asyncio.wait_for(retry_finished.wait(), timeout=1)
        await asyncio.wait_for(source_finished.wait(), timeout=1)
        result = await asyncio.wait_for(run, timeout=1)
    finally:
        if not run.done():
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

    task = next(task for task in result.tasks if task.task.task_name == "retrying")
    logical = task.attempts[0]
    source = next(task for task in result.tasks if task.task.task_name == "breaker")
    assert calls == [("retrying", 1), ("breaker", 1)]
    assert logical.status is AttemptStatus.SKIPPED
    assert logical.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
    assert logical.blocked_by == (source.attempts[0].attempt,)
    assert [item.number for item in logical.invocations] == [1]
    assert logical.invocations[0].retry_delay_seconds == 30
    assert sum(call[0] == "retrying" for call in calls) == 1
    validate_event_sequence(events.items)


@pytest.mark.asyncio
async def test_dependency_blocked_reason_precedes_open_circuit_reason():
    registry = AssetRegistry()
    child_calls = []

    async def upstream():
        raise RuntimeError("fatal upstream")

    async def child(upstream):
        child_calls.append("child")

    registry.register(upstream, resource="api")
    registry.register(child, deps=["upstream"], resource="api")
    result = await Flow(
        ["child"],
        registry=registry,
        resources={
            "api": ResourcePolicy(1, classify_failure=lambda error: OpenCircuit())
        },
    ).run_async()

    child_result = next(task for task in result.tasks if task.task.task_name == "child")
    upstream_result = next(
        task for task in result.tasks if task.task.task_name == "upstream"
    )
    assert child_calls == []
    assert child_result.attempts[0].reason is SkipReason.DEPENDENCY_BLOCKED
    assert child_result.attempts[0].blocked_by == (upstream_result.attempts[0].attempt,)


class MemoryAdapter:
    def __init__(self, *, ready=None, failure=None, record=None, value=None):
        self.ready = ready
        self.failure = failure
        self.record = record
        self.value = value
        self.resolves = 0
        self.loads = 0

    async def resolve(self, context):
        self.resolves += 1
        if self.ready is not None:
            await self.ready.wait()
        if self.failure == "resolve":
            raise RuntimeError("resolve failed")
        if self.record is None:
            return None
        return MaterializationRecord(
            self.record.materialization_id,
            context.asset_name,
            context.version,
            context.partition_domain,
            context.partition_key_present,
            context.partition_key,
            context.input_materialization_ids,
            True,
        )

    async def load(self, record):
        self.loads += 1
        if self.failure == "load":
            raise RuntimeError("load failed")
        return self.value

    async def commit(self, context, output):
        raise AssertionError("these tests do not commit")


def materialization_record(key, *, asset_name="cached"):
    return MaterializationRecord(
        f"cached:{key}", asset_name, "v1", Keys().domain, True, key, (), True
    )


@pytest.mark.asyncio
async def test_materialization_hit_bypasses_open_circuit_and_miss_is_skipped():
    for hit in (True, False):
        registry = AssetRegistry()
        opened = asyncio.Event()
        adapter = MemoryAdapter(
            ready=opened,
            record=materialization_record(0) if hit else None,
            value="cached value",
        )
        cached_calls = []

        async def cached(context: AssetContext):
            cached_calls.append(context.partition_key)
            raise AssertionError("a materialization resolution should decide this")

        async def breaker():
            raise OSError("fatal")

        registry.register(
            cached,
            partition_def=Keys(),
            resource="api",
            materialization=adapter,
            version="v1",
        )
        registry.register(breaker, resource="api")

        def classifier(error):
            opened.set()
            return OpenCircuit()

        result = await asyncio.wait_for(
            Flow(
                ["cached", "breaker"],
                registry=registry,
                resources={"api": ResourcePolicy(1, classify_failure=classifier)},
            ).run_async({"partition_keys": [0]}),
            timeout=1,
        )
        cached_task = next(
            task for task in result.tasks if task.task.task_name == "cached"
        )
        attempt = cached_task.attempts[0]
        assert cached_calls == []
        assert opened.is_set()
        if hit:
            assert attempt.status is AttemptStatus.SUCCESS
            assert attempt.resolution == "materialized"
            assert attempt.output == "cached value"
            assert attempt.invocations == ()
            assert adapter.loads == 1
        else:
            assert attempt.status is AttemptStatus.SKIPPED
            assert attempt.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
            assert attempt.invocations == ()
            assert adapter.loads == 0


@pytest.mark.asyncio
async def test_materialization_preparation_can_start_after_circuit_opened():
    for hit in (True, False):
        registry = AssetRegistry()
        source_finished = asyncio.Event()
        adapter = MemoryAdapter(
            record=materialization_record(0) if hit else None,
            value="loaded after opening",
        )
        source_events = Events(
            lambda event: source_finished.set()
            if event.kind is EventKind.ATTEMPT_FINISHED
            and event.attempt is not None
            and event.attempt.task.task_name == "breaker"
            else None
        )
        cached_calls = []

        async def breaker():
            raise OSError("open circuit first")

        async def gate():
            await source_finished.wait()
            return "gate value"

        class GateAdapter:
            async def resolve(self, context):
                return None

            async def load(self, record):
                raise AssertionError("gate has no existing materialization")

            async def commit(self, context, output):
                return MaterializationRecord(
                    "gate:0",
                    context.asset_name,
                    context.version,
                    context.partition_domain,
                    context.partition_key_present,
                    context.partition_key,
                    context.input_materialization_ids,
                    True,
                )

        async def cached(gate, context: AssetContext):
            cached_calls.append(context.partition_key)
            return "must not execute"

        registry.register(breaker, resource="api")
        registry.register(
            gate,
            partition_def=Keys(),
            materialization=GateAdapter(),
            version="v1",
        )
        registry.register(
            cached,
            partition_def=Keys(),
            resource="api",
            materialization=adapter,
            version="v1",
        )

        result = await asyncio.wait_for(
            Flow(
                ["breaker", "cached"],
                registry=registry,
                resources={
                    "api": ResourcePolicy(
                        1, classify_failure=lambda error: OpenCircuit()
                    )
                },
            ).run_async({"partition_keys": [0]}, event_consumer=source_events),
            timeout=1,
        )
        cached_task = next(
            task for task in result.tasks if task.task.task_name == "cached"
        )
        attempt = cached_task.attempts[0]
        assert source_finished.is_set()
        assert adapter.resolves == 1
        assert cached_calls == []
        if hit:
            assert attempt.status is AttemptStatus.SUCCESS
            assert attempt.resolution == "materialized"
            assert attempt.output == "loaded after opening"
            assert attempt.invocations == ()
            assert adapter.loads == 1
        else:
            assert attempt.status is AttemptStatus.SKIPPED
            assert attempt.reason is SkipReason.RESOURCE_CIRCUIT_OPEN
            assert attempt.invocations == ()
            assert adapter.loads == 0


@pytest.mark.asyncio
async def test_materialization_preparation_failure_does_not_open_circuit():
    registry = AssetRegistry()
    adapter = MemoryAdapter(failure="resolve")
    classifier_calls = []
    peer_calls = []

    async def prepared(context: AssetContext):
        raise AssertionError("failed preparation must not invoke body")

    async def peer():
        peer_calls.append("peer")

    registry.register(
        prepared,
        partition_def=Keys(),
        resource="api",
        materialization=adapter,
        version="v1",
    )
    registry.register(peer, resource="api")

    def classifier(error):
        classifier_calls.append(error)
        return OpenCircuit()

    result = await Flow(
        ["prepared", "peer"],
        registry=registry,
        resources={"api": ResourcePolicy(1, classify_failure=classifier)},
    ).run_async({"partition_keys": [0]})

    prepared_task = next(
        task for task in result.tasks if task.task.task_name == "prepared"
    )
    assert prepared_task.attempts[0].status is AttemptStatus.FAILED
    assert classifier_calls == []
    assert peer_calls == ["peer"]


@pytest.mark.asyncio
async def test_circuit_state_is_fresh_for_each_run():
    registry = AssetRegistry()
    calls = []

    async def breaker():
        calls.append("breaker")
        if calls.count("breaker") == 1:
            raise RuntimeError("open only first run")

    async def peer():
        calls.append("peer")

    registry.register(breaker, resource="api")
    registry.register(peer, resource="api")
    flow = Flow(
        ["breaker", "peer"],
        registry=registry,
        resources={
            "api": ResourcePolicy(1, classify_failure=lambda error: OpenCircuit())
        },
    )

    first = await flow.run_async()
    second = await flow.run_async()

    assert first.status is FlowStatus.FAILED
    assert first.tasks[1].attempts[0].status is AttemptStatus.SKIPPED
    assert second.status is FlowStatus.SUCCESS
    assert second.tasks[1].attempts[0].status is AttemptStatus.SUCCESS
    assert calls == ["breaker", "breaker", "peer"]


@pytest.mark.asyncio
async def test_falsey_partition_key_is_preserved_as_the_circuit_source():
    registry = AssetRegistry()
    calls = []

    async def work(context: AssetContext):
        calls.append(context.partition_key)
        if context.partition_key == 0:
            raise RuntimeError("zero is the source")
        raise AssertionError("later falsey keys should be skipped")

    registry.register(work, partition_def=Keys(), resource="api")
    result = await Flow(
        ["work"],
        registry=registry,
        resources={
            "api": ResourcePolicy(1, classify_failure=lambda error: OpenCircuit())
        },
    ).run_async({"partition_keys": [0, "", ()]})

    assert calls == [0]
    attempts = result.tasks[0].attempts
    assert [item.attempt.partition_key for item in attempts] == [0, "", ()]
    assert all(item.status is AttemptStatus.SKIPPED for item in attempts[1:])
    assert all(item.reason is SkipReason.RESOURCE_CIRCUIT_OPEN for item in attempts[1:])
    assert all(item.blocked_by == (attempts[0].attempt,) for item in attempts[1:])


@pytest.mark.asyncio
async def test_partition_aggregate_preserves_mixed_dependency_and_circuit_blocks():
    registry = AssetRegistry()
    both_started = asyncio.Event()
    source_finished = asyncio.Event()
    started_keys = set()
    events = Events()
    child_calls = []

    async def source(context: AssetContext):
        key = context.partition_key
        started_keys.add(key)
        if len(started_keys) == 2:
            both_started.set()
        if key == "blocked":
            await both_started.wait()
            raise OSError("open the shared resource")
        await both_started.wait()
        await source_finished.wait()
        return "source value"

    def observe(event):
        if (
            event.kind is EventKind.ATTEMPT_FINISHED
            and event.attempt is not None
            and event.attempt.task.task_name == "source"
            and event.attempt.partition_key == "blocked"
        ):
            source_finished.set()

    events.callback = observe

    async def child(source, context: AssetContext):
        child_calls.append(context.partition_key)
        return source

    registry.register(source, partition_def=Keys(), resource="api")
    registry.register(child, partition_def=Keys(), resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["child"],
            registry=registry,
            resources={
                "api": ResourcePolicy(2, classify_failure=lambda error: OpenCircuit())
            },
        ).run_async(
            {"partition_keys": ["blocked", "ready"], "max_concurrency": 2},
            event_consumer=events,
        ),
        timeout=1,
    )

    source_result = next(
        task for task in result.tasks if task.task.task_name == "source"
    )
    child_result = next(task for task in result.tasks if task.task.task_name == "child")
    by_key = {
        attempt.attempt.partition_key: attempt for attempt in child_result.attempts
    }
    assert child_calls == []
    assert by_key["blocked"].reason is SkipReason.DEPENDENCY_BLOCKED
    assert by_key["ready"].reason is SkipReason.RESOURCE_CIRCUIT_OPEN
    assert by_key["ready"].blocked_by == (source_result.attempts[0].attempt,)
    assert child_result.status is AttemptStatus.SKIPPED
    assert child_result.reason is SkipReason.MIXED_BLOCKED
    assert child_result.blocked_by == (
        by_key["blocked"].blocked_by[0],
        by_key["ready"].blocked_by[0],
    )
    assert any(
        event.kind is EventKind.TASK_FINISHED
        and event.task == child_result.task
        and event.reason is SkipReason.MIXED_BLOCKED
        for event in events.items
    )
    validate_event_sequence(events.items)


@pytest.mark.asyncio
async def test_circuit_skips_capacity_and_rate_waiters_without_waiting_for_deadline():
    for max_concurrency, rate_limit in ((1, None), (2, RateLimit(1, 30))):
        registry = AssetRegistry()
        first_started = asyncio.Event()
        release_source = asyncio.Event()
        calls = []

        async def source(context: AssetContext):
            calls.append(context.partition_key)
            first_started.set()
            await release_source.wait()
            raise RuntimeError("open circuit")

        registry.register(source, partition_def=Keys(), resource="api")
        flow = Flow(
            ["source"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    max_concurrency,
                    rate_limit=rate_limit,
                    classify_failure=lambda error: OpenCircuit(),
                )
            },
        )
        run = asyncio.create_task(
            flow.run_async(
                {"partition_keys": ["source", "waiting"], "max_concurrency": 2}
            )
        )
        try:
            await asyncio.wait_for(first_started.wait(), timeout=1)
            await asyncio.sleep(0)
            release_source.set()
            result = await asyncio.wait_for(run, timeout=0.2)
        finally:
            if not run.done():
                run.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await run
        attempts = result.tasks[0].attempts
        assert calls == ["source"]
        assert attempts[1].status is AttemptStatus.SKIPPED
        assert attempts[1].reason is SkipReason.RESOURCE_CIRCUIT_OPEN


def test_package_exports_public_open_circuit_action():
    from kazeflow.resources import OpenCircuit as ResourceOpenCircuit

    assert OpenCircuit is ResourceOpenCircuit
