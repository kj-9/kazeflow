"""Behavior contracts for explicit resource retry and shared cooldown."""

import asyncio
import math
import time

import pytest

from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.events import EventKind
from kazeflow.flow import Flow
from kazeflow.materialization import MaterializationRecord
from kazeflow.partition import PartitionDef
from kazeflow.resources import Fail, RateLimit, ResourcePolicy, Retry
from kazeflow.results import AttemptStatus, FlowStatus


class Keys(PartitionDef):
    def range(self, start, end):
        raise AssertionError("range is not used")


class Events:
    def __init__(self):
        self.items = []

    def on_event(self, event):
        self.items.append(event)


@pytest.mark.parametrize("max_attempts", [0, -1, True, 1.5, "2"])
def test_resource_policy_rejects_invalid_retry_budgets(max_attempts):
    with pytest.raises(ValueError):
        ResourcePolicy(1, max_attempts=max_attempts)


@pytest.mark.parametrize(
    "seconds", [-1, True, math.inf, -math.inf, math.nan, "1", object()]
)
def test_retry_rejects_invalid_delay(seconds):
    with pytest.raises((TypeError, ValueError)):
        Retry(after_seconds=seconds)


@pytest.mark.parametrize("cooldown", [0, 1, "yes", None])
def test_retry_requires_explicit_boolean_cooldown(cooldown):
    with pytest.raises((TypeError, ValueError)):
        Retry(cooldown_resource=cooldown)


def test_policy_plan_exposes_retry_metadata_without_calling_classifier():
    registry = AssetRegistry()
    classified = []

    def body():
        pass

    def classifier(error):
        classified.append(error)
        return Fail()

    registry.register(body, resource="api")
    registry.register(lambda: None, resource="unused")
    plan = Flow(
        ["body"],
        registry=registry,
        resources={
            "api": ResourcePolicy(2, max_attempts=3, classify_failure=classifier),
            "unused": ResourcePolicy(1, max_attempts=8),
        },
    ).plan()

    assert [
        (item.name, item.max_attempts, item.classifier_enabled)
        for item in plan.resources
    ] == [("api", 3, True)]
    assert classified == []


@pytest.mark.asyncio
async def test_transient_body_failure_retries_and_keeps_one_logical_result():
    registry = AssetRegistry()
    calls = []
    events = Events()

    async def flaky(context: AssetContext):
        calls.append(context.invocation_number)
        if len(calls) == 1:
            raise TimeoutError("temporary")
        return "ready"

    registry.register(flaky, resource="api")
    result = await Flow(
        ["flaky"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1,
                max_attempts=2,
                classify_failure=lambda error: Retry(after_seconds=0.005),
            )
        },
    ).run_async(event_consumer=events)

    logical = result.tasks[0].attempts[0]
    assert result.status is FlowStatus.SUCCESS
    assert logical.status is AttemptStatus.SUCCESS
    assert logical.output == "ready"
    assert calls == [1, 2]
    assert [item.number for item in logical.invocations] == [1, 2]
    assert [item.status for item in logical.invocations] == [
        AttemptStatus.FAILED,
        AttemptStatus.SUCCESS,
    ]
    assert logical.invocations[0].failure is not None
    assert logical.invocations[0].retry_delay_seconds == pytest.approx(0.005)
    attempt_ref = logical.attempt
    invocation_events = [
        event
        for event in events.items
        if event.kind in {EventKind.INVOCATION_STARTED, EventKind.INVOCATION_FINISHED}
    ]
    assert [(event.kind, event.invocation_number) for event in invocation_events] == [
        (EventKind.INVOCATION_STARTED, 1),
        (EventKind.INVOCATION_FINISHED, 1),
        (EventKind.INVOCATION_STARTED, 2),
        (EventKind.INVOCATION_FINISHED, 2),
    ]
    assert all(event.attempt == attempt_ref for event in invocation_events)


@pytest.mark.asyncio
async def test_every_retry_reacquires_rolling_rate_permit():
    registry = AssetRegistry()
    starts = []

    async def flaky(context: AssetContext):
        starts.append(time.monotonic())
        if context.invocation_number == 1:
            raise TimeoutError("temporary")

    registry.register(flaky, resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["flaky"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    1,
                    RateLimit(1, 0.04),
                    max_attempts=2,
                    classify_failure=lambda error: Retry(),
                )
            },
        ).run_async(),
        timeout=1,
    )

    assert result.status is FlowStatus.SUCCESS
    assert len(starts) == 2
    assert starts[1] - starts[0] >= 0.03


@pytest.mark.asyncio
async def test_fail_action_and_exhausted_retry_each_finish_once():
    for action, budget, expected_calls in [
        (Fail(), 3, 1),
        (Retry(), 2, 2),
    ]:
        registry = AssetRegistry()
        calls = []
        events = Events()

        async def broken(context: AssetContext):
            calls.append(context.invocation_number)
            raise OSError("unavailable")

        registry.register(broken, resource="api")
        result = await Flow(
            ["broken"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    1,
                    max_attempts=budget,
                    classify_failure=lambda error, action=action: action,
                )
            },
        ).run_async(event_consumer=events)
        logical = result.tasks[0].attempts[0]
        assert result.status is FlowStatus.FAILED
        assert calls == list(range(1, expected_calls + 1))
        assert logical.status is AttemptStatus.FAILED
        assert len(logical.invocations) == expected_calls
        assert (
            sum(event.kind is EventKind.ATTEMPT_FINISHED for event in events.items) == 1
        )


@pytest.mark.asyncio
async def test_default_policy_does_not_retry_or_emit_invocation_events():
    registry = AssetRegistry()
    events = Events()
    calls = []

    async def broken():
        calls.append(1)
        raise RuntimeError("ordinary failure")

    registry.register(broken, resource="api")
    result = await Flow(
        ["broken"], registry=registry, resources={"api": ResourcePolicy(1)}
    ).run_async(event_consumer=events)

    assert result.status is FlowStatus.FAILED
    assert calls == [1]
    assert result.tasks[0].attempts[0].invocations == ()
    assert not any(
        event.kind in {EventKind.INVOCATION_STARTED, EventKind.INVOCATION_FINISHED}
        for event in events.items
    )


@pytest.mark.asyncio
async def test_retry_budget_without_classifier_does_not_retry_automatically():
    registry = AssetRegistry()
    calls = []

    async def broken():
        calls.append(1)
        raise TimeoutError("unclassified")

    registry.register(broken, resource="api")
    result = await Flow(
        ["broken"],
        registry=registry,
        resources={"api": ResourcePolicy(1, max_attempts=4)},
    ).run_async()

    assert result.status is FlowStatus.FAILED
    assert calls == [1]
    assert len(result.tasks[0].attempts[0].invocations) == 1


@pytest.mark.asyncio
async def test_classifier_error_or_invalid_value_fails_without_retry():
    for classify in [
        lambda error: "retry",
        lambda error: (_ for _ in ()).throw(RuntimeError("classifier failed")),
    ]:
        registry = AssetRegistry()
        calls = []

        async def broken():
            calls.append(1)
            raise ValueError("body failed")

        registry.register(broken, resource="api")
        result = await Flow(
            ["broken"],
            registry=registry,
            resources={
                "api": ResourcePolicy(1, max_attempts=4, classify_failure=classify)
            },
        ).run_async()
        assert result.status is FlowStatus.FAILED
        assert calls == [1]
        assert result.tasks[0].attempts[0].status is AttemptStatus.FAILED
        assert len(result.tasks[0].attempts[0].invocations) == 1


@pytest.mark.asyncio
async def test_falsey_partition_keys_keep_selection_order_while_retrying_one_key():
    registry = AssetRegistry()
    calls = []

    async def flaky(context: AssetContext):
        calls.append((context.partition_key, context.invocation_number))
        if context.partition_key == "" and context.invocation_number == 1:
            raise OSError("temporary")
        return context.partition_key

    registry.register(flaky, partition_def=Keys(), resource="api")
    result = await Flow(
        ["flaky"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                2,
                max_attempts=2,
                classify_failure=lambda error: Retry(),
            )
        },
    ).run_async({"partition_keys": [0, "", ()], "max_concurrency": 3})

    attempts = result.tasks[0].attempts
    assert [item.attempt.partition_key for item in attempts] == [0, "", ()]
    assert calls.count((0, 1)) == 1
    assert calls.count(("", 1)) == 1
    assert calls.count(("", 2)) == 1
    assert calls.count(((), 1)) == 1


@pytest.mark.asyncio
async def test_backoff_releases_global_slot_for_unrelated_ready_work():
    registry = AssetRegistry()
    unrelated_started = asyncio.Event()
    calls = []

    async def flaky(context: AssetContext):
        calls.append(context.invocation_number)
        if context.invocation_number == 1:
            raise TimeoutError("once")

    async def independent():
        calls.append("independent")
        unrelated_started.set()

    registry.register(flaky, resource="api")
    registry.register(independent)
    result = await asyncio.wait_for(
        Flow(
            ["flaky", "independent"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    1,
                    max_attempts=2,
                    classify_failure=lambda error: Retry(after_seconds=0.05),
                )
            },
        ).run_async({"max_concurrency": 1}),
        timeout=1,
    )

    assert unrelated_started.is_set()
    assert calls.index("independent") < calls.index(2)
    assert result.status is FlowStatus.SUCCESS


@pytest.mark.asyncio
async def test_shared_cooldown_delays_new_same_resource_body_starts():
    registry = AssetRegistry()
    events = Events()
    started = []

    async def flaky(context: AssetContext):
        started.append(("flaky", context.invocation_number, time.monotonic()))
        if context.invocation_number == 1:
            raise ConnectionError("temporary")

    async def peer(context: AssetContext):
        started.append(("peer", context.invocation_number, time.monotonic()))

    registry.register(flaky, resource="api")
    registry.register(peer, resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["flaky", "peer"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    1,
                    max_attempts=2,
                    classify_failure=lambda error: Retry(
                        after_seconds=0.06,
                        cooldown_resource=True,
                    ),
                )
            },
        ).run_async({"max_concurrency": 2}, event_consumer=events),
        timeout=1,
    )

    assert result.status is FlowStatus.SUCCESS
    first = next(item for item in started if item[0] == "flaky" and item[1] == 1)
    retry = next(item for item in started if item[0] == "flaky" and item[1] == 2)
    peer_start = next(item for item in started if item[0] == "peer")
    # Both newly admitted starts must wait for the shared cooldown requested by
    # invocation 1.
    assert retry[2] - first[2] >= 0.045
    assert peer_start[2] - first[2] >= 0.045
    finished = next(
        event
        for event in events.items
        if event.kind is EventKind.INVOCATION_FINISHED
        and event.invocation_number == 1
        and event.failure is not None
    )
    assert finished.cooldown_seconds == pytest.approx(0.06)


@pytest.mark.asyncio
async def test_later_cooldown_request_extends_the_shared_resource_deadline():
    registry = AssetRegistry()
    both_started = asyncio.Event()
    started_count = 0
    requested = []
    peer_started = []

    async def failing(context: AssetContext):
        nonlocal started_count
        started_count += 1
        if started_count == 2:
            both_started.set()
        await both_started.wait()
        if context.partition_key == "later":
            await asyncio.sleep(0.015)
        raise TimeoutError(str(context.partition_key))

    async def peer():
        peer_started.append(time.monotonic())

    def classify(error):
        delay = 0.07 if str(error) == "later" else 0.025
        requested.append((time.monotonic(), delay))
        return Retry(after_seconds=delay, cooldown_resource=True)

    registry.register(failing, partition_def=Keys(), resource="api")
    registry.register(peer, resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["failing", "peer"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    2,
                    max_attempts=1,
                    classify_failure=classify,
                )
            },
        ).run_async({"partition_keys": ["first", "later"], "max_concurrency": 3}),
        timeout=1,
    )

    assert result.status is FlowStatus.FAILED
    assert len(requested) == 2
    assert requested[1][0] > requested[0][0]
    assert peer_started[0] >= requested[1][0] + requested[1][1] - 0.01


@pytest.mark.asyncio
async def test_retry_cancellation_while_waiting_has_no_synthetic_terminal_event():
    registry = AssetRegistry()
    events = Events()
    first_finished = asyncio.Event()

    async def broken():
        raise TimeoutError("retry after waiting")

    class WaitingEvents(Events):
        def on_event(self, event):
            super().on_event(event)
            if (
                event.kind is EventKind.INVOCATION_FINISHED
                and event.invocation_number == 1
            ):
                first_finished.set()

    events = WaitingEvents()
    registry.register(broken, resource="api")
    run = asyncio.create_task(
        Flow(
            ["broken"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    1,
                    max_attempts=2,
                    classify_failure=lambda error: Retry(after_seconds=1),
                )
            },
        ).run_async(event_consumer=events)
    )
    await asyncio.wait_for(first_finished.wait(), timeout=0.5)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert not any(event.kind is EventKind.ATTEMPT_FINISHED for event in events.items)
    assert not any(
        event.kind is EventKind.FLOW_FINISHED
        and getattr(event.status, "value", None) == "cancelled"
        for event in events.items
    )


@pytest.mark.asyncio
async def test_retry_and_cooldown_state_are_fresh_for_each_run():
    registry = AssetRegistry()
    calls = []

    async def flaky(context: AssetContext):
        calls.append(context.invocation_number)
        if context.invocation_number == 1:
            raise TimeoutError("once per run")

    registry.register(flaky, resource="api")
    flow = Flow(
        ["flaky"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1,
                max_attempts=2,
                classify_failure=lambda error: Retry(
                    after_seconds=0.005, cooldown_resource=True
                ),
            )
        },
    )
    first = await flow.run_async()
    second = await flow.run_async()

    assert first.status is FlowStatus.SUCCESS
    assert second.status is FlowStatus.SUCCESS
    assert calls == [1, 2, 1, 2]
    assert [item.number for item in second.tasks[0].attempts[0].invocations] == [1, 2]


@pytest.mark.asyncio
async def test_materialization_hit_has_no_invocation_during_resource_cooldown():
    registry = AssetRegistry()
    loaded = asyncio.Event()
    cooldown_requested = asyncio.Event()
    adapter = _MemoryAdapter(loaded=loaded, ready=cooldown_requested)

    async def cached(context: AssetContext):
        raise AssertionError("a materialization hit must not call the body")

    registry.register(
        cached,
        partition_def=Keys(),
        resource="api",
        materialization=adapter,
        version="1",
    )
    adapter.seed("cached", 0, "cached")
    calls = []

    async def cooling(context: AssetContext):
        calls.append(context.invocation_number)
        if context.invocation_number == 1:
            raise TimeoutError("trigger cooldown")

    registry.register(cooling, resource="api")
    result = await asyncio.wait_for(
        Flow(
            ["cached", "cooling"],
            registry=registry,
            resources={
                "api": ResourcePolicy(
                    2,
                    max_attempts=2,
                    classify_failure=lambda error: (
                        cooldown_requested.set()
                        or Retry(after_seconds=0.005, cooldown_resource=True)
                    ),
                )
            },
        ).run_async({"partition_keys": [0], "max_concurrency": 2}),
        timeout=1,
    )

    assert result.status is FlowStatus.SUCCESS
    assert loaded.is_set()
    assert result.tasks[0].attempts[0].resolution == "materialized"
    assert result.tasks[0].attempts[0].invocations == ()
    assert calls == [1, 2]


@pytest.mark.asyncio
async def test_materialization_commit_failure_is_numbered_but_never_retried():
    registry = AssetRegistry()
    adapter = _MemoryAdapter(fail_commit=True)
    calls = []

    async def body(context: AssetContext):
        calls.append(context.invocation_number)
        return "new"

    registry.register(
        body,
        partition_def=Keys(),
        resource="api",
        materialization=adapter,
        version="1",
    )
    result = await Flow(
        ["body"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1,
                max_attempts=4,
                classify_failure=lambda error: Retry(),
            )
        },
    ).run_async({"partition_keys": [0]})

    logical = result.tasks[0].attempts[0]
    assert result.status is FlowStatus.FAILED
    assert calls == [1]
    assert len(logical.invocations) == 1
    assert logical.invocations[0].status is AttemptStatus.FAILED


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["resolve", "load"])
async def test_materialization_preparation_failure_is_never_retried(failure):
    registry = AssetRegistry()
    adapter = _MemoryAdapter(fail=failure)
    calls = []
    classified = []

    async def body(context: AssetContext):
        calls.append(context.invocation_number)
        return "body should not run"

    registry.register(
        body,
        partition_def=Keys(),
        resource="api",
        materialization=adapter,
        version="1",
    )
    if failure == "load":
        adapter.seed(body.__name__, 0, "stored")
    result = await Flow(
        ["body"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1,
                max_attempts=3,
                classify_failure=lambda error: classified.append(error) or Retry(),
            )
        },
    ).run_async({"partition_keys": [0]})

    logical = result.tasks[0].attempts[0]
    assert result.status is FlowStatus.FAILED
    assert calls == []
    assert classified == []
    assert logical.invocations == ()


@pytest.mark.asyncio
async def test_materialization_miss_keeps_prepared_identity_across_body_retry():
    registry = AssetRegistry()
    adapter = _MemoryAdapter()
    calls = []

    async def flaky(context: AssetContext):
        calls.append(context.invocation_number)
        if context.invocation_number == 1:
            raise TimeoutError("retry body")
        return "saved"

    registry.register(
        flaky,
        partition_def=Keys(),
        resource="api",
        materialization=adapter,
        version="1",
    )
    result = await Flow(
        ["flaky"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1,
                max_attempts=2,
                classify_failure=lambda error: Retry(),
            )
        },
    ).run_async({"partition_keys": [0]})

    assert result.status is FlowStatus.SUCCESS
    assert calls == [1, 2]
    assert len(adapter.resolve_contexts) == 1
    assert len(adapter.committed) == 1


class _MemoryAdapter:
    def __init__(self, loaded=None, fail_commit=False, ready=None, fail=None):
        self.records = {}
        self.values = {}
        self.loaded = loaded
        self.fail_commit = fail_commit
        self.ready = ready
        self.fail = fail
        self.resolve_contexts = []
        self.committed = []

    async def resolve(self, context):
        self.resolve_contexts.append(context)
        if self.ready is not None:
            await self.ready.wait()
        if self.fail == "resolve":
            raise OSError("resolve failed")
        return self.records.get(context.partition_key)

    async def load(self, record):
        if self.fail == "load":
            raise OSError("load failed")
        if self.loaded is not None:
            self.loaded.set()
        return self.values[record.materialization_id]

    async def commit(self, context, output):
        self.committed.append(context)
        if self.fail_commit:
            raise OSError("commit failed")
        materialization_id = f"cached:{context.partition_key}"
        record = MaterializationRecord(
            materialization_id,
            context.asset_name,
            context.version,
            context.partition_domain,
            context.partition_key_present,
            context.partition_key,
            context.input_materialization_ids,
            True,
        )
        self.records[context.partition_key] = record
        self.values[materialization_id] = output
        return record

    def seed(self, asset_name, key, output):
        record = MaterializationRecord(
            f"cached:{key}",
            asset_name,
            "1",
            Keys().domain,
            True,
            key,
            (),
            True,
        )
        self.records[key] = record
        self.values[record.materialization_id] = output


@pytest.mark.asyncio
async def test_observer_failure_on_invocation_start_propagates_without_classification():
    registry = AssetRegistry()
    classified = []

    async def body():
        raise RuntimeError("should not run")

    class BrokenObserver:
        def on_event(self, event):
            if event.kind is EventKind.INVOCATION_STARTED:
                raise RuntimeError("observer failed")

    registry.register(body, resource="api")
    flow = Flow(
        ["body"],
        registry=registry,
        resources={
            "api": ResourcePolicy(
                1,
                max_attempts=2,
                classify_failure=lambda error: classified.append(error) or Retry(),
            )
        },
    )
    with pytest.raises(RuntimeError, match="observer failed"):
        await flow.run_async(event_consumer=BrokenObserver())
    assert classified == []
