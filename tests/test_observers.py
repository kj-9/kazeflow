import logging
from datetime import datetime, timezone

import pytest

from kazeflow.assets import AssetRegistry
from kazeflow.events import (
    EventKind,
    ExecutionEvent,
    LoggingExecutionEventConsumer,
    NoOpExecutionEventConsumer,
)
from kazeflow.flow import Flow
from kazeflow.results import FlowStatus


def test_noop_observer_discards_a_valid_event_without_side_effect() -> None:
    event = ExecutionEvent(
        "run-1",
        1,
        datetime.now(timezone.utc),
        EventKind.FLOW_STARTED,
        status=FlowStatus.RUNNING,
    )

    assert NoOpExecutionEventConsumer().on_event(event) is None


def test_logging_observer_uses_the_supplied_logger_without_configuration(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("kazeflow.tests.observer")
    consumer = LoggingExecutionEventConsumer(logger)
    event = ExecutionEvent(
        "run-1",
        1,
        datetime.now(timezone.utc),
        EventKind.FLOW_STARTED,
        status=FlowStatus.RUNNING,
    )

    with caplog.at_level(logging.INFO, logger=logger.name):
        consumer.on_event(event)

    assert "run_id=run-1 sequence=1 kind=flow_started status=running" in caplog.text


@pytest.mark.asyncio
async def test_consumer_failure_propagates_without_turning_into_asset_failure() -> None:
    registry = AssetRegistry()
    asset_calls: list[str] = []

    def target() -> None:
        asset_calls.append("target")

    registry.register(target)
    received: list[ExecutionEvent] = []

    class FailingConsumer:
        def on_event(self, event: ExecutionEvent) -> None:
            received.append(event)
            raise RuntimeError("observer failed")

    with pytest.raises(RuntimeError, match="observer failed"):
        await Flow(["target"], registry=registry).run_async(
            event_consumer=FailingConsumer()
        )

    assert asset_calls == []
    assert [event.kind for event in received] == [EventKind.FLOW_STARTED]


@pytest.mark.asyncio
async def test_consumer_receives_synchronously_ordered_events() -> None:
    registry = AssetRegistry()

    async def first() -> str:
        return "first"

    async def second(first: str) -> str:
        return first

    registry.register(first)
    registry.register(second)
    received: list[ExecutionEvent] = []

    class RecordingConsumer:
        def on_event(self, event: ExecutionEvent) -> None:
            received.append(event)

    result = await Flow(["second"], registry=registry).run_async(
        event_consumer=RecordingConsumer()
    )

    assert result.status.value == "success"
    assert [event.sequence for event in received] == list(range(1, len(received) + 1))
    assert received[-1].kind is EventKind.FLOW_FINISHED
