import kazeflow
from kazeflow.assets import default_registry
from kazeflow.events import EventKind, ExecutionEvent, ExecutionEventConsumer
from kazeflow.flow import Flow
from kazeflow.plan import FlowPlan
from kazeflow.results import RunResult


def test_root_exports_the_core_workflow_and_event_boundary() -> None:
    assert kazeflow.__all__ == [
        "asset",
        "AssetContext",
        "DatePartitionDef",
        "EventKind",
        "ExecutionEvent",
        "ExecutionEventConsumer",
        "Flow",
        "FlowPlan",
        "RunConfig",
        "RunResult",
        "run",
    ]
    assert kazeflow.Flow is Flow
    assert kazeflow.FlowPlan is FlowPlan
    assert kazeflow.RunResult is RunResult
    assert kazeflow.EventKind is EventKind
    assert kazeflow.ExecutionEvent is ExecutionEvent
    assert kazeflow.ExecutionEventConsumer is ExecutionEventConsumer
    assert not hasattr(kazeflow, "FlowTUIRenderer")


def test_root_api_plans_then_runs_with_a_structured_result() -> None:
    default_registry.clear()
    invoked: list[str] = []

    @kazeflow.asset
    def target() -> str:
        invoked.append("target")
        return "completed"

    try:
        flow = kazeflow.Flow(["target"])
        plan = flow.plan()

        assert isinstance(plan, kazeflow.FlowPlan)
        assert [task.name for task in plan.tasks] == ["target"]
        assert invoked == []

        result = kazeflow.run(["target"])

        assert isinstance(result, kazeflow.RunResult)
        assert result.status.value == "success"
        assert result.tasks[0].attempts[0].output == "completed"
        assert invoked == ["target"]
    finally:
        default_registry.clear()
