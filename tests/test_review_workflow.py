"""Focused coverage for the documented plan → review → run → result workflow."""

import pytest

import kazeflow
from kazeflow.assets import default_registry
from kazeflow.results import AttemptStatus, FlowStatus, SkipReason


@pytest.fixture(autouse=True)
def clear_default_registry() -> None:
    default_registry.clear()


def test_public_review_workflow_inspects_plan_before_running_selected_targets() -> None:
    invoked: list[str] = []

    @kazeflow.asset
    def source() -> str:
        invoked.append("source")
        return "raw"

    @kazeflow.asset(deps=["source"], partition_def=kazeflow.DatePartitionDef())
    def publish(source: str, context: kazeflow.AssetContext) -> str:
        invoked.append(f"publish:{context.partition_key}")
        return f"{source}:{context.partition_key}"

    run_config = {
        "max_concurrency": 2,
        "partition_keys": ["2026-08-11", "2026-08-12"],
    }
    flow = kazeflow.Flow(["publish"])

    plan = flow.plan(run_config)

    assert isinstance(plan, kazeflow.FlowPlan)
    assert plan.targets == ("publish",)
    assert [(task.name, task.dependencies) for task in plan.tasks] == [
        ("source", ()),
        ("publish", ("source",)),
    ]
    assert plan.config.max_concurrency == 2
    assert tuple(map(str, plan.config.partition_keys or ())) == (
        "2026-08-11",
        "2026-08-12",
    )
    assert plan.tasks[0].partition_keys is None
    assert tuple(map(str, plan.tasks[1].partition_keys or ())) == (
        "2026-08-11",
        "2026-08-12",
    )
    assert invoked == []

    result = kazeflow.run(["publish"], run_config)

    assert isinstance(result, kazeflow.RunResult)
    assert result.status is FlowStatus.SUCCESS
    assert [task.status for task in result.tasks] == [
        AttemptStatus.SUCCESS,
        AttemptStatus.SUCCESS,
    ]
    assert [attempt.output for attempt in result.tasks[1].attempts] == [
        "raw:2026-08-11",
        "raw:2026-08-12",
    ]
    assert invoked[0] == "source"
    assert sorted(invoked[1:]) == [
        "publish:2026-08-11",
        "publish:2026-08-12",
    ]


def test_public_result_exposes_failed_and_dependency_blocked_attempts() -> None:
    @kazeflow.asset
    def extract() -> None:
        raise ValueError("source is invalid")

    @kazeflow.asset(deps=["extract"])
    def publish(extract: object) -> None:
        raise AssertionError("a blocked task must not run")

    result = kazeflow.run(["publish"])

    assert isinstance(result, kazeflow.RunResult)
    assert result.status is FlowStatus.FAILED
    failed, blocked = result.tasks
    assert failed.status is AttemptStatus.FAILED
    assert failed.attempts[0].failure is not None
    assert failed.attempts[0].failure.exception_type == "ValueError"
    assert blocked.status is AttemptStatus.SKIPPED
    assert blocked.attempts[0].reason is SkipReason.DEPENDENCY_BLOCKED
    assert blocked.attempts[0].blocked_by == (failed.attempts[0].attempt,)
