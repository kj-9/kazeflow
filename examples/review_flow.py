"""Review a human- or AI-authored flow before choosing to run it.

Run with a core-only installation:

    python3 examples/review_flow.py
"""

from kazeflow import (
    AssetContext,
    DatePartitionDef,
    Flow,
    FlowPlan,
    RunResult,
    asset,
    run,
)


PARTITIONS = ("2026-08-11", "2026-08-12")


@asset(partition_def=DatePartitionDef())
def collect(context: AssetContext) -> str:
    if str(context.partition_key) == "2026-08-12":
        raise ValueError("source data is unavailable")
    return f"raw-{context.partition_key}"


@asset(partition_def=DatePartitionDef())
def publish(collect: dict[object, str], context: AssetContext) -> str:
    partition_key = context.partition_key
    assert partition_key is not None
    return collect[partition_key].upper()


def review(plan: FlowPlan) -> None:
    """Make the decision gate explicit and inspect the planned work."""
    print("targets:", plan.targets)
    print("run configuration:", plan.config)
    for task in plan.tasks:
        print(
            f"task={task.name} dependencies={task.dependencies} "
            f"partition_keys={task.partition_keys}"
        )

    # Replace these expected values with the reviewer-approved flow definition.
    assert plan.targets == ("publish",)
    assert plan.config.max_concurrency == 2
    assert tuple(map(str, plan.config.partition_keys or ())) == PARTITIONS
    assert [task.name for task in plan.tasks] == ["collect", "publish"]
    assert plan.tasks[0].dependencies == ()
    assert plan.tasks[1].dependencies == ("collect",)
    assert all(
        tuple(map(str, task.partition_keys or ())) == PARTITIONS for task in plan.tasks
    )


def inspect_result(result: RunResult) -> None:
    """Summarize task and partition-attempt outcomes after a completed run."""
    print(f"flow status: {result.status.value}")
    for task in result.tasks:
        print(f"task={task.task.task_name} status={task.status.value}")
        for attempt in task.attempts:
            partition = (
                repr(attempt.attempt.partition_key)
                if attempt.attempt.partition_key_present
                else "unpartitioned"
            )
            print(f"  partition={partition} status={attempt.status.value}")
            if attempt.failure is not None:
                print(
                    "    failure="
                    f"{attempt.failure.exception_type}: {attempt.failure.message}"
                )
            if attempt.reason is not None:
                print(f"    reason={attempt.reason.value}")
            if attempt.blocked_by:
                print(
                    "    blocked_by="
                    f"{[blocker.task.task_name for blocker in attempt.blocked_by]}"
                )


def main() -> None:
    run_config = {"partition_keys": PARTITIONS, "max_concurrency": 2}
    flow = Flow(["publish"])

    # Planning validates and describes work without invoking collect() or publish().
    plan = flow.plan(run_config)
    review(plan)

    # The caller, not kazeflow, decides that the reviewed plan may now run.
    completed = run(["publish"], run_config)
    inspect_result(completed)


if __name__ == "__main__":
    main()
