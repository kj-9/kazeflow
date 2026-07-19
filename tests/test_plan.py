from dataclasses import FrozenInstanceError
from typing import cast

import pytest

from kazeflow.assets import AssetRegistry
from kazeflow.plan import FlowPlan, PlanConfig, TaskPlan, build_flow_plan
from kazeflow.partition import DatePartitionDef


@pytest.fixture
def registry() -> AssetRegistry:
    return AssetRegistry()


def register(registry: AssetRegistry, name: str, *, deps=(), partitioned=False):
    def asset_function():
        raise AssertionError("planning must not invoke asset functions")

    asset_function.__name__ = name
    registry.register(
        asset_function,
        deps=list(deps),
        partition_def=DatePartitionDef() if partitioned else None,
    )


def test_plan_models_are_frozen_tuple_based_values(registry: AssetRegistry):
    register(registry, "target")

    plan = build_flow_plan(["target"], registry=registry)

    assert plan == FlowPlan(
        targets=("target",),
        tasks=(TaskPlan("target", (), None),),
        config=PlanConfig(),
    )
    assert isinstance(plan.targets, tuple)
    assert isinstance(plan.tasks, tuple)
    assert isinstance(plan.tasks[0].dependencies, tuple)
    with pytest.raises(FrozenInstanceError):
        setattr(plan, "targets", ())
    with pytest.raises(FrozenInstanceError):
        setattr(plan.config, "max_concurrency", 2)
    with pytest.raises(FrozenInstanceError):
        setattr(plan.tasks[0], "name", "other")


def test_explicit_empty_partition_selection_is_not_unpartitioned(
    registry: AssetRegistry,
):
    register(registry, "partitioned", partitioned=True)

    plan = build_flow_plan(
        ["partitioned"], config=PlanConfig(partition_keys=[]), registry=registry
    )

    assert plan.config.partition_keys == ()
    assert plan.tasks[0].partition_keys == ()


def test_planning_does_not_execute_assets_or_create_side_effects(
    registry: AssetRegistry,
):
    side_effects: list[str] = []

    def target():
        side_effects.append("ran")

    registry.register(target)

    plan = build_flow_plan(["target"], registry=registry)

    assert plan.tasks[0].name == "target"
    assert side_effects == []


def test_targets_are_canonical_and_equal_across_input_order(registry: AssetRegistry):
    register(registry, "alpha")
    register(registry, "zeta")

    first = build_flow_plan(["zeta", "alpha"], registry=registry)
    second = build_flow_plan(("alpha", "zeta"), registry=registry)

    assert first.targets == ("alpha", "zeta")
    assert first == second


@pytest.mark.parametrize(
    "targets",
    [
        "target",
        b"target",
        bytearray(b"target"),
        None,
        {"target"},
        iter(["target"]),
        [1],
    ],
)
def test_invalid_target_shapes_are_rejected(registry: AssetRegistry, targets):
    register(registry, "target")

    with pytest.raises(TypeError):
        build_flow_plan(targets, registry=registry)


@pytest.mark.parametrize("targets", [[], (), [""], ["target", "target"]])
def test_invalid_target_values_are_rejected(registry: AssetRegistry, targets):
    register(registry, "target")

    with pytest.raises(ValueError):
        build_flow_plan(targets, registry=registry)


def test_unknown_target_and_missing_dependency_are_rejected(registry: AssetRegistry):
    with pytest.raises(ValueError):
        build_flow_plan(["unknown"], registry=registry)

    register(registry, "target", deps=["missing"])
    with pytest.raises(ValueError):
        build_flow_plan(["target"], registry=registry)


def test_plan_contains_transitive_dependency_closure_in_lexical_order(
    registry: AssetRegistry,
):
    register(registry, "root")
    register(registry, "intermediate", deps=["root"])
    register(registry, "target", deps=["intermediate"])
    register(registry, "alpha")
    register(registry, "zeta")
    register(registry, "join", deps={"zeta", "alpha"})

    transitive_plan = build_flow_plan(["target"], registry=registry)
    independent_plan = build_flow_plan(["join"], registry=registry)

    assert [task.name for task in transitive_plan.tasks] == [
        "root",
        "intermediate",
        "target",
    ]
    assert [task.name for task in independent_plan.tasks] == ["alpha", "zeta", "join"]
    assert independent_plan.tasks[-1].dependencies == ("alpha", "zeta")


def test_unordered_dependency_metadata_has_a_repeatable_plan(registry: AssetRegistry):
    register(registry, "alpha")
    register(registry, "beta")
    register(registry, "target", deps=["alpha", "beta"])
    registry.get("target").deps = cast(list[str], {"beta", "alpha"})

    first = build_flow_plan(["target"], registry=registry)
    second = build_flow_plan(["target"], registry=registry)

    assert first == second
    assert first.tasks[-1].dependencies == ("alpha", "beta")


def test_cycle_is_rejected(registry: AssetRegistry):
    register(registry, "alpha", deps=["beta"])
    register(registry, "beta", deps=["alpha"])

    with pytest.raises(ValueError, match="cycle"):
        build_flow_plan(["alpha"], registry=registry)


@pytest.mark.parametrize("max_concurrency", [0, -1, True, "2", 1.5])
def test_invalid_max_concurrency_is_rejected(registry: AssetRegistry, max_concurrency):
    register(registry, "target")

    with pytest.raises(ValueError):
        build_flow_plan(
            ["target"],
            config=PlanConfig(max_concurrency=max_concurrency),
            registry=registry,
        )


def test_partitioned_tasks_require_an_explicit_selection(registry: AssetRegistry):
    register(registry, "partitioned", partitioned=True)

    with pytest.raises(ValueError, match="partition_keys"):
        build_flow_plan(["partitioned"], registry=registry)


@pytest.mark.parametrize(
    "partition_keys", [[None], [["unhashable"]], [1, 1], [0, False]]
)
def test_invalid_partition_keys_are_rejected(registry: AssetRegistry, partition_keys):
    register(registry, "partitioned", partitioned=True)

    with pytest.raises(ValueError):
        build_flow_plan(
            ["partitioned"],
            config=PlanConfig(partition_keys=partition_keys),
            registry=registry,
        )


@pytest.mark.parametrize("partition_key", [0, False, ""])
def test_falsey_partition_keys_are_preserved(registry: AssetRegistry, partition_key):
    register(registry, "partitioned", partitioned=True)

    plan = build_flow_plan(
        ["partitioned"],
        config=PlanConfig(partition_keys=[partition_key]),
        registry=registry,
    )

    assert plan.tasks[0].partition_keys == (partition_key,)
