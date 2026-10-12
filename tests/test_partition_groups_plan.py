from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from kazeflow.assets import AssetRegistry
from kazeflow.groups import PartitionGroup
from kazeflow.partition import PartitionDef
from kazeflow.plan import PlanConfig, build_flow_plan


class IdentityPartitionDef(PartitionDef):
    @property
    def domain(self) -> str:
        return "key"

    def normalize_key(self, key: object):
        return key

    def range(self, start, end):
        raise AssertionError("identity partitions do not support ranges")


def add_asset(
    registry: AssetRegistry,
    name: str,
    *,
    deps: tuple[str, ...] = (),
    partitioned: bool = False,
    body_calls: list[str] | None = None,
) -> None:
    def asset_function():
        if body_calls is not None:
            body_calls.append(name)

    asset_function.__name__ = name
    registry.register(
        asset_function,
        deps=list(deps),
        partition_def=IdentityPartitionDef() if partitioned else None,
    )


def test_partition_group_snapshots_assets_and_is_immutable():
    assets = ["prepare", "publish"]
    group = PartitionGroup(assets, max_active_keys=2)
    assets.append("later")

    assert group.assets == ("prepare", "publish")
    assert group.max_active_keys == 2
    with pytest.raises(FrozenInstanceError):
        setattr(group, "max_active_keys", 3)


@pytest.mark.parametrize(
    ("assets", "limit", "error"),
    [
        (("only",), 1, ValueError),
        (("a", "a"), 1, ValueError),
        (("a", ""), 1, ValueError),
        (("a", 2), 1, TypeError),
        ("ab", 1, TypeError),
        (("a", "b"), 0, ValueError),
        (("a", "b"), True, ValueError),
        (("a", "b"), 1.5, ValueError),
    ],
)
def test_partition_group_rejects_invalid_declarations(assets, limit, error):
    with pytest.raises(error):
        PartitionGroup(assets, max_active_keys=limit)


def test_plan_exposes_only_selected_groups_in_first_member_order():
    registry = AssetRegistry()
    calls: list[str] = []
    add_asset(registry, "a_prepare", partitioned=True, body_calls=calls)
    add_asset(registry, "b_publish", deps=("a_prepare",), partitioned=True)
    add_asset(registry, "c_prepare", partitioned=True)
    add_asset(registry, "d_publish", deps=("c_prepare",), partitioned=True)
    add_asset(registry, "z_target", deps=("b_publish", "d_publish"))
    declared = [
        PartitionGroup(["c_prepare", "d_publish"], max_active_keys=2),
        PartitionGroup(["a_prepare", "b_publish"]),
    ]
    plan = build_flow_plan(
        ["z_target"],
        partition_groups=declared,
        config=PlanConfig(partition_keys=["k1", "k2"]),
        registry=registry,
    )

    assert plan.partition_groups == (declared[1], declared[0])
    assert isinstance(plan.partition_groups, tuple)
    assert calls == []
    with pytest.raises(FrozenInstanceError):
        setattr(plan, "partition_groups", ())


def test_unselected_groups_are_omitted_and_partial_groups_are_rejected():
    registry = AssetRegistry()
    add_asset(registry, "a", partitioned=True)
    add_asset(registry, "b", deps=("a",), partitioned=True)
    add_asset(registry, "other")
    group = PartitionGroup(("a", "b"))

    ungrouped = build_flow_plan(["other"], partition_groups=[group], registry=registry)
    assert ungrouped.partition_groups == ()

    with pytest.raises(ValueError, match="wholly selected"):
        build_flow_plan(
            ["a"],
            partition_groups=[group],
            config=PlanConfig(partition_keys=["k"]),
            registry=registry,
        )


@pytest.mark.parametrize(
    ("groups", "message"),
    [
        ([PartitionGroup(("a", "b")), PartitionGroup(("b", "c"))], "disjoint"),
        ([PartitionGroup(("missing", "b"))], "not found"),
        ([PartitionGroup(("a", "plain"))], "must be partitioned"),
        ([PartitionGroup(("a", "c"))], "directly depend"),
    ],
)
def test_plan_rejects_overlapping_unknown_unpartitioned_or_nonchain_groups(
    groups, message: str
):
    registry = AssetRegistry()
    add_asset(registry, "a", partitioned=True)
    add_asset(registry, "b", deps=("a",), partitioned=True)
    add_asset(registry, "c", partitioned=True)
    add_asset(registry, "plain")

    with pytest.raises(ValueError, match=message):
        build_flow_plan(
            ["a", "b", "c", "plain"],
            partition_groups=groups,
            config=PlanConfig(partition_keys=["k"]),
            registry=registry,
        )


def test_plan_rejects_dependency_path_that_leaves_and_reenters_group():
    registry = AssetRegistry()
    add_asset(registry, "a", partitioned=True)
    add_asset(registry, "external", deps=("a",), partitioned=True)
    add_asset(registry, "b", deps=("a", "external"), partitioned=True)

    with pytest.raises(ValueError, match="leave and re-enter"):
        build_flow_plan(
            ["b"],
            partition_groups=[PartitionGroup(("a", "b"))],
            config=PlanConfig(partition_keys=["k"]),
            registry=registry,
        )


def test_plan_rejects_invalid_partition_group_collection():
    registry = AssetRegistry()
    add_asset(registry, "target")

    with pytest.raises(TypeError, match="sequence"):
        build_flow_plan(
            ["target"], partition_groups=cast(Any, "not groups"), registry=registry
        )
    with pytest.raises(TypeError, match="PartitionGroup"):
        build_flow_plan(
            ["target"], partition_groups=cast(Any, [object()]), registry=registry
        )
