from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from kazeflow.assets import AssetRegistry
from kazeflow.materialization import MaterializationContext, MaterializationRecord
from kazeflow.partition import PartitionDef
from kazeflow.plan import PlanConfig, build_flow_plan
from kazeflow.results import (
    AttemptReference,
    AttemptResult,
    AttemptStatus,
    FailureInfo,
    TaskReference,
)


class TenantPartitionDef(PartitionDef):
    @property
    def domain(self) -> str:
        return "tenant"

    def normalize_key(self, key: object) -> str:
        return str(key)

    def range(self, start: object, end: object):
        raise ValueError("ranges are not supported")


class Adapter:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def resolve(self, context: MaterializationContext):
        self.calls.append("resolve")
        return None

    def load(self, record: MaterializationRecord):
        self.calls.append("load")
        return None

    def commit(self, context: MaterializationContext, output: object):
        self.calls.append("commit")
        raise AssertionError("planning must not call adapter operations")


def _register(
    registry: AssetRegistry,
    name: str,
    *,
    deps: tuple[str, ...] = (),
    adapter: Adapter | None = None,
    version: str | None = None,
    partitioned: bool = True,
) -> None:
    def body() -> str:
        return name

    body.__name__ = name
    registry.register(
        body,
        deps=list(deps),
        partition_def=TenantPartitionDef() if partitioned else None,
        materialization=adapter,
        version=version,
    )


def test_asset_declaration_keeps_function_directly_callable() -> None:
    registry = AssetRegistry()
    adapter = Adapter()

    def body() -> str:
        return "output"

    registry.register(
        body,
        partition_def=TenantPartitionDef(),
        materialization=adapter,
        version="v1",
    )

    assert body() == "output"
    assert registry.get("body").materialization is adapter
    assert registry.get("body").version == "v1"


def test_plan_marks_materialized_root_without_adapter_io() -> None:
    registry = AssetRegistry()
    adapter = Adapter()
    _register(registry, "root", adapter=adapter, version="v1")

    plan = build_flow_plan(
        ["root"],
        config=PlanConfig(partition_keys=[0]),
        registry=registry,
    )

    assert plan.tasks[0].materialization_enabled is True
    assert adapter.calls == []


def test_plan_rejects_materialized_asset_with_nonmaterialized_dependency() -> None:
    registry = AssetRegistry()
    adapter = Adapter()
    _register(registry, "plain", adapter=None, version=None)
    _register(registry, "target", deps=("plain",), adapter=adapter, version="v1")

    with pytest.raises(
        ValueError, match="does not provide a partitioned materialization"
    ):
        build_flow_plan(
            ["target"],
            config=PlanConfig(partition_keys=["acme"]),
            registry=registry,
        )

    assert adapter.calls == []


def test_attempt_materialization_fields_are_in_memory_only() -> None:
    attempt = AttemptReference(TaskReference("asset"), True, "acme")
    result = AttemptResult(
        attempt=attempt,
        status=AttemptStatus.SUCCESS,
        started_at=datetime(2026, 10, 11, tzinfo=timezone.utc),
        ended_at=datetime(2026, 10, 11, 0, 0, 1, tzinfo=timezone.utc),
        duration=timedelta(seconds=1),
        output="value",
        resolution="materialized",
        materialization_id="mat-1",
    )

    assert result.resolution == "materialized"
    assert result.materialization_id == "mat-1"
    assert set(result.to_record()) == {
        "attempt",
        "status",
        "started_at",
        "ended_at",
        "duration_seconds",
        "reason",
        "blocked_by",
        "failure",
    }


def test_executed_materialization_retains_its_committed_id() -> None:
    result = AttemptResult(
        attempt=AttemptReference(TaskReference("asset"), True, "acme"),
        status=AttemptStatus.SUCCESS,
        started_at=datetime(2026, 10, 11, tzinfo=timezone.utc),
        ended_at=datetime(2026, 10, 11, 0, 0, 1, tzinfo=timezone.utc),
        duration=timedelta(seconds=1),
        output="value",
        resolution="executed",
        materialization_id="mat-2",
    )

    assert result.resolution == "executed"
    assert result.materialization_id == "mat-2"


def test_failed_attempt_cannot_retain_materialization_data() -> None:
    with pytest.raises(ValueError, match="only a successful attempt"):
        AttemptResult(
            attempt=AttemptReference(TaskReference("asset")),
            status=AttemptStatus.FAILED,
            started_at=datetime(2026, 10, 11, tzinfo=timezone.utc),
            ended_at=datetime(2026, 10, 11, 0, 0, 1, tzinfo=timezone.utc),
            duration=timedelta(seconds=1),
            failure=FailureInfo("ValueError", "bad", ""),
            resolution="executed",
        )
