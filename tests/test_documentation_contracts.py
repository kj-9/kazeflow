"""Executable evidence for trust-sensitive statements in the user documentation."""

from __future__ import annotations

import asyncio
from datetime import date
import json
from typing import Any, cast

import pytest

from kazeflow import AssetContext, DatePartitionDef, Flow, asset
from kazeflow.assets import AssetRegistry, default_registry
from kazeflow.cli import EXIT_SUCCESS, EXIT_USAGE, main
from kazeflow.partition import PartitionDef


@pytest.fixture(autouse=True)
def clear_default_registry() -> None:
    default_registry.clear()


def test_partition_selection_contract_matches_documentation(
    tmp_path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry = tmp_path / "partitioned.py"
    marker = tmp_path / "asset-ran"
    entry.write_text(
        "\n".join(
            (
                "from pathlib import Path",
                "from kazeflow import DatePartitionDef, Flow, asset",
                "",
                "@asset(partition_def=DatePartitionDef())",
                "def daily():",
                f"    Path({str(marker)!r}).write_text('ran')",
                "",
                "flow = Flow(['daily'])",
            )
        ),
        encoding="utf-8",
    )

    assert main(["plan", str(entry)]) == EXIT_USAGE
    assert capsys.readouterr().err
    assert not marker.exists()

    invalid_date = "not-a-date"
    assert (
        main(
            [
                "plan",
                str(entry),
                "--partition-key",
                invalid_date,
            ]
        )
        == EXIT_USAGE
    )
    diagnostics = capsys.readouterr().err
    assert invalid_date not in diagnostics
    assert not marker.exists()

    assert main(["partitions", str(entry), "--format", "json"]) == EXIT_SUCCESS
    inspection = json.loads(capsys.readouterr().out)
    assert inspection["partitions"] == [
        {
            "asset": "daily",
            "definition_kind": "DatePartitionDef",
            "domain": "date",
            "key_format": "YYYY-MM-DD",
            "supports_range": True,
        }
    ]
    assert not marker.exists()

    assert (
        main(
            [
                "plan",
                str(entry),
                "--partition-key",
                "2026-08-11",
                "--format",
                "json",
            ]
        )
        == EXIT_SUCCESS
    )
    key_projection = json.loads(capsys.readouterr().out)
    assert key_projection["config"]["partition_selection"] == {
        "kind": "keys",
        "domain": "date",
        "count": 1,
    }
    assert key_projection["tasks"][0]["partition_selection"] == {
        "kind": "keys",
        "domain": "date",
        "count": 1,
    }
    assert "2026-08-11" not in json.dumps(key_projection)
    assert not marker.exists()

    assert (
        main(
            [
                "plan",
                str(entry),
                "--partition-range",
                "2026-08-11",
                "2026-08-13",
                "--format",
                "json",
            ]
        )
        == EXIT_SUCCESS
    )
    range_projection = json.loads(capsys.readouterr().out)
    assert range_projection["config"]["partition_selection"] == {
        "kind": "range",
        "domain": "date",
        "count": 3,
    }
    assert not marker.exists()

    assert (
        main(["plan", str(entry), "--empty-partitions", "--format", "json"])
        == EXIT_SUCCESS
    )
    empty_projection = json.loads(capsys.readouterr().out)
    assert empty_projection["config"]["partition_selection"] == {
        "kind": "empty",
        "domain": "date",
        "count": 0,
    }
    assert not marker.exists()

    registry = AssetRegistry()

    def daily() -> None:
        raise AssertionError("planning must not execute assets")

    registry.register(daily, partition_def=DatePartitionDef())
    plan = Flow(["daily"], registry=registry).plan({"partition_keys": ("2026-08-11",)})
    assert plan.config.partition_keys == (date(2026, 8, 11),)
    assert plan.config.selection_kind == "keys"
    assert plan.config.partition_domain == "date"

    date_partitions = DatePartitionDef()
    keys = date_partitions.range("2026-08-11", "2026-08-12")
    assert keys == [date(2026, 8, 11), date(2026, 8, 12)]
    assert Flow(["daily"], registry=registry).plan(
        {"partition_range": ("2026-08-11", "2026-08-12")}
    ).config.partition_keys == tuple(keys)

    with pytest.raises(ValueError):
        Flow(["daily"], registry=registry).plan(
            {"partition_range": ("2026-08-12", "2026-08-11")}
        )


def test_portable_failure_can_repeat_an_omitted_partition_key() -> None:
    secret_key = "tenant-secret-east"

    class TenantPartitionDef(PartitionDef):
        def range(self, start, end):
            return (start, end)

    @asset(partition_def=TenantPartitionDef())
    def fails(context: AssetContext) -> None:
        raise RuntimeError(f"failed for {context.partition_key}")

    result = asyncio.run(Flow(["fails"]).run_async({"partition_keys": (secret_key,)}))
    record = result.to_record()
    serialized = json.dumps(record)

    tasks = cast(list[dict[str, Any]], record["tasks"])
    attempt = cast(list[dict[str, Any]], tasks[0]["attempts"])[0]
    assert attempt["attempt"]["partition"] == {"present": True}
    assert "partition_key" not in attempt["attempt"]["partition"]
    assert secret_key in attempt["failure"]["message"]
    assert secret_key in attempt["failure"]["traceback"]
    assert secret_key in serialized


@pytest.mark.asyncio
async def test_external_cancellation_propagates_without_a_terminal_result() -> None:
    started = asyncio.Event()

    @asset
    async def waits_forever() -> None:
        started.set()
        await asyncio.Event().wait()

    run_task = asyncio.create_task(Flow(["waits_forever"]).run_async())
    await started.wait()
    run_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await run_task
