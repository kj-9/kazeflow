"""Commit, explicitly invalidate, rebuild, and reuse one partition.

Run from the repository root:

    uv run python examples/materialization_invalidation.py --root ./materialized
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path

from kazeflow import AssetContext, DatePartitionDef, Flow, asset
from kazeflow.file_materialization import FileMaterialization, JsonCodec
from kazeflow.materialization import MaterializationContext


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()

    adapter = FileMaterialization(args.root, codec=JsonCodec())
    partition_def = DatePartitionDef()
    partition_key = date(2026, 10, 12)
    body_calls: list[date] = []

    @asset(
        partition_def=partition_def,
        materialization=adapter,
        version="daily-report-v1",
    )
    def daily_report(context: AssetContext) -> dict[str, str]:
        assert isinstance(context.partition_key, date)
        body_calls.append(context.partition_key)
        return {"partition": context.partition_key.isoformat()}

    flow = Flow(["daily_report"])
    run_config = {"partition_keys": [partition_key]}
    identity = MaterializationContext(
        asset_name="daily_report",
        version="daily-report-v1",
        partition_domain=partition_def.domain,
        partition_key_present=True,
        partition_key=partition_key,
    )

    first = asyncio.run(flow.run_async(run_config))
    first_attempt = first.tasks[0].attempts[0]
    assert first_attempt.resolution in ("executed", "materialized")
    previous_id = first_attempt.materialization_id
    assert previous_id is not None
    calls_before_rebuild = len(body_calls)
    assert adapter.inspect(identity).status == "available"

    adapter.invalidate(identity, reason="source data was corrected")
    inspection = adapter.inspect(identity)
    assert inspection.status == "invalidated"
    assert inspection.reason == "source data was corrected"

    # Rebuild only because this new run explicitly selects the partition.
    rebuilt = asyncio.run(flow.run_async(run_config))
    rebuilt_attempt = rebuilt.tasks[0].attempts[0]
    assert rebuilt_attempt.resolution == "executed"
    rebuilt_id = rebuilt_attempt.materialization_id
    assert rebuilt_id is not None and rebuilt_id != previous_id
    assert len(body_calls) == calls_before_rebuild + 1

    later = asyncio.run(flow.run_async(run_config))
    later_attempt = later.tasks[0].attempts[0]
    assert later_attempt.resolution == "materialized"
    assert later_attempt.materialization_id == rebuilt_id
    assert len(body_calls) == calls_before_rebuild + 1

    print(f"revoked generation: {previous_id}")
    print(f"rebuilt generation reused by later run: {rebuilt_id}")


if __name__ == "__main__":
    main()
