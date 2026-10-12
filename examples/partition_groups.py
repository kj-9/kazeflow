#!/usr/bin/env python3
"""Show one selected partition traversing a linear group before the next starts."""

from __future__ import annotations

import asyncio

from kazeflow import AssetContext, DatePartitionDef, Flow, PartitionGroup, asset


trace: list[tuple[str, str]] = []


@asset(partition_def=DatePartitionDef())
def extract_daily(context: AssetContext) -> str:
    partition_key = context.partition_key
    assert partition_key is not None
    key = str(partition_key)
    trace.append(("extract", key))
    return key


@asset(partition_def=DatePartitionDef())
def publish_daily(extract_daily: dict[object, str], context: AssetContext) -> str:
    partition_key = context.partition_key
    assert partition_key is not None
    key = str(partition_key)
    assert extract_daily[partition_key] == key
    trace.append(("publish", key))
    return key


flow = Flow(
    ["publish_daily"],
    partition_groups=[PartitionGroup(("extract_daily", "publish_daily"))],
)


def main() -> None:
    config = {"partition_keys": ["2026-08-08", "2026-08-09"]}
    plan = flow.plan(config)
    assert plan.partition_groups[0].assets == ("extract_daily", "publish_daily")

    result = asyncio.run(flow.run_async(config))
    assert [task.task.task_name for task in result.tasks] == [
        "extract_daily",
        "publish_daily",
    ]
    assert trace == [
        ("extract", "2026-08-08"),
        ("publish", "2026-08-08"),
        ("extract", "2026-08-09"),
        ("publish", "2026-08-09"),
    ]
    for step, key in trace:
        print(f"{key}: {step}")


if __name__ == "__main__":
    main()
