#!/usr/bin/env python3
"""Demonstrate local materialization reuse across three fresh processes.

Run from the repository root:

    uv run python examples/file_materialization.py seed --root ./materialized --partition-key 2026-08-11
    uv run python examples/file_materialization.py fail --root ./materialized --partition-key 2026-08-11
    uv run python examples/file_materialization.py retry --root ./materialized --partition-key 2026-08-11
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from kazeflow import DatePartitionDef, asset, run
from kazeflow.file_materialization import FileMaterialization, JsonCodec
from kazeflow.results import FlowStatus


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("seed", "fail", "retry"))
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--partition-key", required=True)
    args = parser.parse_args()

    store = FileMaterialization(args.root, codec=JsonCodec())
    partition_def = DatePartitionDef()

    @asset(
        partition_def=partition_def,
        materialization=store,
        version="daily-source-v1",
    )
    def producer(context):
        print(f"executing producer for {context.partition_key}")
        return {"partition": context.partition_key.isoformat(), "rows": [1, 2, 3]}

    @asset(partition_def=partition_def)
    def publish(producer, context):
        print("executing downstream publish")
        if args.stage == "fail":
            raise RuntimeError("demonstration failure; retry in another process")
        return {"published_rows": len(producer[context.partition_key]["rows"])}

    targets = ["producer"] if args.stage == "seed" else ["publish"]
    result = run(targets, {"partition_keys": [args.partition_key]})
    tasks = {task.task.task_name: task for task in result.tasks}

    if args.stage == "seed":
        assert tasks["producer"].attempts[0].resolution == "executed"
        assert result.status is FlowStatus.SUCCESS
        print("producer output committed; start the fail command")
        return 0

    assert tasks["producer"].attempts[0].resolution == "materialized"
    if args.stage == "fail":
        assert result.status is FlowStatus.FAILED
        print("producer reused; downstream failed as requested")
        return 1

    assert tasks["publish"].attempts[0].resolution == "executed"
    assert result.status is FlowStatus.SUCCESS
    print("producer reused; downstream retry succeeded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
