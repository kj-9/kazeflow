#!/usr/bin/env python3
"""Show committed and reused provenance in two explicit process runs.

Run twice with the same root and partition key:

    uv run python examples/materialization_resume.py --root ./materialized
    uv run python examples/materialization_resume.py --root ./materialized

The first run executes and commits the producer. The second resolves and loads
that committed output. Each invocation prints the v4 result and finish event;
starting the script again is an explicit caller action, not automatic resume.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from kazeflow import DatePartitionDef, Flow, asset
from kazeflow.events import EventKind, ExecutionEvent
from kazeflow.file_materialization import FileMaterialization, JsonCodec


class EventCollector:
    def __init__(self) -> None:
        self.events: list[ExecutionEvent] = []

    def on_event(self, event: ExecutionEvent) -> None:
        self.events.append(event)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--partition-key", default="2026-10-12")
    args = parser.parse_args()

    adapter = FileMaterialization(args.root, codec=JsonCodec())

    @asset(
        partition_def=DatePartitionDef(),
        materialization=adapter,
        version="resume-example-v1",
    )
    def producer(context):
        return {"partition": context.partition_key.isoformat(), "rows": [1, 2, 3]}

    flow = Flow(["producer"])
    events = EventCollector()
    result = asyncio.run(
        flow.run_async({"partition_keys": [args.partition_key]}, event_consumer=events)
    )
    attempt = result.tasks[0].attempts[0]
    finish = next(
        event for event in events.events if event.kind is EventKind.ATTEMPT_FINISHED
    )

    result_record = result.to_record(schema_version=4)
    event_record = finish.to_record()
    assert event_record["resolution"] == attempt.resolution
    assert event_record["materialization_id"] == attempt.materialization_id
    print(
        json.dumps(
            {"result": result_record, "attempt_finished_event": event_record},
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
