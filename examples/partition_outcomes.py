"""Inspect terminal partition outcomes and explicitly choose a later run."""

import asyncio
from datetime import date

from kazeflow import (
    AssetContext,
    DatePartitionDef,
    Flow,
    OpenCircuit,
    ResourcePolicy,
    asset,
)


PARTITIONS = (
    date(2026, 8, 11),
    date(2026, 8, 12),
    date(2026, 8, 13),
)
resource_available = False


@asset(partition_def=DatePartitionDef(), resource="daily-publisher")
def publish_daily(context: AssetContext) -> str:
    if context.partition_key == date(2026, 8, 12) and not resource_available:
        raise OSError("the remote service is temporarily unavailable")
    return f"published {context.partition_key}"


def classify_failure(error: Exception) -> OpenCircuit:
    if isinstance(error, OSError):
        return OpenCircuit()
    raise error


def main() -> None:
    global resource_available

    flow = Flow(
        ["publish_daily"],
        resources={
            "daily-publisher": ResourcePolicy(
                max_concurrency=1,
                classify_failure=classify_failure,
            )
        },
    )
    config = {"partition_keys": PARTITIONS}

    first_result = asyncio.run(flow.run_async(config))
    failed = first_result.failed_partition_keys("publish_daily")
    circuit_skipped = first_result.circuit_skipped_partition_keys("publish_daily")
    print("failed:", failed)
    print("circuit skipped:", circuit_skipped)

    # The application decides when the external service is ready and starts a new
    # run with the selected keys. kazeflow does not resume the earlier run.
    resource_available = True
    retry_keys = failed + circuit_skipped
    second_result = asyncio.run(flow.run_async({"partition_keys": retry_keys}))
    print("selected for the new run:", retry_keys)
    print("new run status:", second_result.status.value)


if __name__ == "__main__":
    main()
