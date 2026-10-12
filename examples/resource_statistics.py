"""Inspect per-run resource operation and observed admission statistics."""

import asyncio
from datetime import date

from kazeflow import (
    AssetContext,
    DatePartitionDef,
    Flow,
    ResourcePolicy,
    Retry,
    asset,
)


attempts = [0]


@asset(partition_def=DatePartitionDef(), resource="remote-api")
def fetch_daily(context: AssetContext) -> str:
    attempts[0] += 1
    if attempts[0] == 1:
        raise TimeoutError("temporary upstream timeout")
    return f"fetched {context.partition_key}"


def classify_failure(error: Exception) -> Retry:
    if isinstance(error, TimeoutError):
        return Retry(after_seconds=0.01)
    raise error


def main() -> None:
    flow = Flow(
        ["fetch_daily"],
        resources={
            "remote-api": ResourcePolicy(
                max_concurrency=1,
                max_attempts=2,
                classify_failure=classify_failure,
            )
        },
    )

    recovered = asyncio.run(flow.run_async({"partition_keys": [date(2026, 8, 11)]}))
    stats = recovered.resource_statistics("remote-api")
    assert (stats.invocations, stats.failed, stats.succeeded, stats.retries) == (
        2,
        1,
        1,
        1,
    )
    print("recovered run:", recovered.status.value)
    print(
        "operations:",
        stats.invocations,
        "succeeded:",
        stats.succeeded,
        "failed:",
        stats.failed,
        "retries:",
        stats.retries,
    )
    print("invocation time:", stats.invocation_duration.total_seconds(), "seconds")

    # An explicit empty partition selection starts a new run with fresh counters.
    empty = asyncio.run(flow.run_async({"partition_keys": []}))
    empty_stats = empty.resource_statistics("remote-api")
    assert (
        empty_stats.invocations,
        empty_stats.succeeded,
        empty_stats.failed,
        empty_stats.retries,
        empty_stats.wait_count,
    ) == (0, 0, 0, 0, 0)
    print("new empty run counters:", empty_stats)


if __name__ == "__main__":
    main()
