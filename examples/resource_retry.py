"""Run a small flow with an explicitly classified transient resource failure."""

import asyncio

from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.flow import Flow
from kazeflow.resources import Fail, ResourcePolicy, Retry


def main() -> None:
    registry = AssetRegistry()
    invocations = 0

    async def fetch(context: AssetContext) -> str:
        nonlocal invocations
        invocations += 1
        print(f"fetch invocation {context.invocation_number}")
        if invocations == 1:
            raise TimeoutError("temporary upstream timeout")
        return "response"

    def classify(error: Exception) -> Fail | Retry:
        if isinstance(error, TimeoutError):
            return Retry(after_seconds=0.05, cooldown_resource=True)
        return Fail()

    registry.register(fetch, resource="remote-api")
    flow = Flow(
        ["fetch"],
        registry=registry,
        resources={
            "remote-api": ResourcePolicy(
                2,
                max_attempts=3,
                classify_failure=classify,
            )
        },
    )
    result = asyncio.run(flow.run_async())
    print(f"run: {result.status.value}")
    for invocation in result.tasks[0].attempts[0].invocations:
        print(f"invocation {invocation.number}: {invocation.status.value}")


if __name__ == "__main__":
    main()
