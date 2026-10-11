"""Run a flow that explicitly opens a resource circuit after a fatal error."""

import asyncio

from kazeflow.assets import AssetRegistry
from kazeflow.flow import Flow
from kazeflow.resources import Fail, OpenCircuit, ResourcePolicy


def main() -> None:
    registry = AssetRegistry()

    async def fetch():
        raise OSError("upstream credentials were rejected")

    async def publish():
        return "this body is skipped after the resource circuit opens"

    async def summarize():
        return "independent work can continue"

    def classify(error: Exception) -> Fail | OpenCircuit:
        if isinstance(error, OSError):
            return OpenCircuit()
        return Fail()

    registry.register(fetch, resource="remote-api")
    registry.register(publish, resource="remote-api")
    registry.register(summarize)
    result = asyncio.run(
        Flow(
            ["fetch", "publish", "summarize"],
            registry=registry,
            resources={"remote-api": ResourcePolicy(1, classify_failure=classify)},
        ).run_async()
    )
    print(f"run: {result.status.value}")
    for task in result.tasks:
        attempt = task.attempts[0]
        print(f"{task.task.task_name}: {attempt.status.value}")


if __name__ == "__main__":
    main()
