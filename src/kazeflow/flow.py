import asyncio
import logging
from graphlib import TopologicalSorter
from typing import Any, Optional

from .assets import get_asset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


class Flow:
    """A class representing a workflow of assets."""

    graph: dict[str, set[str]]
    ts: TopologicalSorter
    static_order: list[str]

    def __init__(self, asset_names: list[str]):
        self.graph = {}
        self.ts = self._set_ts(asset_names=asset_names)
        self.static_order = list(self.ts.static_order())
    
    def _set_ts(self, asset_names) -> TopologicalSorter:
        """Sets up the topological sorter based on asset dependencies."""
        
        for asset_name in asset_names:
            asset = get_asset(asset_name)
            self.graph[asset_name] = set(asset["deps"])
            for dep in asset["deps"]:
                if dep not in self.graph:
                    self._set_ts([dep])

        ts = TopologicalSorter(self.graph)
        return ts

    def run(self, config: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Executes the assets in the flow."""
        results: dict[str, Any] = {}
        for asset_name in self.static_order:
            results[asset_name] = self._execute_asset(asset_name, results, config or {})
        return results

    def _execute_asset(
        self,
        asset_name: str,
        results: dict[str, Any],
        config: dict[str, Any],
    ) -> Any:
        """Executes a single asset and its dependencies."""
        if asset_name in results:
            return results[asset_name]

        logger.info(f"Executing asset: {asset_name}")

        asset = get_asset(asset_name)

        for dep_name in asset["deps"]:
            self._execute_asset(dep_name, results, config)

        asset["func"]()
        logger.info(f"Finished executing asset: {asset_name}")


# ワーカーを複数起動（並列実行）
num_workers = 3


# DAGの定義
graph = {
    'E': {'C', 'D'},
    'D': {'B'},
    'C': {'A', 'B'},
    'B': {'A'},
    'A': set()
}

async def run_task(name):
    print(f"{name} start")
    await asyncio.sleep(1)  # 疑似処理
    print(f"{name} done")

async def main():
    ts = TopologicalSorter(graph)
    ts.prepare()
    
    queue = asyncio.Queue()

    # 初期で実行可能なノードをキューに入れる
    for node in ts.get_ready():
        queue.put_nowait(node)

    async def worker():
        while True:
            node = await queue.get()
            if node is None:
                break  # 終了シグナル
            await run_task(node)
            ts.done(node)
            # 処理後に新しく実行可能になったノードをキューに追加
            for ready_node in ts.get_ready():
                queue.put_nowait(ready_node)
            queue.task_done()

    tasks = [asyncio.create_task(worker()) for _ in range(num_workers)]

    # キューの処理が終わるまで待つ
    await queue.join()

    # ワーカーを終了させる
    for _ in range(num_workers):
        queue.put_nowait(None)
    await asyncio.gather(*tasks)

asyncio.run(main())
