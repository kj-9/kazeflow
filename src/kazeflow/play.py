import asyncio
import time
from graphlib import TopologicalSorter


async def task(name, delay=1, records: list = []):
    start = time.time()
    print(f"start {name}")
    await asyncio.sleep(delay)
    end = time.time()
    print(f"done {name}")

    records.append((name, start, end))
    return name


dag = {
    "A": [],
    "B": ["A"],
    "C": ["A"],
    "D": ["B", "C"],
    "E": ["C"],
    "F": ["C"],
}


async def run_tasks():
    ts = TopologicalSorter(dag)
    ts.prepare()

    records = []
    running = {
        asyncio.create_task(task(n, delay=1 + len(n), records=records)): n
        for n in ts.get_ready()
    }

    while running:
        done, _ = await asyncio.wait(
            running.keys(), return_when=asyncio.FIRST_COMPLETED
        )

        for d in done:
            name = running.pop(d)
            ts.done(name)
            for new in ts.get_ready():
                running[
                    asyncio.create_task(task(new, delay=1 + len(new), records=records))
                ] = new

    return records


def text_gantt(records):
    base = min(start for _, start, _ in records)
    end_time = max(end for _, _, end in records)
    length = int(end_time - base) + 1  # 秒単位

    tasks = sorted(set(name for name, _, _ in records))

    lines = []
    for name in tasks:
        line = [" "] * length
        for n, start, end in records:
            if n == name:
                s = int(start - base)
                e = int(end - base)
                for i in range(s, e):
                    line[i] = "#"
        lines.append(f"{name:>3} |{''.join(line)}|")
    return "\n".join(lines)


def text_graph(dag):
    lines = []
    for node, deps in dag.items():
        if deps:
            for dep in deps:
                lines.append(f"{dep} --> {node}")
        else:
            lines.append(f"{node}")
    return "\n".join(lines)


if __name__ == "__main__":
    print("\nDependency Graph (text-based):")
    print(text_graph(dag))
    records = asyncio.run(run_tasks())
    print("\nGantt Chart (text-based):")
    print(text_gantt(records))
