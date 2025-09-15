from rich.console import Console
from rich.panel import Panel

console = Console()


def draw_simple_dag():
    # ノード
    a = Panel("A", style="green", expand=False)
    b = Panel("B", style="yellow", expand=False)
    c = Panel("C", style="yellow", expand=False)
    d = Panel("D", style="grey50", expand=False)

    lines = [
        f"    {a}",
        "      │",
        "      ▼",
        f"{b}   {c}",
        "   │     │",
        "   └──┬──┘",
        "      ▼",
        f"    {d}",
    ]

    for line in lines:
        console.print(line)


draw_simple_dag()
