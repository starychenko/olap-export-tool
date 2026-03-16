"""Консольний інтерактивний UI (InquirerPy + rich)."""

from rich.console import Console
from rich.table import Table

console = Console()


def show_summary(params: dict[str, str]) -> None:
    """Відображає таблицю-підсумок параметрів перед запуском."""
    table = Table(show_header=False, border_style="cyan", box=None, padding=(0, 1))
    table.add_column(style="dim cyan", no_wrap=True)
    table.add_column(style="white")
    for key, value in params.items():
        table.add_row(key, value)
    console.print()
    console.print(table)
    console.print()
