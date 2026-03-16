"""Головне меню консольного UI."""
from __future__ import annotations

from InquirerPy import inquirer
from InquirerPy.separator import Separator
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()


def _print_header() -> None:
    """Виводить заголовок програми."""
    try:
        from dotenv import dotenv_values
        from pathlib import Path
        env = dotenv_values(Path(__file__).parent.parent.parent / ".env")
        server = env.get("OLAP_SERVER") or env.get("SERVER") or "—"
        auth = env.get("OLAP_AUTH_METHOD") or env.get("AUTH_METHOD") or "SSPI"
    except Exception:
        server, auth = "—", "—"

    text = Text()
    text.append("OLAP Export Tool\n", style="bold cyan")
    text.append(f"Сервер: ", style="dim")
    text.append(server, style="cyan")
    text.append(f"  ·  Auth: ", style="dim")
    text.append(auth, style="cyan")

    console.print(Panel(text, border_style="cyan", padding=(0, 2)))


def run() -> None:
    """Запускає цикл головного меню."""
    _print_header()

    while True:
        try:
            action = inquirer.select(
                message="Оберіть дію:",
                choices=[
                    {"name": "Експорт з OLAP куба", "value": "export"},
                    {"name": "Імпорт XLSX в аналітику", "value": "import"},
                    Separator(),
                    {"name": "Вийти", "value": "quit"},
                ],
                default="export",
            ).execute()
        except KeyboardInterrupt:
            console.print("\n[dim]До побачення.[/dim]")
            return

        if action == "export":
            try:
                from .olap_export import run_wizard as export_wizard
                export_wizard()
            except KeyboardInterrupt:
                console.print("\n[yellow]Скасовано.[/yellow]")
        elif action == "import":
            try:
                from .xlsx_import import run_wizard as import_wizard
                import_wizard()
            except KeyboardInterrupt:
                console.print("\n[yellow]Скасовано.[/yellow]")
        elif action == "quit":
            console.print("[dim]До побачення.[/dim]")
            return
