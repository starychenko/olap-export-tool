"""Wizard: Експорт з OLAP куба."""
from __future__ import annotations

import re
import sys
from pathlib import Path

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator
from prompt_toolkit.validation import ValidationError, Validator
from rich.console import Console
from rich.table import Table

console = Console()


# ─── Validators ──────────────────────────────────────────────────────────────

class WeeksValidator(Validator):
    def validate(self, document):
        text = document.text.strip()
        if not text.isdigit() or not (1 <= int(text) <= 520):
            raise ValidationError(
                message="Введіть ціле число від 1 до 520",
                cursor_position=len(text),
            )


class ManualPeriodValidator(Validator):
    _PATTERN = re.compile(r"^\d{4}-\d{2}:\d{4}-\d{2}$")

    def validate(self, document):
        text = document.text.strip()
        if not self._PATTERN.match(text):
            raise ValidationError(
                message="Формат: YYYY-WW:YYYY-WW  (наприклад 2025-01:2025-12)",
                cursor_position=len(text),
            )


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _list_profiles() -> list[Choice]:
    """Повертає список профілів для InquirerPy fuzzy-select."""
    profiles_dir = Path(__file__).parent.parent.parent / "profiles"
    choices: list[Choice] = [Choice(value="", name="(без профілю)")]
    if profiles_dir.exists():
        for p in sorted(profiles_dir.glob("*.yaml")):
            choices.append(Choice(value=p.stem, name=p.stem))
    return choices


def _show_summary(params: dict[str, str]) -> None:
    table = Table(show_header=False, border_style="cyan", box=None, padding=(0, 1))
    table.add_column(style="dim cyan", no_wrap=True)
    table.add_column(style="white")
    for key, value in params.items():
        table.add_row(key, value)
    console.print()
    console.print(table)
    console.print()


# ─── Wizard ──────────────────────────────────────────────────────────────────

FORMAT_CHOICES = [
    Choice(value="xlsx",       name="XLSX"),
    Choice(value="csv",        name="CSV"),
    Choice(value="both",       name="XLSX + CSV"),
    Separator(),
    Choice(value="ch",         name="ClickHouse"),
    Choice(value="duck",       name="DuckDB"),
    Choice(value="pg",         name="PostgreSQL"),
]

PERIOD_CHOICES = [
    Choice(value="last-weeks",       name="Останні N тижнів"),
    Choice(value="current-month",    name="Поточний місяць"),
    Choice(value="last-month",       name="Попередній місяць"),
    Choice(value="current-quarter",  name="Поточний квартал"),
    Choice(value="last-quarter",     name="Попередній квартал"),
    Choice(value="year-to-date",     name="З початку року"),
    Choice(value="manual",           name="Ручний діапазон YYYY-WW:YYYY-WW"),
]

COMPRESS_CHOICES = [
    Choice(value="none", name="Без стиснення"),
    Choice(value="zip",  name="ZIP архів"),
]

_PERIOD_LABELS = {
    "last-weeks":      "last-weeks",
    "current-month":   "поточний місяць",
    "last-month":      "попередній місяць",
    "current-quarter": "поточний квартал",
    "last-quarter":    "попередній квартал",
    "year-to-date":    "з початку року",
    "manual":          "ручний діапазон",
}


def run_wizard() -> None:
    """Інтерактивний wizard OLAP Export."""
    console.rule("[cyan]Експорт з OLAP куба[/cyan]")

    # 1. Профіль
    profile: str = inquirer.fuzzy(
        message="Профіль:",
        choices=_list_profiles(),
        default="",
        max_height="40%",
    ).execute()

    # 2. Формат
    fmt: str = inquirer.select(
        message="Формат виводу:",
        choices=FORMAT_CHOICES,
        default="xlsx",
    ).execute()

    # 3. Тип періоду
    period_type: str = inquirer.select(
        message="Тип періоду:",
        choices=PERIOD_CHOICES,
        default="last-weeks",
    ).execute()

    # 4. Значення (тільки для last-weeks і manual)
    period_value: str = ""
    if period_type == "last-weeks":
        period_value = inquirer.text(
            message="Кількість тижнів:",
            default="4",
            validate=WeeksValidator(),
        ).execute()
    elif period_type == "manual":
        period_value = inquirer.text(
            message="Діапазон (YYYY-WW:YYYY-WW):",
            validate=ManualPeriodValidator(),
        ).execute()

    # 5. Стиснення
    compress: str = inquirer.select(
        message="Стиснення:",
        choices=COMPRESS_CHOICES,
        default="none",
    ).execute()

    # 6. Підсумок
    period_label = _PERIOD_LABELS.get(period_type, period_type)
    if period_value:
        period_label = f"{period_label} ({period_value})"
    summary = {
        "Профіль":   profile or "(без профілю)",
        "Формат":    fmt,
        "Період":    period_label,
        "Стиснення": compress,
    }
    _show_summary(summary)

    # 7. Підтвердження
    confirmed: bool = inquirer.confirm(
        message="Запустити?",
        default=True,
    ).execute()

    if not confirmed:
        console.print("[yellow]Скасовано.[/yellow]")
        return

    # 8. Будуємо argv і запускаємо
    argv = ["olap.py"]
    if profile:
        argv += ["--profile", profile]
    argv += ["--format", fmt]

    if period_type == "last-weeks":
        argv += ["--last-weeks", period_value or "4"]
    elif period_type == "current-month":
        argv.append("--current-month")
    elif period_type == "last-month":
        argv.append("--last-month")
    elif period_type == "current-quarter":
        argv.append("--current-quarter")
    elif period_type == "last-quarter":
        argv.append("--last-quarter")
    elif period_type == "year-to-date":
        argv.append("--year-to-date")
    elif period_type == "manual" and period_value:
        argv += ["--period", period_value]

    if compress != "none":
        argv += ["--compress", compress]

    console.print(f"[dim]▶ {' '.join(argv)}[/dim]\n")

    from olap_tool.core.runner import main as runner_main
    old_argv = sys.argv
    sys.argv = argv
    try:
        result = runner_main()
    except SystemExit as e:
        result = e.code if isinstance(e.code, int) else 0
    finally:
        sys.argv = old_argv

    if (result or 0) == 0:
        console.print("\n[bold green]✓ Завершено успішно[/bold green]")
    else:
        console.print(f"\n[bold red]✗ Завершено з помилкою (код {result})[/bold red]")
