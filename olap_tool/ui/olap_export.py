"""Wizard: Експорт з OLAP куба."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator
from prompt_toolkit.validation import ValidationError, Validator
from . import console, show_summary


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


def _load_profile_defaults(profile_name: str) -> dict[str, Any]:
    """Читає профіль і повертає defaults для wizard (format, period_type, period_value, compress)."""
    defaults: dict[str, Any] = {}
    if not profile_name:
        return defaults

    from olap_tool.core.profiles import load_profile
    profile_data = load_profile(profile_name)
    if not profile_data:
        return defaults

    # format
    fmt = profile_data.get("export", {}).get("format")
    if fmt:
        defaults["format"] = fmt

    # compress
    compress = profile_data.get("export", {}).get("compress")
    if compress:
        defaults["compress"] = compress

    # period
    period_cfg = profile_data.get("period", {})
    period_type = period_cfg.get("type")
    if period_type == "auto":
        auto_type = period_cfg.get("auto_type")
        auto_value = period_cfg.get("auto_value")
        if auto_type:
            defaults["period_type"] = auto_type
        if auto_value is not None:
            defaults["period_value"] = str(auto_value)
    elif period_type == "manual":
        start = period_cfg.get("start", "")
        end = period_cfg.get("end", "")
        if start and end:
            defaults["period_type"] = "manual"
            defaults["period_value"] = f"{start}:{end}"

    return defaults


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

_FORMAT_VALUES = [c.value for c in FORMAT_CHOICES if isinstance(c, Choice)]

PERIOD_CHOICES = [
    Choice(value="last-weeks",       name="Останні N тижнів"),
    Choice(value="current-month",    name="Поточний місяць"),
    Choice(value="last-month",       name="Попередній місяць"),
    Choice(value="current-quarter",  name="Поточний квартал"),
    Choice(value="last-quarter",     name="Попередній квартал"),
    Choice(value="year-to-date",     name="З початку року"),
    Choice(value="manual",           name="Ручний діапазон YYYY-WW:YYYY-WW"),
]

_PERIOD_VALUES = [c.value for c in PERIOD_CHOICES if isinstance(c, Choice)]

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

    # Читаємо defaults з профілю для pre-populate наступних кроків
    p_defaults = _load_profile_defaults(profile)

    # 2. Формат (default з профілю або xlsx)
    fmt_default = p_defaults.get("format", "xlsx")
    if fmt_default not in _FORMAT_VALUES:
        fmt_default = "xlsx"
    fmt: str = inquirer.select(
        message="Формат виводу:",
        choices=FORMAT_CHOICES,
        default=fmt_default,
    ).execute()

    # 3. Тип періоду (default з профілю або last-weeks)
    period_default = p_defaults.get("period_type", "last-weeks")
    if period_default not in _PERIOD_VALUES:
        period_default = "last-weeks"
    period_type: str = inquirer.select(
        message="Тип періоду:",
        choices=PERIOD_CHOICES,
        default=period_default,
    ).execute()

    # 4. Значення (тільки для last-weeks і manual)
    period_value: str = ""
    if period_type == "last-weeks":
        weeks_default = p_defaults.get("period_value", "4") if period_type == p_defaults.get("period_type") else "4"
        period_value = inquirer.text(
            message="Кількість тижнів:",
            default=weeks_default,
            validate=WeeksValidator(),
        ).execute()
    elif period_type == "manual":
        manual_default = p_defaults.get("period_value", "") if period_type == p_defaults.get("period_type") else ""
        period_value = inquirer.text(
            message="Діапазон (YYYY-WW:YYYY-WW):",
            default=manual_default,
            validate=ManualPeriodValidator(),
        ).execute()

    # 5. Стиснення (default з профілю або none)
    compress_default = p_defaults.get("compress", "none")
    if compress_default not in ("none", "zip"):
        compress_default = "none"
    compress: str = inquirer.select(
        message="Стиснення:",
        choices=COMPRESS_CHOICES,
        default=compress_default,
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
    show_summary(summary)

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
    try:
        result = runner_main(argv=argv)
    except SystemExit as e:
        result = e.code if isinstance(e.code, int) else 0

    if (result or 0) == 0:
        console.print("\n[bold green]✓ Завершено успішно[/bold green]")
    else:
        console.print(f"\n[bold red]✗ Завершено з помилкою (код {result})[/bold red]")
