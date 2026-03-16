"""Wizard: Імпорт XLSX в аналітичне сховище."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from prompt_toolkit.validation import ValidationError, Validator
from . import console, show_summary

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ─── Validators ──────────────────────────────────────────────────────────────

class YearValidator(Validator):
    def validate(self, document):
        text = document.text.strip()
        if text == "":
            return  # Опціонально
        if not text.isdigit() or not (2000 <= int(text) <= 2099):
            raise ValidationError(
                message="Рік: 4-цифрове число 2000–2099, або порожньо",
                cursor_position=len(text),
            )


class WeekValidator(Validator):
    def validate(self, document):
        text = document.text.strip()
        if text == "":
            return  # Опціонально
        if not text.isdigit() or not (1 <= int(text) <= 53):
            raise ValidationError(
                message="Тиждень: число 1–53, або порожньо",
                cursor_position=len(text),
            )


class WorkersValidator(Validator):
    def validate(self, document):
        text = document.text.strip()
        if not text.isdigit() or not (1 <= int(text) <= 32):
            raise ValidationError(
                message="Workers: ціле число 1–32",
                cursor_position=len(text),
            )


# ─── Helpers ─────────────────────────────────────────────────────────────────

TARGET_CHOICES = [
    Choice(value="ch",   name="ClickHouse"),
    Choice(value="duck", name="DuckDB"),
    Choice(value="pg",   name="PostgreSQL"),
]


# ─── Wizard ──────────────────────────────────────────────────────────────────

def run_wizard() -> None:
    """Інтерактивний wizard XLSX Import."""
    console.rule("[cyan]Імпорт XLSX в аналітику[/cyan]")

    # 1. Ціль
    target: str = inquirer.select(
        message="Ціль:",
        choices=TARGET_CHOICES,
        default="ch",
    ).execute()

    # 2. Директорія
    class DirectoryValidator(Validator):
        def validate(self, document):
            text = document.text.strip()
            if not text or not Path(text).exists():
                raise ValidationError(
                    message="Директорія не існує",
                    cursor_position=len(text),
                )

    directory: str = inquirer.text(
        message="Директорія з XLSX:",
        default="result/",
        validate=DirectoryValidator(),
    ).execute()

    # 3. Рік (опційно)
    year: str = inquirer.text(
        message="Рік (Enter — всі роки):",
        default="",
        validate=YearValidator(),
    ).execute()

    # 4. Тиждень (опційно)
    week: str = inquirer.text(
        message="Тиждень (Enter — всі тижні):",
        default="",
        validate=WeekValidator(),
    ).execute()

    # 5. Workers
    workers: str = inquirer.text(
        message="Workers (паралельні потоки):",
        default="4",
        validate=WorkersValidator(),
    ).execute()

    # 6. Dry run
    dry_run: bool = inquirer.confirm(
        message="Dry run (без запису в БД)?",
        default=False,
    ).execute()

    # 7. Підсумок
    summary = {
        "Ціль":       target,
        "Директорія": directory,
        "Рік":        year or "(всі)",
        "Тиждень":    week or "(всі)",
        "Workers":    workers,
        "Dry Run":    "так" if dry_run else "ні",
    }
    show_summary(summary)

    # 8. Підтвердження
    confirmed: bool = inquirer.confirm(
        message="Запустити?",
        default=True,
    ).execute()

    if not confirmed:
        console.print("[yellow]Скасовано.[/yellow]")
        return

    # 9. Будуємо argv і запускаємо через importlib
    script_args = [
        "scripts/import_xlsx.py",
        "--target", target,
        "--dir", directory,
        "--workers", workers,
    ]
    if year:
        script_args += ["--year", year]
    if week:
        script_args += ["--week", week]
    if dry_run:
        script_args.append("--dry-run")

    console.print(f"[dim]▶ python {' '.join(script_args)}[/dim]\n")

    script_path = _PROJECT_ROOT / "scripts" / "import_xlsx.py"
    spec = importlib.util.spec_from_file_location("import_xlsx", script_path)
    if spec is None or spec.loader is None:
        console.print(f"[red]✗ Не вдалося завантажити: {script_path}[/red]")
        return

    old_argv = sys.argv
    sys.argv = script_args
    try:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        mod.main()
        console.print("\n[bold green]✓ Імпорт завершено[/bold green]")
    except SystemExit as e:
        if e.code not in (0, None):
            console.print(f"\n[bold red]✗ Завершено з кодом {e.code}[/bold red]")
        else:
            console.print("\n[bold green]✓ Імпорт завершено[/bold green]")
    except Exception as exc:
        console.print(f"\n[bold red]✗ Помилка: {exc}[/bold red]")
    finally:
        sys.argv = old_argv
