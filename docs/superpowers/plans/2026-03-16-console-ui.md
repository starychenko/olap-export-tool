# Console UI Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Замінити Textual TUI на консольний інтерактивний інтерфейс (InquirerPy + rich) зі стрілковою навігацією.

**Architecture:** Новий пакет `olap_tool/ui/` з трьома модулями: `menu.py` (головне меню), `olap_export.py` (wizard OLAP Export), `xlsx_import.py` (wizard XLSX Import). Entry point `olap.py` без аргументів запускає `ui.menu.run()`. CLI режим не змінюється.

**Tech Stack:** `InquirerPy` (arrow-key select/input), `rich` (panels, tables, status — вже в requirements), `colorama` (вже є).

---

## File Map

| Дія | Файл | Відповідальність |
|-----|------|-----------------|
| Видалити | `olap_tool/tui/` | весь TUI пакет |
| Змінити | `olap_tool/core/utils.py` | прибрати `TUIStream` |
| Змінити | `requirements.txt` | прибрати `textual`, додати `InquirerPy` |
| Змінити | `olap.py` | запускати `ui.menu` замість TUI |
| Створити | `olap_tool/ui/__init__.py` | пустий init |
| Створити | `olap_tool/ui/menu.py` | цикл головного меню |
| Створити | `olap_tool/ui/olap_export.py` | wizard OLAP Export |
| Створити | `olap_tool/ui/xlsx_import.py` | wizard XLSX Import |

---

## Chunk 1: Cleanup — видалити TUI, оновити залежності

### Task 1: Оновити requirements.txt

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Видалити рядок `textual`, додати `InquirerPy`**

Знайти рядок:
```
textual>=0.70.0  # TUI фреймворк для інтерактивного меню
```
Замінити на:
```
InquirerPy>=0.3.4  # Консольне інтерактивне меню зі стрілковою навігацією
```

- [ ] **Step 2: Встановити нову залежність**

```bash
pip install InquirerPy
```

Очікуваний вивід: `Successfully installed InquirerPy-...`

- [ ] **Step 3: Перевірити імпорт**

```bash
python -c "from InquirerPy import inquirer; print('InquirerPy OK')"
```

Очікуваний вивід: `InquirerPy OK`

---

### Task 2: Видалити TUIStream з utils.py

**Files:**
- Modify: `olap_tool/core/utils.py`

- [ ] **Step 1: Видалити клас TUIStream і його імпорти**

Знайти і видалити весь блок від коментаря до кінця класу:
```python
# ---------------------------------------------------------------------------
# TUI stdout redirect
# ---------------------------------------------------------------------------
import re as _re
import io as _io

_ANSI_ESCAPE = _re.compile(r"\x1b\[[0-9;]*m")


class TUIStream:
    ...
    def fileno(self):
        raise _io.UnsupportedOperation("no fileno")
```

- [ ] **Step 2: Перевірити, що utils.py імпортується**

```bash
python -c "from olap_tool.core.utils import print_info, init_utils; print('utils OK')"
```

Очікуваний вивід: `utils OK`

---

### Task 3: Видалити пакет olap_tool/tui/

**Files:**
- Delete: `olap_tool/tui/` (весь каталог)

- [ ] **Step 1: Видалити каталог**

```bash
rm -rf olap_tool/tui/
```

- [ ] **Step 2: Перевірити, що основні модулі ще імпортуються**

```bash
python -c "from olap_tool.core.runner import main; from olap_tool.sinks import ClickHouseSink; print('core OK')"
```

Очікуваний вивід: `core OK`

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: видалити TUI (textual), TUIStream; додати InquirerPy"
```

---

## Chunk 2: Головне меню і OLAP Export wizard

### Task 4: Створити olap_tool/ui/__init__.py

**Files:**
- Create: `olap_tool/ui/__init__.py`

- [ ] **Step 1: Створити порожній init**

```python
"""Консольний інтерактивний UI (InquirerPy + rich)."""
```

---

### Task 5: Створити olap_tool/ui/menu.py

**Files:**
- Create: `olap_tool/ui/menu.py`

- [ ] **Step 1: Написати модуль**

```python
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
```

- [ ] **Step 2: Перевірити імпорт**

```bash
python -c "from olap_tool.ui.menu import run; print('menu OK')"
```

Очікуваний вивід: `menu OK`

---

### Task 6: Створити olap_tool/ui/olap_export.py

**Files:**
- Create: `olap_tool/ui/olap_export.py`

- [ ] **Step 1: Написати модуль**

```python
"""Wizard: Експорт з OLAP куба."""
from __future__ import annotations

import re
import sys
from pathlib import Path

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator
from InquirerPy.validator import EmptyInputValidator
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

    if result == 0:
        console.print("\n[bold green]✓ Завершено успішно[/bold green]")
    else:
        console.print(f"\n[bold red]✗ Завершено з помилкою (код {result})[/bold red]")
```

- [ ] **Step 2: Перевірити імпорт**

```bash
python -c "from olap_tool.ui.olap_export import run_wizard; print('olap_export OK')"
```

Очікуваний вивід: `olap_export OK`

- [ ] **Step 3: Commit**

```bash
git add olap_tool/ui/
git commit -m "feat: olap_tool/ui — головне меню та wizard OLAP Export (InquirerPy + rich)"
```

---

## Chunk 3: XLSX Import wizard і оновлення entry point

### Task 7: Створити olap_tool/ui/xlsx_import.py

**Files:**
- Create: `olap_tool/ui/xlsx_import.py`

- [ ] **Step 1: Написати модуль**

```python
"""Wizard: Імпорт XLSX в аналітичне сховище."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from prompt_toolkit.validation import ValidationError, Validator
from rich.console import Console
from rich.table import Table

console = Console()

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
    directory: str = inquirer.text(
        message="Директорія з XLSX:",
        default="result/",
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
    _show_summary(summary)

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
```

- [ ] **Step 2: Перевірити імпорт**

```bash
python -c "from olap_tool.ui.xlsx_import import run_wizard; print('xlsx_import OK')"
```

Очікуваний вивід: `xlsx_import OK`

---

### Task 8: Оновити olap.py

**Files:**
- Modify: `olap.py`

- [ ] **Step 1: Замінити TUI-логіку на ui.menu**

Поточний вміст:
```python
#!/usr/bin/env python3
"""
OLAP Export Tool — точка входу.

Без аргументів → запускає Textual TUI.
З аргументами → CLI режим.
"""
import sys
from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except Exception:
        pass

if len(sys.argv) == 1:
    import os
    from olap_tool.tui.app import OlapApp
    try:
        OlapApp().run()
    except KeyboardInterrupt:
        pass
    finally:
        # Примусово завершуємо всі фонові потоки (наприклад, завислі запити до БД)
        os._exit(0)
else:
    from olap_tool.core.runner import main
    sys.exit(main())
```

Замінити на:
```python
#!/usr/bin/env python3
"""
OLAP Export Tool — точка входу.

Без аргументів → консольне інтерактивне меню.
З аргументами → CLI режим.
"""
import sys
from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except Exception:
        pass

if len(sys.argv) == 1:
    from olap_tool.ui.menu import run
    run()
else:
    from olap_tool.core.runner import main
    sys.exit(main())
```

- [ ] **Step 2: Перевірити синтаксис**

```bash
python -c "import ast; ast.parse(open('olap.py').read()); print('olap.py syntax OK')"
```

Очікуваний вивід: `olap.py syntax OK`

- [ ] **Step 3: Перевірити CLI режим (без підключення до OLAP)**

```bash
python olap.py --help
```

Очікуваний вивід: usage message (список CLI аргументів).

- [ ] **Step 4: Commit**

```bash
git add olap.py olap_tool/ui/xlsx_import.py
git commit -m "feat: xlsx_import wizard + оновити olap.py (запуск ui.menu)"
```

---

## Chunk 4: Фінальна перевірка і cleanup

### Task 9: Повна перевірка імпортів і чистота коду

- [ ] **Step 1: Переконатися, що textual не залишився ніде**

```bash
grep -r "textual\|TUIStream\|from olap_tool.tui" olap_tool/ olap.py scripts/ --include="*.py"
```

Очікуваний вивід: порожній (жодних знайдених рядків).

- [ ] **Step 2: Перевірити всі нові модулі UI**

```bash
python -c "
from olap_tool.ui.menu import run
from olap_tool.ui.olap_export import run_wizard
from olap_tool.ui.xlsx_import import run_wizard as import_wizard
print('All UI modules OK')
"
```

Очікуваний вивід: `All UI modules OK`

- [ ] **Step 3: Перевірити CLI режим**

```bash
python olap.py --list-profiles
```

Очікуваний вивід: список профілів або повідомлення "профілів не знайдено".

- [ ] **Step 4: Фінальний commit**

```bash
git add -A
git commit -m "chore: фінальна перевірка — Console UI готовий, TUI видалено"
```

- [ ] **Step 5: Оновити пам'ять**

Оновити `MEMORY.md`: замінити згадки про TUI на Console UI (InquirerPy + rich).
