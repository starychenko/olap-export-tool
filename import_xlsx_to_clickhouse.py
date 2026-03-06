#!/usr/bin/env python
"""
Імпорт існуючих Excel-файлів у ClickHouse (паралельний режим).

Використання:
    python import_xlsx_to_clickhouse.py                       # всі файли, 4 воркери
    python import_xlsx_to_clickhouse.py --workers 8           # 8 паралельних воркерів
    python import_xlsx_to_clickhouse.py --year 2025           # тільки 2025 рік
    python import_xlsx_to_clickhouse.py --year 2025 --week 10 # тільки тиждень 10
    python import_xlsx_to_clickhouse.py --dry-run             # показати файли без завантаження
"""

import sys
import argparse
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
except Exception:
    pass

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text
from rich import box

from olap_tool.config import load_clickhouse_from_env
from olap_tool.clickhouse_export import (
    export_to_clickhouse,
    create_client,
    ensure_database,
    ensure_table,
    get_table_schema,
    sanitize_df,
)
from olap_tool.utils import init_utils

init_utils(ascii_logs=False)

console = Console()

# ---------------------------------------------------------------------------
# Excel engine: calamine (Rust) з fallback на openpyxl
# ---------------------------------------------------------------------------
try:
    import python_calamine  # noqa: F401
    _EXCEL_ENGINE = "calamine"
except ImportError:
    _EXCEL_ENGINE = "openpyxl"

# ---------------------------------------------------------------------------
# Thread-local клієнти: одне з'єднання на потік, не перевідкривається
# ---------------------------------------------------------------------------
_thread_local = threading.local()
_all_clients: list = []
_all_clients_lock = threading.Lock()


def _get_thread_client(cfg):
    if not hasattr(_thread_local, "client") or _thread_local.client is None:
        client = create_client(cfg)
        _thread_local.client = client
        with _all_clients_lock:
            _all_clients.append(client)
    return _thread_local.client


# ---------------------------------------------------------------------------
# Файловий пошук
# ---------------------------------------------------------------------------

def find_xlsx_files(
    base_dir: Path,
    year: Optional[int],
    week: Optional[int],
) -> list[tuple[Path, int, int]]:
    pattern = re.compile(r"^(\d{4})-(\d{2})\.xlsx$")
    results = []
    for f in sorted(base_dir.rglob("*.xlsx")):
        m = pattern.match(f.name)
        if not m:
            continue
        y, w = int(m.group(1)), int(m.group(2))
        if year is not None and y != year:
            continue
        if week is not None and w != week:
            continue
        results.append((f, y, w))
    return results


def _read_excel(file_path: Path, sheet) -> pd.DataFrame:
    try:
        return pd.read_excel(str(file_path), sheet_name=sheet, engine=_EXCEL_ENGINE)
    except Exception:
        if _EXCEL_ENGINE != "openpyxl":
            return pd.read_excel(str(file_path), sheet_name=sheet, engine="openpyxl")
        raise


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def process_file(
    file_path: Path,
    year: int,
    week: int,
    cfg,
    sheet,
    cached_schema: Optional[dict],
) -> tuple[int, bool, float]:
    """
    Повертає (row_count, success, elapsed_sec).
    Весь вивід заглушено — rich progress відображає стан у головному потоці.
    """
    t0 = time.monotonic()
    try:
        df = _read_excel(file_path, sheet)
    except Exception as e:
        return 0, False, time.monotonic() - t0

    if df.empty:
        return 0, True, time.monotonic() - t0

    client = _get_thread_client(cfg)
    rows = export_to_clickhouse(
        df, cfg, year=year, week=week,
        client=client,
        schema=cached_schema,
        silent=True,          # воркери мовчать — вивід тільки через rich
    )
    return rows, rows >= 0, time.monotonic() - t0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Паралельний імпорт Excel файлів OLAP-експорту у ClickHouse"
    )
    parser.add_argument("--dir",     default="result", help="Базова директорія")
    parser.add_argument("--year",    type=int, default=None, help="Фільтр за роком")
    parser.add_argument("--week",    type=int, default=None, help="Фільтр за тижнем")
    parser.add_argument("--sheet",   default="0",            help="Аркуш Excel (назва або індекс)")
    parser.add_argument("--workers", type=int, default=4,    help="Паралельних воркерів")
    parser.add_argument("--dry-run", action="store_true",    help="Показати файли без завантаження")
    args = parser.parse_args()

    base_dir = Path(args.dir)
    if not base_dir.exists():
        console.print(f"[red]❌ Директорія не знайдена: {base_dir}[/red]")
        return 1

    cfg = load_clickhouse_from_env()

    # ── Заголовок ──────────────────────────────────────────────────────────
    info = Table.grid(padding=(0, 2))
    info.add_column(style="cyan")
    info.add_column(style="white")
    info.add_row("Директорія",  str(base_dir.resolve()))
    info.add_row("ClickHouse",  f"{cfg.host}:{cfg.port}  →  {cfg.database}.{cfg.table}")
    info.add_row("Excel engine", _EXCEL_ENGINE)
    if args.year:
        info.add_row("Рік", str(args.year))
    if args.week:
        info.add_row("Тиждень", str(args.week))
    if not args.dry_run:
        info.add_row("Воркери", str(args.workers))

    console.print()
    console.print(Panel(
        info,
        title="[bold cyan]ІМПОРТ EXCEL → CLICKHOUSE[/bold cyan]",
        border_style="cyan",
        expand=False,
    ))
    console.print()

    # ── Пошук файлів ───────────────────────────────────────────────────────
    files = find_xlsx_files(base_dir, args.year, args.week)
    if not files:
        console.print("[yellow]⚠️  Файлів не знайдено за вказаними параметрами[/yellow]")
        return 0

    console.print(f"  [cyan]Знайдено файлів:[/cyan] [white bold]{len(files)}[/white bold]\n")

    if args.dry_run:
        for i, (fp, y, w) in enumerate(files, 1):
            console.print(f"  [dim]{i:>4}.[/dim] [white]{fp}[/white]  [yellow]({y}-{w:02d})[/yellow]")
        console.print(f"\n[yellow]DRY RUN завершено. Файлів: {len(files)}[/yellow]")
        return 0

    # Sheet: int або str
    sheet: str | int = args.sheet
    try:
        sheet = int(sheet)
    except (ValueError, TypeError):
        pass

    # ── Ініціалізація: БД + таблиця + схема — один раз ────────────────────
    with console.status("[cyan]Ініціалізація ClickHouse...[/cyan]", spinner="dots"):
        try:
            init_client = create_client(cfg)
            ensure_database(init_client, cfg.database)
            df_init = _read_excel(files[0][0], sheet)
            if not df_init.empty:
                ensure_table(init_client, cfg.database, cfg.table, sanitize_df(df_init))
            cached_schema = get_table_schema(init_client, cfg.database, cfg.table)
            init_client.close()
        except Exception as e:
            console.print(f"[red]❌ Помилка ініціалізації: {e}[/red]")
            return 1

    console.print(
        f"  [green]✅ Ініціалізовано[/green]  "
        f"[dim]схема: {len(cached_schema)} колонок[/dim]\n"
    )

    # ── Паралельне завантаження з rich progress bar ────────────────────────
    total = len(files)
    total_rows = 0
    errors = 0
    start_time = time.monotonic()

    progress = Progress(
        SpinnerColumn(),
        BarColumn(bar_width=36),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TextColumn("[dim]•[/dim]"),
        TimeElapsedColumn(),
        TextColumn("[dim]•[/dim] ETA"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )

    task_id = progress.add_task("", total=total)

    with progress:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    process_file,
                    fp, y, w, cfg, sheet, cached_schema,
                ): (fp, y, w)
                for fp, y, w in files
            }

            for future in as_completed(futures):
                fp, y, w = futures[future]
                try:
                    rows, success, elapsed = future.result()
                except Exception as e:
                    rows, success, elapsed = 0, False, 0.0
                    progress.console.print(
                        f"  [red]❌ {y}-{w:02d}[/red]  [dim]{fp.name}[/dim]  "
                        f"[red]{e}[/red]"
                    )

                total_rows += rows
                if not success:
                    errors += 1

                # Один рядок на завершений файл
                icon = "[green]✅[/green]" if success else "[red]❌[/red]"
                rows_str = f"[white]{rows:>7,}[/white] рядків" if rows > 0 else "[dim]    порожній[/dim]"
                progress.console.print(
                    f"  {icon} [cyan]{y}-{w:02d}[/cyan]  "
                    f"{rows_str}  "
                    f"[dim]{elapsed:.1f}с[/dim]"
                )

                # Оновлюємо лічильник рядків у progress description
                elapsed_total = time.monotonic() - start_time
                rate = (total_rows / elapsed_total) if elapsed_total > 0 else 0
                progress.update(
                    task_id,
                    advance=1,
                    description=(
                        f"[white bold]{total_rows:,}[/white bold] рядків  "
                        f"[dim]{rate:,.0f} рядків/с[/dim]"
                    ),
                )

    # Закриваємо thread-local клієнти
    for client in _all_clients:
        try:
            client.close()
        except Exception:
            pass

    # ── Підсумок ───────────────────────────────────────────────────────────
    elapsed_total = time.monotonic() - start_time
    rate_files = total / elapsed_total if elapsed_total > 0 else 0
    rate_rows  = total_rows / elapsed_total if elapsed_total > 0 else 0

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="cyan")
    summary.add_column(style="white bold")
    summary.add_row("Файлів оброблено",  f"{total - errors}/{total}")
    summary.add_row("Рядків завантажено", f"{total_rows:,}")
    summary.add_row("Час",               f"{elapsed_total:.1f} с")
    summary.add_row("Швидкість",         f"{rate_files:.1f} файл/с  ·  {rate_rows:,.0f} рядків/с")
    if errors:
        summary.add_row("[red]Помилок[/red]", f"[red]{errors}[/red]")

    border = "green" if not errors else "yellow"
    title  = "[bold green]✅ Імпорт завершено[/bold green]" if not errors else "[bold yellow]⚠️  Завершено з помилками[/bold yellow]"

    console.print()
    console.print(Panel(summary, title=title, border_style=border, expand=False))
    console.print()

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
