#!/usr/bin/env python
"""
Імпорт існуючих Excel-файлів у DuckDB через REST API (паралельний режим).

Використання:
    python import_xlsx_to_duckdb.py                       # всі файли, 4 воркери
    python import_xlsx_to_duckdb.py --workers 8           # 8 паралельних воркерів
    python import_xlsx_to_duckdb.py --year 2025           # тільки 2025 рік
    python import_xlsx_to_duckdb.py --year 2025 --week 10 # тільки тиждень 10
    python import_xlsx_to_duckdb.py --dry-run             # показати файли без завантаження
"""

import sys
import argparse
import re
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

from olap_tool.config import load_duckdb_from_env
from olap_tool.sinks import DuckDBSink, sanitize_df
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
# Файловий пошук (аналог import_xlsx_to_clickhouse.py)
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
# Worker — використовує спільний DuckDBSink (requests.Session thread-safe)
# ---------------------------------------------------------------------------

def process_file(
    file_path: Path,
    year: int,
    week: int,
    sink: DuckDBSink,
) -> tuple[int, bool, float]:
    """Повертає (row_count, success, elapsed_sec)."""
    t0 = time.monotonic()
    try:
        df = _read_excel(file_path, 0)
    except Exception:
        return 0, False, time.monotonic() - t0

    if df.empty:
        return 0, True, time.monotonic() - t0

    df = sanitize_df(df)
    df["year_num"] = year
    df["week_num"] = week

    try:
        sink.delete_period(year, week)
        rows = sink.insert(df, year=year, week=week)
        success = rows > 0 or df.empty
        return rows, success, time.monotonic() - t0
    except Exception:
        return 0, False, time.monotonic() - t0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Паралельний імпорт Excel файлів OLAP-експорту у DuckDB"
    )
    parser.add_argument("--dir",     default="result", help="Базова директорія")
    parser.add_argument("--year",    type=int, default=None, help="Фільтр за роком")
    parser.add_argument("--week",    type=int, default=None, help="Фільтр за тижнем")
    parser.add_argument("--workers", type=int, default=4,    help="Паралельних воркерів")
    parser.add_argument("--dry-run", action="store_true",    help="Показати файли без завантаження")
    args = parser.parse_args()

    base_dir = Path(args.dir)
    if not base_dir.exists():
        console.print(f"[red]❌ Директорія не знайдена: {base_dir}[/red]")
        return 1

    cfg = load_duckdb_from_env()

    info = Table.grid(padding=(0, 2))
    info.add_column(style="cyan")
    info.add_column(style="white")
    info.add_row("Директорія",  str(base_dir.resolve()))
    info.add_row("DuckDB URL",  cfg.url)
    info.add_row("Таблиця",     cfg.table)
    info.add_row("Excel engine", _EXCEL_ENGINE)
    if args.year is not None:
        info.add_row("Рік", str(args.year))
    if args.week is not None:
        info.add_row("Тиждень", str(args.week))
    if not args.dry_run:
        info.add_row("Воркери", str(args.workers))

    console.print()
    console.print(Panel(
        info,
        title="[bold cyan]ІМПОРТ EXCEL → DUCKDB[/bold cyan]",
        border_style="cyan",
        expand=False,
    ))
    console.print()

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

    # Ініціалізація: CREATE TABLE з першого файлу
    with console.status("[cyan]Ініціалізація DuckDB...[/cyan]", spinner="dots"):
        try:
            sink = DuckDBSink(cfg)
            df_init = _read_excel(files[0][0], 0)
            if not df_init.empty:
                df_init = sanitize_df(df_init)
                df_init["year_num"] = files[0][1]
                df_init["week_num"] = files[0][2]
                sink.setup(df_init)
        except Exception as e:
            console.print(f"[red]❌ Помилка ініціалізації: {e}[/red]")
            return 1

    console.print("  [green]✅ Ініціалізовано[/green]\n")

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
                executor.submit(process_file, fp, y, w, sink): (fp, y, w)
                for fp, y, w in files
            }
            for future in as_completed(futures):
                fp, y, w = futures[future]
                try:
                    rows, success, elapsed = future.result()
                except Exception as e:
                    rows, success, elapsed = 0, False, 0.0
                    progress.console.print(
                        f"  [red]❌ {y}-{w:02d}[/red]  [dim]{fp.name}[/dim]  [red]{e}[/red]"
                    )

                total_rows += rows
                if not success:
                    errors += 1

                icon = "[green]✅[/green]" if success else "[red]❌[/red]"
                rows_str = f"[white]{rows:>7,}[/white] рядків" if rows > 0 else "[dim]    порожній[/dim]"
                progress.console.print(
                    f"  {icon} [cyan]{y}-{w:02d}[/cyan]  {rows_str}  [dim]{elapsed:.1f}с[/dim]"
                )

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

    sink.close()

    elapsed_total = time.monotonic() - start_time
    rate_files = total / elapsed_total if elapsed_total > 0 else 0
    rate_rows  = total_rows / elapsed_total if elapsed_total > 0 else 0

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="cyan")
    summary.add_column(style="white bold")
    summary.add_row("Файлів оброблено",   f"{total - errors}/{total}")
    summary.add_row("Рядків завантажено", f"{total_rows:,}")
    summary.add_row("Час",                f"{elapsed_total:.1f} с")
    summary.add_row("Швидкість",          f"{rate_files:.1f} файл/с  ·  {rate_rows:,.0f} рядків/с")
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
