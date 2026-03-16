#!/usr/bin/env python3
"""
Паралельний імпорт XLSX файлів в аналітичне сховище.

Використання:
  python scripts/import_xlsx.py --target ch   --dir result/ --workers 4
  python scripts/import_xlsx.py --target duck --year 2025 --week 10
  python scripts/import_xlsx.py --target pg   --dry-run

Підтримувані цілі (--target):
  ch / clickhouse   — ClickHouse (thread-local з'єднання на кожен воркер)
  duck / duckdb     — DuckDB REST API (один спільний sink, thread-safe)
  pg / postgresql   — PostgreSQL через COPY FROM STDIN (thread-local з'єднання на кожен воркер)
"""

import sys
import argparse
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import fields as dc_fields
from pathlib import Path
from typing import Optional

# Додаємо корінь проєкту до sys.path, щоб можна було імпортувати olap_tool
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

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

from olap_tool.core.utils import init_utils

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
# Thread-local sink pool (одне з'єднання на потік для не-thread-safe sinks)
# ---------------------------------------------------------------------------

class ThreadLocalSinkPool:
    """Пул thread-local sinks для паралельного завантаження.

    Для кожного потоку створює окремий sink-екземпляр і зберігає
    посилання для cleanup після завершення всіх задач.
    setup() ідемпотентний (CREATE TABLE IF NOT EXISTS).
    """

    def __init__(self, sink_class, config_class, sink_kwargs: dict | None = None):
        self._sink_class = sink_class
        self._config_class = config_class
        self._sink_kwargs = sink_kwargs or {}
        self._local = threading.local()
        self._all_sinks: list = []
        self._lock = threading.Lock()
        self._setup_df: Optional[pd.DataFrame] = None

    def set_setup_df(self, df: pd.DataFrame) -> None:
        self._setup_df = df

    def get_sink(self, cfg_kwargs: dict):
        """Повертає thread-local sink, створює новий якщо потрібно."""
        if not hasattr(self._local, "sink") or self._local.sink is None:
            sink = self._sink_class(self._config_class(**cfg_kwargs), **self._sink_kwargs)
            if self._setup_df is not None:
                sink.setup(self._setup_df)
            self._local.sink = sink
            with self._lock:
                self._all_sinks.append(sink)
        return self._local.sink

    def close_all(self) -> None:
        for sink in self._all_sinks:
            try:
                sink.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Файловий пошук
# ---------------------------------------------------------------------------

def find_xlsx_files(
    base_dir: Path,
    year: Optional[int],
    week: Optional[int],
) -> list[tuple[Path, int, int]]:
    """Рекурсивно знаходить файли формату YYYY-WW.xlsx."""
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
    """Читає Excel-файл через calamine з fallback на openpyxl."""
    try:
        return pd.read_excel(str(file_path), sheet_name=sheet, engine=_EXCEL_ENGINE)
    except Exception:
        if _EXCEL_ENGINE != "openpyxl":
            return pd.read_excel(str(file_path), sheet_name=sheet, engine="openpyxl")
        raise


# ---------------------------------------------------------------------------
# Generic worker
# ---------------------------------------------------------------------------

def _process_file(
    file_path: Path,
    year: int,
    week: int,
    sink_or_pool,
    sheet,
    cfg_kwargs: "dict | None" = None,  # потрібен тільки для ThreadLocalSinkPool
) -> tuple[int, bool, float]:
    """
    Універсальний воркер для всіх sink-типів.

    sink_or_pool: або sink напряму (DuckDB — thread-safe),
                  або ThreadLocalSinkPool (ClickHouse/PostgreSQL — thread-local).
    """
    t0 = time.monotonic()
    df = _read_excel(file_path, sheet)

    if df.empty:
        return 0, True, time.monotonic() - t0

    from olap_tool.sinks import sanitize_df
    df = sanitize_df(df)
    df["year_num"] = year
    df["week_num"] = week

    sink = (
        sink_or_pool.get_sink(cfg_kwargs or {})
        if isinstance(sink_or_pool, ThreadLocalSinkPool)
        else sink_or_pool
    )
    sink.delete_period(year, week)
    rows = sink.insert(df, year=year, week=week)
    return rows, rows >= 0, time.monotonic() - t0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Паралельний імпорт Excel файлів OLAP-експорту в аналітичне сховище",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Приклади:\n"
            "  python scripts/import_xlsx.py --target ch --dir result/\n"
            "  python scripts/import_xlsx.py --target duck --year 2025 --week 10\n"
            "  python scripts/import_xlsx.py --target pg --dry-run\n"
        ),
    )
    parser.add_argument(
        "--target", "-t",
        required=True,
        choices=["ch", "clickhouse", "duck", "duckdb", "pg", "postgresql"],
        help="Ціль імпорту: ch/clickhouse | duck/duckdb | pg/postgresql",
    )
    parser.add_argument("--dir",     default="result",  help="Базова директорія з XLSX файлами")
    parser.add_argument("--year",    type=int, default=None, help="Фільтр за роком")
    parser.add_argument("--week",    type=int, default=None, help="Фільтр за тижнем")
    parser.add_argument("--sheet",   default="0",            help="Аркуш Excel (назва або індекс)")
    parser.add_argument("--workers", type=int, default=4,    help="Паралельних воркерів")
    parser.add_argument("--dry-run", action="store_true",    help="Показати файли без завантаження")
    args = parser.parse_args()

    # Нормалізуємо target
    target = args.target.lower()
    if target in ("ch", "clickhouse"):
        target = "clickhouse"
    elif target in ("duck", "duckdb"):
        target = "duckdb"
    elif target in ("pg", "postgresql"):
        target = "postgresql"

    base_dir = Path(args.dir)
    if not base_dir.exists():
        console.print(f"[red]❌ Директорія не знайдена: {base_dir}[/red]")
        return 1

    # Sheet: int або str
    sheet: str | int = args.sheet
    try:
        sheet = int(sheet)
    except (ValueError, TypeError):
        pass

    # ── Завантаження конфігурації з env ────────────────────────────────────
    from olap_tool.core.config import (
        load_clickhouse_from_env,
        load_duckdb_from_env,
        load_postgres_from_env,
    )

    if target == "clickhouse":
        cfg = load_clickhouse_from_env()
        target_label = f"ClickHouse  {cfg.host}:{cfg.port}  →  {cfg.database}.{cfg.table}"
        target_title = "[bold cyan]ІМПОРТ EXCEL → CLICKHOUSE[/bold cyan]"
    elif target == "duckdb":
        cfg = load_duckdb_from_env()
        target_label = f"DuckDB  {cfg.url}  →  {cfg.table}"
        target_title = "[bold cyan]ІМПОРТ EXCEL → DUCKDB[/bold cyan]"
    else:  # postgresql
        cfg = load_postgres_from_env()
        target_label = (
            f"PostgreSQL  {cfg.host}:{cfg.port}  →  "
            f"{cfg.database}/{cfg.schema}.{cfg.table}"
        )
        target_title = "[bold cyan]ІМПОРТ EXCEL → POSTGRESQL[/bold cyan]"

    # ── Заголовок ──────────────────────────────────────────────────────────
    info = Table.grid(padding=(0, 2))
    info.add_column(style="cyan")
    info.add_column(style="white")
    info.add_row("Директорія",   str(base_dir.resolve()))
    info.add_row("Ціль",         target_label)
    info.add_row("Excel engine", _EXCEL_ENGINE)
    if args.year is not None:
        info.add_row("Рік", str(args.year))
    if args.week is not None:
        info.add_row("Тиждень", str(args.week))
    if not args.dry_run:
        info.add_row("Воркери", str(args.workers))
    if args.dry_run:
        info.add_row("Режим", "[yellow]DRY RUN[/yellow]")

    console.print()
    console.print(Panel(info, title=target_title, border_style="cyan", expand=False))
    console.print()

    sink_pool: "ThreadLocalSinkPool | None" = None
    sink = None  # shared sink (DuckDB)
    cfg_kwargs: dict = {}

    # ── Пошук файлів ───────────────────────────────────────────────────────
    files = find_xlsx_files(base_dir, args.year, args.week)
    if not files:
        console.print("[yellow]⚠️  Файлів не знайдено за вказаними параметрами[/yellow]")
        return 0

    console.print(f"  [cyan]Знайдено файлів:[/cyan] [white bold]{len(files)}[/white bold]\n")

    if args.dry_run:
        for i, (fp, y, w) in enumerate(files, 1):
            console.print(
                f"  [dim]{i:>4}.[/dim] [white]{fp}[/white]  [yellow]({y}-{w:02d})[/yellow]"
            )
        console.print(f"\n[yellow]DRY RUN завершено. Файлів: {len(files)}[/yellow]")
        return 0

    # ── Ініціалізація sink та CREATE TABLE з першого непорожнього файлу ──────
    # Перший файл може бути порожнім → шукаємо перший з даними для setup()
    with console.status(f"[cyan]Ініціалізація {target.upper()}...[/cyan]", spinner="dots"):
        try:
            df_init = pd.DataFrame()
            init_file_idx = 0
            for _i, (_fp, _y, _w) in enumerate(files):
                df_init = _read_excel(_fp, sheet)
                if not df_init.empty:
                    init_file_idx = _i
                    break

            from olap_tool.sinks import sanitize_df

            # Підготовка init DataFrame для CREATE TABLE
            df_init_clean = None
            if not df_init.empty:
                df_init_clean = sanitize_df(df_init.copy())
                df_init_clean["year_num"] = files[init_file_idx][1]
                df_init_clean["week_num"] = files[init_file_idx][2]

            if target == "clickhouse":
                from olap_tool.sinks import ClickHouseSink
                from olap_tool.core.config import ClickHouseConfig
                cfg_kwargs = {f.name: getattr(cfg, f.name) for f in dc_fields(cfg)}
                sink_pool = ThreadLocalSinkPool(ClickHouseSink, ClickHouseConfig, sink_kwargs={"silent": True})
                # Ініціалізаційний setup через тимчасовий sink
                init_sink = ClickHouseSink(ClickHouseConfig(**cfg_kwargs), silent=True)
                if df_init_clean is not None:
                    init_sink.setup(df_init_clean)
                    sink_pool.set_setup_df(df_init_clean)
                init_sink.close()

            elif target == "duckdb":
                from olap_tool.sinks import DuckDBSink
                from olap_tool.core.config import DuckDBConfig
                if not isinstance(cfg, DuckDBConfig):
                    raise TypeError(f"Очікувався DuckDBConfig, отримано {type(cfg).__name__}")
                sink = DuckDBSink(cfg, silent=True)
                if df_init_clean is not None:
                    sink.setup(df_init_clean)

            else:  # postgresql
                from olap_tool.sinks import PostgreSQLSink
                from olap_tool.core.config import PostgreSQLConfig
                if not isinstance(cfg, PostgreSQLConfig):
                    raise TypeError(f"Очікувався PostgreSQLConfig, отримано {type(cfg).__name__}")
                cfg_kwargs = {f.name: getattr(cfg, f.name) for f in dc_fields(cfg)}
                sink_pool = ThreadLocalSinkPool(PostgreSQLSink, PostgreSQLConfig, sink_kwargs={"silent": True})
                init_sink = PostgreSQLSink(cfg, silent=True)
                if df_init_clean is not None:
                    init_sink.setup(df_init_clean)
                    sink_pool.set_setup_df(df_init_clean)
                init_sink.close()

        except Exception as e:
            console.print(f"[red]❌ Помилка ініціалізації: {e}[/red]")
            return 1

    console.print("  [green]✅ Ініціалізовано[/green]\n")

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

    # sink_or_pool: або ThreadLocalSinkPool (CH/PG), або sink напряму (DuckDB)
    sink_or_pool = sink_pool if sink_pool is not None else sink

    with progress:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    _process_file, fp, y, w, sink_or_pool, sheet,
                    cfg_kwargs if sink_pool is not None else None
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
                        f"  [red]❌ {y}-{w:02d}[/red]  [dim]{fp.name}[/dim]  [red]{e}[/red]"
                    )

                total_rows += rows
                if not success:
                    errors += 1

                icon = "[green]✅[/green]" if success else "[red]❌[/red]"
                rows_str = (
                    f"[white]{rows:>7,}[/white] рядків"
                    if rows > 0
                    else "[dim]    порожній[/dim]"
                )
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

    # ── Закриваємо з'єднання ───────────────────────────────────────────────
    if sink_pool is not None:
        sink_pool.close_all()
    elif sink is not None:
        try:
            sink.close()
        except Exception:
            pass

    # ── Підсумок ───────────────────────────────────────────────────────────
    elapsed_total = time.monotonic() - start_time
    rate_files = total / elapsed_total if elapsed_total > 0 else 0
    rate_rows  = total_rows / elapsed_total if elapsed_total > 0 else 0

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="cyan")
    summary.add_column(style="white bold")
    summary.add_row("Ціль",               target.upper())
    summary.add_row("Файлів оброблено",   f"{total - errors}/{total}")
    summary.add_row("Рядків завантажено", f"{total_rows:,}")
    summary.add_row("Час",                f"{elapsed_total:.1f} с")
    summary.add_row("Швидкість",          f"{rate_files:.1f} файл/с  ·  {rate_rows:,.0f} рядків/с")
    if errors:
        summary.add_row("[red]Помилок[/red]", f"[red]{errors}[/red]")

    border = "green" if not errors else "yellow"
    title = (
        "[bold green]✅ Імпорт завершено[/bold green]"
        if not errors
        else "[bold yellow]⚠️  Завершено з помилками[/bold yellow]"
    )

    console.print()
    console.print(Panel(summary, title=title, border_style=border, expand=False))
    console.print()

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
