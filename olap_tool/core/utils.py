import datetime

from rich.console import Console
from rich.table import Table

# colorama потрібен для progress.py (spinner/countdown з \r overwrite)
from colorama import init as _colorama_init
_colorama_init(autoreset=True)

_console = Console(highlight=False)

# Набір символів для логів — налаштовується через init_utils()
_ascii_logs = False

ICON_INFO = "ℹ️"
ICON_WARN = "⚠️"
ICON_ERR = "❌"
ICON_OK = "✅"
ICON_PROGRESS = "🔄"
ICON_STOP = "🛑"


def init_utils(ascii_logs: bool = False) -> None:
    """Ініціалізація модуля після побудови конфігурації."""
    global _ascii_logs
    global ICON_INFO, ICON_WARN, ICON_ERR, ICON_OK, ICON_PROGRESS, ICON_STOP
    _ascii_logs = ascii_logs
    if _ascii_logs:
        ICON_INFO = "i"
        ICON_WARN = "!"
        ICON_ERR = "x"
        ICON_OK = "+"
        ICON_PROGRESS = "*"
        ICON_STOP = "X"
    else:
        ICON_INFO = "ℹ️"
        ICON_WARN = "⚠️"
        ICON_ERR = "❌"
        ICON_OK = "✅"
        ICON_PROGRESS = "🔄"
        ICON_STOP = "🛑"


def ensure_dir(pathlike, verbose: bool = False):
    from pathlib import Path

    path = Path(pathlike)
    created = not path.exists()
    path.mkdir(parents=True, exist_ok=True)
    if verbose or created:
        print_info(f"Директорія '{path}' створена")
    return path


def get_current_time():
    return datetime.datetime.now().strftime("%H:%M:%S")


def print_header(text: str):
    _console.print()
    _console.rule(f"[bold]{text}[/bold]", style="cyan")
    _console.print()


def print_info_detail(text: str, details: dict | None = None):
    _console.print(
        f"[dim]\\[{get_current_time()}][/dim] [green]{ICON_INFO}  {text}[/green]"
    )
    if details:
        table = Table(
            show_header=False, box=None, padding=(0, 1), pad_edge=False,
        )
        table.add_column(style="dim cyan", no_wrap=True, min_width=3)
        table.add_column(style="white")
        for key, value in details.items():
            if "password" in key.lower() or "пароль" in key.lower():
                value = "********"
            table.add_row(f"   {key}:", str(value))
        _console.print(table)


def print_tech_error(text: str, error_obj: Exception | None = None):
    _console.print(
        f"[dim]\\[{get_current_time()}][/dim] [red]{ICON_STOP} {text}[/red]"
    )
    if error_obj:
        error_type = type(error_obj).__name__
        error_message = str(error_obj)
        table = Table(
            show_header=False, box=None, padding=(0, 1), pad_edge=False,
        )
        table.add_column(style="red", no_wrap=True, min_width=3)
        table.add_column(style="white")
        table.add_row("   Тип помилки:", error_type)
        table.add_row("   Повідомлення:", error_message)
        _console.print(table)
        if hasattr(error_obj, "__traceback__") and error_obj.__traceback__:
            import traceback

            tb_lines = traceback.format_tb(error_obj.__traceback__)
            if len(tb_lines) > 3:
                tb_lines = tb_lines[-3:]
            _console.print("   [red]Стек викликів:[/red]")
            for line in tb_lines:
                _console.print(f"   [yellow]{line.strip()}[/yellow]")


def print_info(text: str):
    _console.print(
        f"[dim]\\[{get_current_time()}][/dim] [green]{ICON_INFO}  {text}[/green]"
    )


def print_warning(text: str):
    _console.print(
        f"[dim]\\[{get_current_time()}][/dim] [yellow]{ICON_WARN}  {text}[/yellow]"
    )


def print_error(text: str):
    _console.print(
        f"[dim]\\[{get_current_time()}][/dim] [red]{ICON_ERR} {text}[/red]"
    )


def print_success(text: str):
    _console.print(
        f"[dim]\\[{get_current_time()}][/dim] [green]{ICON_OK} {text}[/green]"
    )


def print_progress(text: str):
    _console.print(
        f"[dim]\\[{get_current_time()}][/dim] [blue]{ICON_PROGRESS} {text}[/blue]"
    )


def format_file_size(size_bytes: int) -> str:
    """Форматує розмір файлу у зручну форму (Б/КБ/МБ/ГБ)."""
    if size_bytes < 1024:
        return f"{size_bytes} Б"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} КБ"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} МБ"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} ГБ"


def format_time(seconds: float):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{int(hours)} год {int(minutes)} хв {seconds:.2f} сек"
    elif minutes > 0:
        return f"{int(minutes)} хв {seconds:.2f} сек"
    else:
        return f"{seconds:.2f} сек"


_System = None  # Кеш .NET System модуля (завантажується один раз)
_System_loaded = False


def convert_dotnet_to_python(value):
    """Конвертує .NET типи (через pythonnet) у серіалізовані Python значення для запису в CSV/XLSX."""
    global _System, _System_loaded
    if not _System_loaded:
        try:
            import System  # type: ignore
            _System = System
        except Exception:
            _System = None
        _System_loaded = True

    if value is None:
        return None
    S = _System
    if S is not None:
        if isinstance(value, S.DateTime):
            epoch = datetime.date(1899, 12, 30)
            d = datetime.date(value.Year, value.Month, value.Day)
            return (d - epoch).days
        if isinstance(value, (S.Double, S.Single)):
            return float(value)
        if isinstance(value, S.Decimal):
            return float(value)
        if isinstance(value, S.DBNull):
            return None
        if isinstance(value, (S.Int32, S.Int64, S.UInt32, S.UInt64)):
            return int(value)
        if isinstance(value, S.String):
            return str(value)
        if isinstance(value, S.Boolean):
            return bool(value)
    try:
        return str(value)
    except Exception:
        return None
