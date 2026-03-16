import datetime
from colorama import init, Fore, Style


init(autoreset=True)


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
    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 80}")
    print(f"{Fore.CYAN}{Style.BRIGHT}== {text}")
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 80}")
    print()


def print_info_detail(text: str, details: dict | None = None):
    print(f"{Fore.GREEN}[{get_current_time()}] {ICON_INFO}  {text}")
    if details:
        for key, value in details.items():
            if "password" in key.lower() or "пароль" in key.lower():
                value = "********"
            print(f"   {Fore.CYAN}{key}: {Fore.WHITE}{value}")


def print_tech_error(text: str, error_obj: Exception | None = None):
    print(f"{Fore.RED}[{get_current_time()}] {ICON_STOP} {text}")
    if error_obj:
        error_type = type(error_obj).__name__
        error_message = str(error_obj)
        print(f"   {Fore.RED}Тип помилки: {Fore.WHITE}{error_type}")
        print(f"   {Fore.RED}Повідомлення: {Fore.WHITE}{error_message}")
        if hasattr(error_obj, "__traceback__") and error_obj.__traceback__:
            import traceback

            tb_lines = traceback.format_tb(error_obj.__traceback__)
            if len(tb_lines) > 3:
                tb_lines = tb_lines[-3:]
            print(f"   {Fore.RED}Стек викликів:")
            for line in tb_lines:
                print(f"   {Fore.YELLOW}{line.strip()}")


def print_info(text: str):
    print(f"{Fore.GREEN}[{get_current_time()}] {ICON_INFO}  {text}")


def print_warning(text: str):
    print(f"{Fore.YELLOW}[{get_current_time()}] {ICON_WARN}  {text}")


def print_error(text: str):
    print(f"{Fore.RED}[{get_current_time()}] {ICON_ERR} {text}")


def print_success(text: str):
    print(f"{Fore.GREEN}[{get_current_time()}] {ICON_OK} {text}")


def print_progress(text: str):
    print(f"{Fore.BLUE}[{get_current_time()}] {ICON_PROGRESS} {text}")


def format_time(seconds: float):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{int(hours)} год {int(minutes)} хв {seconds:.2f} сек"
    elif minutes > 0:
        return f"{int(minutes)} хв {seconds:.2f} сек"
    else:
        return f"{seconds:.2f} сек"


def convert_dotnet_to_python(value):
    """Конвертує .NET типи (через pythonnet) у серіалізовані Python значення для запису в CSV/XLSX."""
    try:
        import System  # type: ignore
    except Exception:
        System = None  # type: ignore

    if value is None:
        return None
    if System is not None:
        if isinstance(value, System.DateTime):
            # Повертаємо Excel-серійний номер (int) — як зберігалось раніше
            epoch = datetime.date(1899, 12, 30)
            d = datetime.date(value.Year, value.Month, value.Day)
            return (d - epoch).days
        if isinstance(value, (System.Double, System.Single)):
            return float(value)
        if isinstance(value, System.Decimal):
            return float(value)
        if isinstance(value, System.DBNull):
            return None
        if isinstance(value, (System.Int32, System.Int64, System.UInt32, System.UInt64)):
            return int(value)
        if isinstance(value, System.String):
            return str(value)
        if isinstance(value, System.Boolean):
            return bool(value)
    # Невідомий .NET тип — конвертуємо в str, щоб xlsxwriter не падав
    try:
        return str(value)
    except Exception:
        return None
