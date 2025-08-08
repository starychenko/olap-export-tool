import datetime
import sys
from colorama import init, Fore, Style


init(autoreset=True)


def ensure_dir(pathlike):
    from pathlib import Path

    path = Path(pathlike)
    path.mkdir(parents=True, exist_ok=True)
    print_info(f"Директорія '{path}' перевірена/створена")
    return path


def get_current_time():
    return datetime.datetime.now().strftime("%H:%M:%S")


def print_header(text: str):
    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 80}")
    print(f"{Fore.CYAN}{Style.BRIGHT}== {text}")
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 80}")
    print()


def print_info_detail(text: str, details: dict | None = None):
    print(f"{Fore.GREEN}[{get_current_time()}] ℹ️  {text}")
    if details:
        for key, value in details.items():
            if "password" in key.lower() or "пароль" in key.lower():
                value = "********"
            print(f"   {Fore.CYAN}{key}: {Fore.WHITE}{value}")


def print_tech_error(text: str, error_obj: Exception | None = None):
    print(f"{Fore.RED}[{get_current_time()}] 🛑 {text}")
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
    print(f"{Fore.GREEN}[{get_current_time()}] ℹ️  {text}")


def print_warning(text: str):
    print(f"{Fore.YELLOW}[{get_current_time()}] ⚠️  {text}")


def print_error(text: str):
    print(f"{Fore.RED}[{get_current_time()}] ❌ {text}")


def print_success(text: str):
    print(f"{Fore.GREEN}[{get_current_time()}] ✅ {text}")


def print_progress(text: str):
    print(f"{Fore.BLUE}[{get_current_time()}] 🔄 {text}")


def format_time(seconds: float):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{int(hours)} год {int(minutes)} хв {seconds:.2f} сек"
    elif minutes > 0:
        return f"{int(minutes)} хв {seconds:.2f} сек"
    else:
        return f"{seconds:.2f} сек"


