import getpass
from typing import Optional
from colorama import Fore

from ..core.utils import print_info


def prompt_credentials(with_domain: bool = False, domain: Optional[str] = None):
    import sys
    if hasattr(sys, "stdout") and hasattr(sys.stdout, "_app"):
        # TUI mode
        app = getattr(sys.stdout, "_app")
        import threading
        event = threading.Event()
        result_store = [None, None]
        
        def show_dialog():
            try:
                from olap_tool.tui.screens.credentials import CredentialsDialog
                def cb(res: tuple[str, str] | None):
                    if res:
                        result_store[0], result_store[1] = res
                    event.set()
                app.push_screen(CredentialsDialog(domain=domain), cb)
            except Exception as e:
                print_info(f"Помилка виклику TUI діалогу: {e}")
                event.set()

        app.call_from_thread(show_dialog)
        event.wait()
        
        username, password = result_store[0], result_store[1]
        if with_domain and username and domain:
            if "\\" not in username and not username.startswith(f"{domain}\\"):
                username = f"{domain}\\{username}"
                print_info(f"Використовуємо повне ім'я користувача: {username}")
        return username, password

    # CLI mode
    print_info("Введіть облікові дані для підключення до OLAP:")
    username = input(f"{Fore.CYAN}Ім'я користувача: {Fore.RESET}")
    password = getpass.getpass(f"{Fore.CYAN}Пароль: {Fore.RESET}")
    if with_domain and username and domain:
        if "\\" not in username and not username.startswith(f"{domain}\\"):
            username = f"{domain}\\{username}"
            print_info(f"Використовуємо повне ім'я користувача: {username}")
    return username, password
