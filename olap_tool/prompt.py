import getpass
from typing import Optional
from colorama import Fore

from .utils import print_info


def prompt_credentials(with_domain: bool = False, domain: Optional[str] = None):
    print_info("Введіть облікові дані для підключення до OLAP:")
    username = input(f"{Fore.CYAN}Ім'я користувача: {Fore.RESET}")
    password = getpass.getpass(f"{Fore.CYAN}Пароль: {Fore.RESET}")
    if with_domain and username and domain:
        if "\\" not in username and not username.startswith(f"{domain}\\"):
            username = f"{domain}\\{username}"
            print_info(f"Використовуємо повне ім'я користувача: {username}")
    return username, password
