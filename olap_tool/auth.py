from pathlib import Path

from .security import (
    get_machine_id,
    generate_encryption_key,
    get_master_password,
    secure_credentials_file,
    encrypt_credentials,
    decrypt_credentials,
)
from .utils import print_info, print_error


auth_username: str | None = None


def save_credentials(
    username: str,
    password: str,
    encrypted: bool = False,
    credentials_file: str = ".credentials",
) -> bool:
    global auth_username
    cred_path = Path(credentials_file)
    try:
        if encrypted:
            machine_id = get_machine_id()
            master_password = get_master_password()
            base_secret = (
                f"{machine_id}:{master_password}" if master_password else machine_id
            )
            key, salt = generate_encryption_key(base_secret)
            encrypted_data = encrypt_credentials(username, password, key)
            with open(cred_path, "wb") as f:
                f.write(salt)
                f.write(b"\n")
                f.write(encrypted_data)
        else:
            with open(cred_path, "w") as f:
                f.write(f"{username}:{password}")

        secure_credentials_file(cred_path)
        auth_username = username
        return True
    except Exception as e:
        print_error(f"Помилка збереження облікових даних: {e}")
        return False


def load_credentials(
    encrypted: bool = False,
    credentials_file: str = ".credentials",
    use_master_password: bool = False,
    master_password: str | None = None,
):
    global auth_username
    cred_path = Path(credentials_file)
    if not cred_path.exists():
        return None, None
    try:
        if encrypted:
            with open(cred_path, "rb") as f:
                content = f.read().split(b"\n", 1)
                if len(content) < 2:
                    print_error("Невірний формат файлу облікових даних")
                    return None, None
                salt, encrypted_data = content
                machine_id = get_machine_id()
                mp = get_master_password(
                    use_master_password=use_master_password,
                    master_password=master_password,
                )
                base_secret = (
                    f"{machine_id}:{mp}" if mp else machine_id
                )
                key, _ = generate_encryption_key(base_secret, salt)
                username, password = decrypt_credentials(encrypted_data, key)
                if not (username and password) and use_master_password and not master_password:
                    try:
                        import getpass
                        from colorama import Fore

                        mp_retry = getpass.getpass(
                            f"{Fore.CYAN}Введіть майстер-пароль для розшифрування: {Fore.RESET}"
                        )
                        base_secret_retry = f"{machine_id}:{mp_retry}" if mp_retry else machine_id
                        key_retry, _ = generate_encryption_key(base_secret_retry, salt)
                        username, password = decrypt_credentials(
                            encrypted_data, key_retry
                        )
                    except Exception:
                        pass
                if username and password:
                    print_info("Облікові дані успішно розшифровано")
                    auth_username = username
                    return username, password
                print_error(
                    "Не вдалося розшифрувати облікові дані. Перевірте налаштування майстер-пароля."
                )
                return None, None
        else:
            with open(cred_path, "r") as f:
                content = f.read().strip()
                if ":" not in content:
                    print_error("Невірний формат файлу облікових даних")
                    return None, None
                username, password = content.split(":", 1)
                auth_username = username
                return username, password
    except Exception as e:
        print_error(f"Помилка завантаження облікових даних: {e}")
        return None, None


def delete_credentials(credentials_file: str = ".credentials") -> bool:
    global auth_username
    cred_path = Path(credentials_file)
    if not cred_path.exists():
        return True
    try:
        cred_path.unlink()
        auth_username = None
        return True
    except Exception as e:
        print_error(f"Помилка видалення файлу облікових даних: {e}")
        return False


def get_current_windows_user() -> str:
    import os as _os

    try:
        return _os.getlogin()
    except Exception:
        return _os.getenv("USERNAME", "Невідомий користувач")
