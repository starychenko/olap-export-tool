import base64
import os
from pathlib import Path
from typing import Optional, Tuple

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from ..core.utils import print_info, print_warning, print_error


def get_machine_id() -> str:
    """
    Генерує стабільний ідентифікатор пристрою, що не змінюється залежно від
    типу терміналу (Git Bash, CMD, PowerShell, планувальник).
    Використовує platform.node() замість змінних середовища, які можуть
    відрізнятися або бути відсутніми в різних оточеннях.
    """
    try:
        import hashlib
        import platform
        import getpass

        hostname = platform.node() or "unknown_host"
        username = _safe_getuser()
        unique_id = f"{hostname.lower()}-{username.lower()}"
        return hashlib.md5(unique_id.encode("utf-8")).hexdigest()
    except Exception as e:
        print_warning(f"Не вдалося отримати унікальний ідентифікатор пристрою: {e}")
        import hashlib
        fallback = f"user-{os.environ.get('USERNAME', 'unknown')}"
        return hashlib.md5(fallback.encode("utf-8")).hexdigest()


def _safe_getuser() -> str:
    """Безпечно отримує ім'я поточного користувача, обходячи баг `getpass.getuser()` у Git Bash."""
    import getpass
    try:
        return getpass.getuser()
    except Exception:
        # getpass.getuser() може впасти у деяких середовищах (особливо в Git Bash на Windows)
        return (
            os.environ.get("USERNAME")
            or os.environ.get("USER")
            or os.environ.get("LOGNAME")
            or "unknown_user"
        )


def generate_encryption_key(
    password: str | bytes, salt: bytes | None = None
) -> Tuple[bytes, bytes]:
    if salt is None:
        salt = os.urandom(16)
    if isinstance(password, str):
        password = password.encode()
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    key = base64.urlsafe_b64encode(kdf.derive(password))
    return key, salt


def get_master_password(
    use_master_password: bool = False,
    master_password: Optional[str] = None,
) -> str | None:
    """Повертає майстер-пароль із параметрів або інтерактивного вводу."""
    if not use_master_password:
        return None
    if master_password:
        return master_password
    try:
        import sys
        import getpass
        from colorama import Fore

        if sys.stdin and sys.stdin.isatty():
            mp = getpass.getpass(
                f"{Fore.CYAN}Введіть майстер-пароль для шифрування (залиште порожнім, щоб пропустити): {Fore.RESET}"
            )
            return mp if mp else None
    except Exception:
        pass
    return None


def secure_credentials_file(file_path: Path):
    try:
        if os.name == "nt":
            import subprocess

            cmd = f'icacls "{str(file_path)}" /inheritance:r /grant:r "{os.environ.get("USERNAME", "")}":F /C'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print_warning(
                    f"Не вдалося застосувати ACL через icacls: {result.stderr.strip()}"
                )
        else:
            import stat

            os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception as e:
        print_warning(f"Не вдалося посилити права доступу до файлу: {e}")


def encrypt_credentials(username: str, password: str, encryption_key: bytes) -> bytes:
    cipher = Fernet(encryption_key)
    data = f"{username}:{password}".encode()
    return cipher.encrypt(data)


def decrypt_credentials(encrypted_data: bytes, encryption_key: bytes):
    try:
        cipher = Fernet(encryption_key)
        decrypted_data = cipher.decrypt(encrypted_data)
        username, password = decrypted_data.decode().split(":", 1)
        return username, password
    except Exception as e:
        print_error(f"Помилка розшифрування облікових даних: {e}")
        return None, None
