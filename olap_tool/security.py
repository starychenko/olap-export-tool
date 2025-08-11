import base64
import os
from pathlib import Path
from typing import Tuple

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .utils import print_info, print_warning, print_error


def get_machine_id() -> str:
    try:
        identifiers: list[str] = []
        computer_name = os.environ.get("COMPUTERNAME", "")
        if computer_name:
            identifiers.append(computer_name)
        user_domain = os.environ.get("USERDOMAIN", "")
        if user_domain:
            identifiers.append(user_domain)
        username = os.environ.get("USERNAME", "")
        if username:
            identifiers.append(username)
        windows_dir = os.environ.get("WINDIR", "")
        if windows_dir:
            identifiers.append(windows_dir)
        system_drive = os.environ.get("SystemDrive", "")
        if system_drive:
            identifiers.append(system_drive)
        try:
            import subprocess

            volume_info = subprocess.run(
                f"vol {system_drive}", shell=True, capture_output=True, text=True
            )
            if volume_info.returncode == 0:
                for line in volume_info.stdout.strip().split("\n"):
                    if "Serial Number" in line or "Серійний номер" in line:
                        identifiers.append(line.strip())
        except Exception:
            pass

        import hashlib

        unique_id = "-".join(identifiers)
        if not unique_id:
            unique_id = f"windows-fallback"
            print_warning(
                "Не вдалося отримати стабільні ідентифікатори системи, використовуємо запасний варіант"
            )
        return hashlib.md5(unique_id.encode()).hexdigest()
    except Exception as e:
        print_warning(f"Не вдалося отримати унікальний ідентифікатор пристрою: {e}")
        import hashlib

        fallback = (
            f"user-{os.environ.get('USERNAME', '')}-{os.environ.get('WINDIR', '')}"
        )
        return hashlib.md5(fallback.encode()).hexdigest()


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


def get_master_password() -> str | None:
    import sys
    import getpass
    from colorama import Fore

    use_master = os.getenv("OLAP_USE_MASTER_PASSWORD", "false").lower() in (
        "true",
        "1",
        "yes",
    )
    if not use_master:
        return None
    master_env = os.getenv("OLAP_MASTER_PASSWORD")
    if master_env:
        return master_env
    try:
        if sys.stdin and sys.stdin.isatty():
            mp = getpass.getpass(
                f"{Fore.CYAN}Введіть майстер‑пароль для шифрування (залиште порожнім, щоб пропустити): {Fore.RESET}"
            )
            return mp if mp else None
    except Exception:
        pass
    return None


def secure_credentials_file(file_path: Path):
    try:
        if os.name == "nt":
            import subprocess

            cmd = f'icacls "{str(file_path)}" /inheritance:r /grant:r "{os.getenv("USERNAME", "")}":F /C'
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
