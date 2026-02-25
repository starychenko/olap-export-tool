import sys
from pathlib import Path
from typing import TYPE_CHECKING

from .auth import (
    save_credentials,
    load_credentials,
    delete_credentials,
    get_current_windows_user,
)
from .prompt import prompt_credentials
from .utils import (
    print_info,
    print_info_detail,
    print_success,
    print_warning,
    print_error,
    print_tech_error,
)

if TYPE_CHECKING:
    from .config import SecretsConfig

# Константи для методів автентифікації
AUTH_SSPI = "SSPI"
AUTH_LOGIN = "LOGIN"


def _escape_conn_str_value(value: str) -> str:
    """Обгортає значення у подвійні лапки якщо воно містить спецсимволи connection string."""
    if any(ch in value for ch in (";", "=", "{", "}")):
        return f'"{value}"'
    return value


def init_dotnet_and_providers(adomd_dll_path: str = ""):
    """Ініціалізує середовище .NET та завантажує необхідні провайдери."""
    try:
        if sys.version_info >= (3, 14):
            print_warning(
                f"УВАГА: Python {sys.version_info.major}.{sys.version_info.minor} не підтримується через несумісність pythonnet."
            )
            print_warning("Рекомендовано використовувати Python 3.13 або нижче.")
            print_warning("Спроба ініціалізації pythonnet може завершитися помилкою.")

        import clr  # type: ignore

        if adomd_dll_path:
            if adomd_dll_path not in sys.path:
                sys.path.append(adomd_dll_path)
        else:
            print(
                "[INIT] Попередження: Шлях ADOMD_DLL_PATH не задано. Перевірте config.yaml або .env"
            )

        clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
        clr.AddReference("System.Data")

        from Microsoft.AnalysisServices.AdomdClient import AdomdConnection  # type: ignore
        from pyadomd import Pyadomd  # type: ignore

        OleDbConnection = None
        OleDbCommand = None
        try:
            from System.Data.OleDb import OleDbConnection, OleDbCommand  # type: ignore
        except Exception:
            print_warning(
                "[INIT] System.Data.OleDb недоступний. Підключення через LOGIN (логін/пароль) не буде працювати."
            )
            print_warning(
                "[INIT] Для LOGIN режиму потрібен .NET Framework 4.x або встановлений MSOLAP провайдер."
            )

        return Pyadomd, OleDbConnection, OleDbCommand
    except Exception as e:
        print(f"[INIT] Помилка ініціалізації .NET провайдерів/бібліотек: {e}")
        return None, None, None


def get_connection_string(secrets: "SecretsConfig"):
    """Формує рядок підключення та деталі автентифікації на основі SecretsConfig."""
    server = secrets.server
    database = secrets.database
    auth_method = secrets.auth_method.upper()

    connection_string = (
        f"Provider=MSOLAP;Data Source={server};Initial Catalog={database};"
    )
    auth_details = {}

    if auth_method == AUTH_SSPI:
        connection_string += "Integrated Security=SSPI;"
        auth_details = {
            "Метод автентифікації": "Windows-автентифікація (SSPI)",
            "Поточний користувач": get_current_windows_user(),
        }
    elif auth_method == AUTH_LOGIN:
        username, password = load_credentials(
            encrypted=secrets.credentials_encrypted,
            credentials_file=secrets.credentials_file,
            use_master_password=secrets.use_master_password,
            master_password=secrets.master_password,
        )

        if not username or not password:
            print_info("Облікові дані не знайдені або пошкоджені.")
            delete_credentials(credentials_file=secrets.credentials_file)
            username, password = prompt_credentials(
                with_domain=True, domain=secrets.domain
            )

        if username and password:
            safe_uid = _escape_conn_str_value(username)
            safe_pwd = _escape_conn_str_value(password)
            connection_string += f"User ID={safe_uid};Password={safe_pwd};Persist Security Info=True;Update Isolation Level=2;"
            auth_details = {
                "Метод автентифікації": "Логін/пароль",
                "Користувач": username,
                "Пароль": "********",
                # Зберігаємо для явної передачі — не парсимо з connection string
                "_username": username,
                "_password": password,
            }
        else:
            print_warning(
                "Облікові дані не вказані. Використовуємо Windows-автентифікацію (SSPI)."
            )
            connection_string += "Integrated Security=SSPI;"
            auth_details = {
                "Метод автентифікації": "Windows-автентифікація (SSPI) - автоматично",
                "Поточний користувач": get_current_windows_user(),
                "Причина": "Облікові дані не вказані",
            }
    else:
        print_warning(
            f"Невідомий метод автентифікації '{auth_method}'. Використовуємо SSPI."
        )
        connection_string += "Integrated Security=SSPI;"
        auth_details = {
            "Метод автентифікації": "Windows-автентифікація (SSPI) - автоматично",
            "Поточний користувач": get_current_windows_user(),
            "Причина": f"Невідомий метод автентифікації: {auth_method}",
        }
    return connection_string, auth_details


class OleDbCursor:
    """Клас-обгортка для OleDb, що імітує стандартний курсор Python DB API."""

    def __init__(self, connection, OleDbCommand):
        self.connection = connection
        self.reader = None
        self.command = None
        self.OleDbCommand = OleDbCommand
        self.description = None

    def execute(self, query: str):
        self.command = self.OleDbCommand(query, self.connection)
        self.reader = self.command.ExecuteReader()
        if self.reader and self.reader.FieldCount > 0:
            self.description = [
                (self.reader.GetName(i), None, None, None, None, None, None)
                for i in range(self.reader.FieldCount)
            ]

    def fetchall(self):
        if not self.reader:
            return []
        rows = []
        import System  # type: ignore

        while self.reader.Read():
            row = [
                (
                    self.reader.GetValue(i)
                    if not isinstance(self.reader.GetValue(i), System.DBNull)
                    else None
                )
                for i in range(self.reader.FieldCount)
            ]
            rows.append(row)
        return rows

    def fetchone(self):
        if not self.reader or not self.reader.Read():
            return None
        import System  # type: ignore

        return [
            (
                self.reader.GetValue(i)
                if not isinstance(self.reader.GetValue(i), System.DBNull)
                else None
            )
            for i in range(self.reader.FieldCount)
        ]

    def close(self):
        if self.reader and not self.reader.IsClosed:
            self.reader.Close()
        self.reader = None
        self.command = None


class OleDbConnectionWrapper:
    """Обгортка для з'єднання OleDb, щоб забезпечити уніфікований інтерфейс."""

    def __init__(self, conn, cur):
        self._conn = conn
        self._cursor = cur

    def cursor(self):
        return self._cursor

    def close(self):
        try:
            self._cursor.close()
        except Exception:
            pass
        try:
            self._conn.Close()
        except Exception:
            pass


def connect_using_oledb(connection_string, auth_details, OleDbConnection, OleDbCommand, secrets: "SecretsConfig"):
    """Встановлює з'єднання через OleDb і зберігає облікові дані при успіху."""
    try:
        print_info_detail(
            f"Підключення до OLAP сервера {secrets.server} через OleDb...",
            {k: v for k, v in auth_details.items() if not k.startswith("_")},
        )
        connection = OleDbConnection(connection_string)
        connection.Open()
        cursor = OleDbCursor(connection, OleDbCommand)
        print_success("Підключення до OLAP сервера через OleDb успішно встановлено")

        # Зберігаємо через явні дані, не парсимо з connection string
        username = auth_details.get("_username")
        password = auth_details.get("_password")
        if username and password:
            if save_credentials(
                username, password,
                encrypted=secrets.credentials_encrypted,
                credentials_file=secrets.credentials_file,
            ):
                print_success(
                    "Облікові дані успішно збережено"
                    + (" (зашифровано)" if secrets.credentials_encrypted else "")
                )

        return connection, cursor
    except Exception as e:
        print_tech_error("Помилка підключення до OLAP сервера через OleDb", e)
        return None, None


def connect_to_olap(
    secrets: "SecretsConfig",
    adomd_dll_path: str = "",
    connection_string=None,
    auth_details=None,
    retry_count=1,
):
    """Основна функція для підключення до OLAP."""
    Pyadomd, OleDbConnection, OleDbCommand = init_dotnet_and_providers(adomd_dll_path)

    if connection_string is None:
        connection_string, auth_details = get_connection_string(secrets)

    auth_method = auth_details.get("Метод автентифікації", "")

    try:
        if "Логін/пароль" in auth_method:
            if Pyadomd is not None:
                print_info(
                    "Спроба підключення через Pyadomd (ADOMD.NET) для автентифікації за логіном/паролем"
                )
                try:
                    connection = Pyadomd(connection_string)
                    connection.open()
                    print_success("Підключення через Pyadomd (ADOMD.NET) успішне")
                    # Зберігаємо через явні дані
                    username = auth_details.get("_username")
                    password = auth_details.get("_password")
                    if username and password:
                        save_credentials(
                            username, password,
                            encrypted=secrets.credentials_encrypted,
                            credentials_file=secrets.credentials_file,
                        )
                    return connection
                except Exception as pyadomd_error:
                    print_warning(f"Не вдалося підключитися через Pyadomd: {pyadomd_error}")

            if OleDbConnection is not None and OleDbCommand is not None:
                print_info(
                    "Використовуємо підключення через OleDbConnection для автентифікації за логіном/паролем"
                )
                oledb_connection, cursor = connect_using_oledb(
                    connection_string, auth_details, OleDbConnection, OleDbCommand, secrets
                )

                if oledb_connection and cursor:
                    return OleDbConnectionWrapper(oledb_connection, cursor)
            else:
                print_error(
                    "OleDb провайдер недоступний. Для LOGIN потрібен Pyadomd або MSOLAP (System.Data.OleDb)."
                )

            if retry_count > 0:
                print_warning(
                    "Не вдалося підключитися. Спробуйте ввести облікові дані ще раз."
                )
                delete_credentials(credentials_file=secrets.credentials_file)
                new_username, new_password = prompt_credentials(
                    with_domain=True, domain=secrets.domain
                )
                if new_username and new_password:
                    safe_uid = _escape_conn_str_value(new_username)
                    safe_pwd = _escape_conn_str_value(new_password)
                    new_connection_string = (
                        f"Provider=MSOLAP;Data Source={secrets.server};"
                        f"Initial Catalog={secrets.database};"
                        f"User ID={safe_uid};Password={safe_pwd};"
                        f"Persist Security Info=True;Update Isolation Level=2;"
                    )
                    new_auth_details = {
                        "Метод автентифікації": "Логін/пароль",
                        "Користувач": new_username,
                        "Пароль": "********",
                        "_username": new_username,
                        "_password": new_password,
                    }
                    return connect_to_olap(
                        secrets, adomd_dll_path,
                        new_connection_string, new_auth_details, retry_count - 1,
                    )

            print_error(
                "Не вдалося встановити підключення через OleDb після повторних спроб."
            )
            return None

        else:  # Для SSPI
            if Pyadomd is None:
                print_error(
                    "Pyadomd/ADOMD.NET недоступні. Перевірте ADOMD_DLL_PATH та наявність бібліотек."
                )
                return None
            print_info_detail(
                f"Підключення до OLAP сервера {secrets.server} через ADOMD.NET (SSPI)...",
                {k: v for k, v in auth_details.items() if not k.startswith("_")},
            )
            connection = Pyadomd(connection_string)
            connection.open()
            print_success(
                "Підключення до OLAP сервера через ADOMD.NET успішно встановлено"
            )
            return connection

    except Exception as e:
        print_tech_error("Помилка підключення до OLAP сервера", e)
        return None
