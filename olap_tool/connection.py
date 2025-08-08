import os
import re
from pathlib import Path

from .auth import (
    save_credentials,
    load_credentials,
    delete_credentials,
    get_current_windows_user,
)
from .utils import print_info, print_info_detail, print_success, print_warning, print_error, print_tech_error


AUTH_SSPI = "SSPI"
AUTH_LOGIN = "LOGIN"


def init_dotnet_and_providers():
    adomd_dll_path = os.getenv("ADOMD_DLL_PATH")
    try:
        import clr  # type: ignore
        if adomd_dll_path:
            try:
                import sys as _sys

                _sys.path.append(adomd_dll_path)
            except Exception as e:
                print(f"[INIT] Попередження: Не вдалося додати ADOMD_DLL_PATH до sys.path: {e}")
            try:
                adomd_path_obj = Path(adomd_dll_path)
                if not adomd_path_obj.exists():
                    print("[INIT] Попередження: Шлях до ADOMD.NET не знайдено. Перевірте ADOMD_DLL_PATH у .env")
            except Exception:
                pass
        else:
            print("[INIT] Попередження: Змінна ADOMD_DLL_PATH не задана. Перевірте .env")

        clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
        clr.AddReference("System.Data")
        from Microsoft.AnalysisServices.AdomdClient import AdomdConnection  # noqa: F401
        from System.Data.OleDb import OleDbConnection, OleDbCommand  # noqa: F401
        from pyadomd import Pyadomd  # type: ignore
        return Pyadomd, OleDbConnection, OleDbCommand
    except Exception as e:
        print(f"[INIT] Помилка ініціалізації .NET провайдерів/бібліотек: {e}")
        return None, None, None


def get_connection_string():
    server = os.getenv("OLAP_SERVER")
    database = os.getenv("OLAP_DATABASE")
    auth_method = os.getenv("OLAP_AUTH_METHOD", AUTH_SSPI).upper()
    connection_string = f"Provider=MSOLAP;Data Source={server};Initial Catalog={database};"

    if auth_method == AUTH_SSPI:
        connection_string += "Integrated Security=SSPI;"
        auth_details = {
            "Метод автентифікації": "Windows-автентифікація (SSPI)",
            "Поточний користувач": get_current_windows_user(),
        }
    elif auth_method == AUTH_LOGIN:
        use_encryption = os.getenv("OLAP_CREDENTIALS_ENCRYPTED", "false").lower() in ("true", "1", "yes")
        username, password = load_credentials(encrypted=use_encryption)
        if not username or not password:
            print_info("Облікові дані не знайдені або пошкоджені.")
            delete_credentials()
            from .prompt import prompt_credentials

            username, password = prompt_credentials(with_domain=True)

        if not username or not password:
            print_warning("Облікові дані не вказані. Використовуємо Windows-автентифікацію (SSPI).")
            connection_string += "Integrated Security=SSPI;"
            auth_details = {
                "Метод автентифікації": "Windows-автентифікація (SSPI) - автоматично",
                "Поточний користувач": get_current_windows_user(),
                "Причина": "Облікові дані не вказані",
            }
        else:
            connection_string += f"User ID={username};Password={password};Persist Security Info=True;Update Isolation Level=2;"
            auth_details = {
                "Метод автентифікації": "Логін/пароль",
                "Користувач": username,
                "Пароль": "********",
            }
    else:
        print_warning(f"Невідомий метод автентифікації '{auth_method}'. Використовуємо SSPI.")
        connection_string += "Integrated Security=SSPI;"
        auth_details = {
            "Метод автентифікації": "Windows-автентифікація (SSPI) - автоматично",
            "Поточний користувач": get_current_windows_user(),
            "Причина": f"Невідомий метод автентифікації: {auth_method}",
        }
    return connection_string, auth_details


class OleDbCursor:
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
            self.description = []
            for i in range(self.reader.FieldCount):
                self.description.append((self.reader.GetName(i), None, None, None, None, None, None))

    def fetchall(self):
        if not self.reader:
            return []
        rows = []
        import System  # type: ignore

        while self.reader.Read():
            row = []
            for i in range(self.reader.FieldCount):
                value = self.reader.GetValue(i)
                if value is None or isinstance(value, System.DBNull):
                    value = None
                row.append(value)
            rows.append(row)
        return rows

    def fetchone(self):
        if not self.reader or not self.reader.Read():
            return None
        row = []
        import System  # type: ignore

        for i in range(self.reader.FieldCount):
            value = self.reader.GetValue(i)
            if value is None or isinstance(value, System.DBNull):
                value = None
            row.append(value)
        return row

    def close(self):
        if self.reader and not self.reader.IsClosed:
            self.reader.Close()
        self.reader = None
        self.command = None


def connect_using_oledb(connection_string, auth_details, OleDbConnection, OleDbCommand):
    try:
        print_info_detail(
            f"Підключення до OLAP сервера {os.getenv('OLAP_SERVER')} через OleDb...",
            auth_details,
        )
        connection = OleDbConnection(connection_string)
        connection.Open()
        cursor = OleDbCursor(connection, OleDbCommand)
        print_success("Підключення до OLAP сервера через OleDb успішно встановлено")

        if os.getenv("OLAP_AUTH_METHOD", "").upper() == AUTH_LOGIN:
            user_match = re.search(r"User ID=([^;]+)", connection_string)
            password_match = re.search(r"Password=([^;]+)", connection_string)
            if user_match and password_match:
                username = user_match.group(1)
                password = password_match.group(1)
                use_encryption = os.getenv("OLAP_CREDENTIALS_ENCRYPTED", "false").lower() in ("true", "1", "yes")
                if save_credentials(username, password, encrypted=use_encryption):
                    print_success("Облікові дані успішно збережено" + (" (зашифровано)" if use_encryption else ""))
        return connection, cursor
    except Exception as e:
        print_tech_error("Помилка підключення до OLAP сервера через OleDb", e)
        return None, None


def connect_to_olap(connection_string=None, auth_details=None, retry_count=1):
    Pyadomd, OleDbConnection, OleDbCommand = init_dotnet_and_providers()
    if connection_string is None:
        connection_string, auth_details = get_connection_string()
    auth_method = os.getenv("OLAP_AUTH_METHOD", AUTH_SSPI).upper()

    try:
        if auth_method == AUTH_LOGIN and ("User ID=" in connection_string and "Password=" in connection_string):
            print_info("Використовуємо підключення через OleDbConnection для автентифікації за логіном/паролем")
            oledb_connection, cursor = connect_using_oledb(connection_string, auth_details, OleDbConnection, OleDbCommand)
            if oledb_connection and cursor:
                class OleDbConnectionWrapper:
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

                return OleDbConnectionWrapper(oledb_connection, cursor)

            if retry_count > 0:
                print_warning("Не вдалося підключитися через OleDb. Повторна спроба...")
                delete_credentials()
                from .prompt import prompt_credentials

                username, password = prompt_credentials(with_domain=True)
                if username and password:
                    new_connection_string = (
                        f"Provider=MSOLAP;Data Source={os.getenv('OLAP_SERVER')};Initial Catalog={os.getenv('OLAP_DATABASE')};"
                        f"User ID={username};Password={password};Persist Security Info=True;Update Isolation Level=2;"
                    )
                    new_auth_details = {
                        "Метод автентифікації": "Логін/пароль",
                        "Користувач": username,
                        "Пароль": "********",
                    }
                    return connect_to_olap(new_connection_string, new_auth_details, retry_count=retry_count - 1)

            print_error("Не вдалося встановити підключення через OleDb після повторних спроб")
            return None

        adomd_auth_details = {
            "Метод автентифікації": "Windows-автентифікація (SSPI)",
            "Поточний користувач": get_current_windows_user(),
        }
        if "User ID=" in connection_string:
            import re as _re

            connection_string = _re.sub(r";User ID=[^;]+;Password=[^;]+;", ";Integrated Security=SSPI;", connection_string)
        elif "Integrated Security=SSPI" not in connection_string:
            connection_string += "Integrated Security=SSPI;"

        print_info_detail(
            f"Підключення до OLAP сервера {os.getenv('OLAP_SERVER')} через ADOMD.NET...",
            adomd_auth_details,
        )

        adomd_dll_path = os.getenv("ADOMD_DLL_PATH")
        print_info(f"Шлях до ADOMD.NET: {adomd_dll_path}")
        adomd_path = Path(adomd_dll_path) if adomd_dll_path else None
        if not adomd_path or not adomd_path.exists():
            print_warning("Шлях до ADOMD.NET не знайдено! Перевірте налаштування ADOMD_DLL_PATH у файлі .env")
        else:
            dll_files = [f for f in adomd_path.iterdir() if f.name.lower().endswith(".dll")]
            adomd_files = [f.name for f in dll_files if "adomd" in f.name.lower()]
            if adomd_files:
                print_info(f"Знайдено ADOMD.NET файли: {', '.join(adomd_files)}")
            else:
                print_warning("У вказаному каталозі не знайдено файлів ADOMD.NET!")

        if Pyadomd is None:
            print_error("Pyadomd не ініціалізовано. Перевірте ADOMD_DLL_PATH та наявність бібліотек.")
            return None

        connection = Pyadomd(connection_string)
        connection.open()
        print_success("Підключення до OLAP сервера успішно встановлено")
        return connection
    except Exception as e:
        print_tech_error("Помилка підключення до OLAP сервера", e)
        return None


