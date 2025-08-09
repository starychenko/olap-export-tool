import os
import re
from pathlib import Path

# Припускаємо, що ці функції знаходяться у відповідних файлах .auth та .utils
from .auth import (
    save_credentials,
    load_credentials,
    delete_credentials,
    get_current_windows_user,
)
from .prompt import prompt_credentials
from .utils import print_info, print_info_detail, print_success, print_warning, print_error, print_tech_error

# Константи для методів автентифікації
AUTH_SSPI = "SSPI"
AUTH_LOGIN = "LOGIN"


def init_dotnet_and_providers():
    """Ініціалізує середовище .NET та завантажує необхідні провайдери."""
    adomd_dll_path = os.getenv("ADOMD_DLL_PATH")
    try:
        import clr  # type: ignore

        if adomd_dll_path:
            import sys
            # Перевіряємо, чи шлях вже існує, щоб не додавати його декілька разів
            if adomd_dll_path not in sys.path:
                sys.path.append(adomd_dll_path)
        else:
            print("[INIT] Попередження: Змінна ADOMD_DLL_PATH не задана. Перевірте .env")

        clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
        clr.AddReference("System.Data")

        from Microsoft.AnalysisServices.AdomdClient import AdomdConnection  # type: ignore
        from System.Data.OleDb import OleDbConnection, OleDbCommand  # type: ignore
        from pyadomd import Pyadomd  # type: ignore

        return Pyadomd, OleDbConnection, OleDbCommand
    except Exception as e:
        print(f"[INIT] Помилка ініціалізації .NET провайдерів/бібліотек: {e}")
        return None, None, None


def get_connection_string():
    """Формує рядок підключення та деталі автентифікації на основі .env."""
    server = os.getenv("OLAP_SERVER")
    database = os.getenv("OLAP_DATABASE")
    auth_method = os.getenv("OLAP_AUTH_METHOD", AUTH_SSPI).upper()

    connection_string = f"Provider=MSOLAP;Data Source={server};Initial Catalog={database};"
    auth_details = {}

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
            username, password = prompt_credentials(with_domain=True)

        if username and password:
            connection_string += f"User ID={username};Password={password};Persist Security Info=True;Update Isolation Level=2;"
            auth_details = {
                "Метод автентифікації": "Логін/пароль",
                "Користувач": username,
                "Пароль": "********",
            }
        else:
            # Фолбек на SSPI, якщо облікові дані не були надані
            print_warning("Облікові дані не вказані. Використовуємо Windows-автентифікацію (SSPI).")
            connection_string += "Integrated Security=SSPI;"
            auth_details = {
                "Метод автентифікації": "Windows-автентифікація (SSPI) - автоматично",
                "Поточний користувач": get_current_windows_user(),
                "Причина": "Облікові дані не вказані",
            }
    else:
        # Фолбек на SSPI для невідомих методів
        print_warning(f"Невідомий метод автентифікації '{auth_method}'. Використовуємо SSPI.")
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
                self.reader.GetValue(i) if not isinstance(self.reader.GetValue(i), System.DBNull) else None
                for i in range(self.reader.FieldCount)
            ]
            rows.append(row)
        return rows

    def fetchone(self):
        if not self.reader or not self.reader.Read():
            return None
        import System  # type: ignore
        return [
            self.reader.GetValue(i) if not isinstance(self.reader.GetValue(i), System.DBNull) else None
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


def connect_using_oledb(connection_string, auth_details, OleDbConnection, OleDbCommand):
    """Встановлює з'єднання через OleDb і зберігає облікові дані при успіху."""
    try:
        print_info_detail(
            f"Підключення до OLAP сервера {os.getenv('OLAP_SERVER')} через OleDb...",
            auth_details,
        )
        connection = OleDbConnection(connection_string)
        connection.Open()
        cursor = OleDbCursor(connection, OleDbCommand)
        print_success("Підключення до OLAP сервера через OleDb успішно встановлено")

        # Збереження облікових даних після успішного підключення
        user_match = re.search(r"User ID=([^;]+)", connection_string, re.IGNORECASE)
        password_match = re.search(r"Password=([^;]+)", connection_string, re.IGNORECASE)
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
    """
    Основна функція для підключення до OLAP, яка відтворює логіку старого скрипту.
    """
    Pyadomd, OleDbConnection, OleDbCommand = init_dotnet_and_providers()

    if connection_string is None:
        connection_string, auth_details = get_connection_string()

    auth_method = auth_details.get("Метод автентифікації", "")

    try:
        # Логіка, ідентична старій версії:
        # - Для "Логін/пароль" використовується OleDbConnection.
        # - Для "SSPI" (Windows Auth) використовується Pyadomd.

        if "Логін/пароль" in auth_method:
            if OleDbConnection is None or OleDbCommand is None:
                print_error("OleDb провайдер недоступний. Потрібний встановлений MSOLAP (System.Data.OleDb).")
                return None
            print_info("Використовуємо підключення через OleDbConnection для автентифікації за логіном/паролем")
            oledb_connection, cursor = connect_using_oledb(connection_string, auth_details, OleDbConnection, OleDbCommand)

            if oledb_connection and cursor:
                return OleDbConnectionWrapper(oledb_connection, cursor)
            
            # Логіка повторної спроби, якщо перша не вдалася
            if retry_count > 0:
                print_warning("Не вдалося підключитися. Спробуйте ввести облікові дані ще раз.")
                delete_credentials()
                new_username, new_password = prompt_credentials(with_domain=True)
                if new_username and new_password:
                    # Створюємо новий рядок підключення та деталі
                    new_connection_string = f"Provider=MSOLAP;Data Source={os.getenv('OLAP_SERVER')};Initial Catalog={os.getenv('OLAP_DATABASE')};User ID={new_username};Password={new_password};Persist Security Info=True;Update Isolation Level=2;"
                    new_auth_details = {
                        "Метод автентифікації": "Логін/пароль",
                        "Користувач": new_username, "Пароль": "********",
                    }
                    return connect_to_olap(new_connection_string, new_auth_details, retry_count - 1)
            
            print_error("Не вдалося встановити підключення через OleDb після повторних спроб.")
            return None

        else: # Для SSPI
            if Pyadomd is None:
                print_error("Pyadomd/ADOMD.NET недоступні. Перевірте ADOMD_DLL_PATH та наявність бібліотек.")
                return None
            print_info_detail(
                f"Підключення до OLAP сервера {os.getenv('OLAP_SERVER')} через ADOMD.NET (SSPI)...",
                auth_details,
            )
            connection = Pyadomd(connection_string)
            connection.open()
            print_success("Підключення до OLAP сервера через ADOMD.NET успішно встановлено")
            return connection

    except Exception as e:
        print_tech_error("Помилка підключення до OLAP сервера", e)
        # Тут можна додати більш детальну обробку помилок, як у старому скрипті
        return None