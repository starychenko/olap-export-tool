"""
Модуль для парсингу аргументів командного рядка та управління конфігурацією.

Підтримує пріоритет конфігурації: CLI > Profile > .env > Defaults
"""

import argparse
import os
import sys
from typing import Any

from .utils import print_error, print_warning


def parse_arguments() -> argparse.Namespace:
    """
    Парсинг аргументів командного рядка.

    Returns:
        argparse.Namespace: Об'єкт з розпарсеними аргументами
    """
    parser = argparse.ArgumentParser(
        description="OLAP Export Tool - інструмент експорту даних з OLAP кубів",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади використання:
  # Експорт останніх 4 тижнів у XLSX з ZIP стисненням
  python olap.py --last-weeks 4 --format xlsx --compress zip

  # Експорт поточного місяця у CSV
  python olap.py --current-month --format csv

  # Використання профілю
  python olap.py --profile weekly_sales

  # Експорт конкретного періоду
  python olap.py --period 2025-01:2025-12 --format both

  # Запуск планувальника
  python olap.py --profile weekly_sales --schedule "every monday at 09:00"
        """
    )

    # Позиційні аргументи (legacy)
    parser.add_argument(
        'command',
        nargs='?',
        choices=['clear_credentials'],
        help='Команда для виконання (clear_credentials - видалити збережені облікові дані)'
    )

    # Група: Періоди (ручне вказування)
    period_manual = parser.add_argument_group('Ручне вказування періоду')
    period_manual.add_argument(
        '--period',
        type=str,
        metavar='START:END',
        help='Діапазон періодів у форматі YYYY-WW:YYYY-WW (напр. 2025-01:2025-12)'
    )
    period_manual.add_argument(
        '--start',
        type=str,
        metavar='YYYY-WW',
        help='Початковий період у форматі YYYY-WW (напр. 2025-01)'
    )
    period_manual.add_argument(
        '--end',
        type=str,
        metavar='YYYY-WW',
        help='Кінцевий період у форматі YYYY-WW (напр. 2025-52)'
    )

    # Група: Автоматичні періоди
    period_auto = parser.add_argument_group('Автоматичні періоди')
    period_auto.add_argument(
        '--last-weeks',
        type=int,
        metavar='N',
        help='Експорт останніх N тижнів (включно з поточним)'
    )
    period_auto.add_argument(
        '--current-month',
        action='store_true',
        help='Експорт всіх тижнів поточного місяця'
    )
    period_auto.add_argument(
        '--last-month',
        action='store_true',
        help='Експорт всіх тижнів попереднього місяця'
    )
    period_auto.add_argument(
        '--current-quarter',
        action='store_true',
        help='Експорт поточного кварталу (Q1-Q4)'
    )
    period_auto.add_argument(
        '--last-quarter',
        action='store_true',
        help='Експорт попереднього кварталу'
    )
    period_auto.add_argument(
        '--year-to-date',
        action='store_true',
        help='Експорт з початку року до сьогодні'
    )
    period_auto.add_argument(
        '--rolling-weeks',
        type=int,
        metavar='N',
        help='Експорт ковзаючого вікна N тижнів'
    )

    # Група: Параметри експорту
    export_group = parser.add_argument_group('Параметри експорту')
    export_group.add_argument(
        '--format',
        type=str,
        choices=['xlsx', 'csv', 'both'],
        help='Формат експорту: xlsx, csv або both (за замовчуванням з .env)'
    )
    export_group.add_argument(
        '--filter',
        type=str,
        metavar='CATEGORY',
        help='Фільтр по категорії (FILTER_FG1_NAME)'
    )
    export_group.add_argument(
        '--timeout',
        type=int,
        metavar='SECONDS',
        help='Таймаут між запитами в секундах'
    )
    export_group.add_argument(
        '--compress',
        type=str,
        choices=['zip', 'none'],
        help='Стиснення результатів у ZIP архів'
    )

    # Група: Профілі та планування
    advanced_group = parser.add_argument_group('Профілі та планування')
    advanced_group.add_argument(
        '--profile',
        type=str,
        metavar='NAME',
        help='Використати збережений профіль конфігурації'
    )
    advanced_group.add_argument(
        '--schedule',
        type=str,
        metavar='SPEC',
        help='Налаштувати розклад виконання (напр. "every monday at 09:00")'
    )
    advanced_group.add_argument(
        '--daemon',
        action='store_true',
        help='Запустити в режимі daemon (фоновий сервіс)'
    )
    advanced_group.add_argument(
        '--list-profiles',
        action='store_true',
        help='Показати список доступних профілів'
    )

    # Група: Додаткові опції
    misc_group = parser.add_argument_group('Додаткові опції')
    misc_group.add_argument(
        '--debug',
        action='store_true',
        help='Увімкнути режим налагодження'
    )

    args = parser.parse_args()

    # Обробка legacy команди clear_credentials
    if args.command == 'clear_credentials':
        args.clear_credentials = True
    else:
        args.clear_credentials = False

    return args


def validate_arguments(args: argparse.Namespace) -> bool:
    """
    Валідація аргументів командного рядка.

    Args:
        args: Розпарсені аргументи

    Returns:
        bool: True якщо валідація пройшла успішно
    """
    # Перевірка конфліктів періодів
    period_options = [
        args.period, args.start, args.end,
        args.last_weeks, args.current_month, args.last_month,
        args.current_quarter, args.last_quarter, args.year_to_date,
        args.rolling_weeks
    ]
    specified_periods = sum(1 for opt in period_options if opt)

    if specified_periods > 1:
        # Виняток: --start та --end можуть бути разом
        if not (args.start and args.end and specified_periods == 2):
            print_error("Не можна одночасно вказувати кілька варіантів періоду")
            print_warning("Виберіть один з: --period, --start/--end, --last-weeks, --current-month, і т.д.")
            return False

    # Перевірка --start та --end разом
    if (args.start and not args.end) or (args.end and not args.start):
        print_error("Параметри --start та --end мають використовуватися разом")
        return False

    # Перевірка --daemon вимагає --profile
    if args.daemon and not args.profile:
        print_error("Режим daemon вимагає вказання профілю (--profile)")
        return False

    # Перевірка --schedule вимагає --profile
    if args.schedule and not args.profile:
        print_error("Планувальник вимагає вказання профілю (--profile)")
        return False

    # Перевірка позитивних значень
    if args.last_weeks is not None and args.last_weeks < 1:
        print_error(f"Значення --last-weeks має бути більше 0, отримано: {args.last_weeks}")
        return False

    if args.rolling_weeks is not None and args.rolling_weeks < 1:
        print_error(f"Значення --rolling-weeks має бути більше 0, отримано: {args.rolling_weeks}")
        return False

    if args.timeout is not None and args.timeout < 0:
        print_error(f"Значення --timeout має бути не менше 0, отримано: {args.timeout}")
        return False

    return True


def merge_config(args: argparse.Namespace, profile_config: dict = None) -> dict:
    """
    Об'єднання конфігурації з різних джерел з урахуванням пріоритету:
    CLI аргументи > Профіль > .env файл > Defaults

    Args:
        args: Розпарсені CLI аргументи
        profile_config: Конфігурація з профілю (опційно)

    Returns:
        dict: Об'єднана конфігурація
    """
    # Базова конфігурація з .env
    config = {
        # OLAP connection
        "OLAP_SERVER": os.getenv("OLAP_SERVER"),
        "OLAP_DATABASE": os.getenv("OLAP_DATABASE"),
        "OLAP_PORT": os.getenv("OLAP_PORT"),
        "OLAP_HTTP_URL": os.getenv("OLAP_HTTP_URL"),
        "OLAP_AUTH_METHOD": os.getenv("OLAP_AUTH_METHOD", "SSPI"),
        "OLAP_DOMAIN": os.getenv("OLAP_DOMAIN"),
        "OLAP_TIMEOUT": os.getenv("OLAP_TIMEOUT"),

        # Credentials
        "OLAP_CREDENTIALS_ENCRYPTED": os.getenv("OLAP_CREDENTIALS_ENCRYPTED", "true"),
        "OLAP_CREDENTIALS_FILE": os.getenv("OLAP_CREDENTIALS_FILE", ".credentials"),
        "OLAP_USE_MASTER_PASSWORD": os.getenv("OLAP_USE_MASTER_PASSWORD", "false"),
        "OLAP_MASTER_PASSWORD": os.getenv("OLAP_MASTER_PASSWORD"),

        # Query settings
        "FILTER_FG1_NAME": os.getenv("FILTER_FG1_NAME"),
        "YEAR_WEEK_START": os.getenv("YEAR_WEEK_START"),
        "YEAR_WEEK_END": os.getenv("YEAR_WEEK_END"),
        "QUERY_TIMEOUT": int(os.getenv("QUERY_TIMEOUT", 30)),

        # Export settings
        "EXPORT_FORMAT": os.getenv("EXPORT_FORMAT", "XLSX"),
        "FORCE_CSV_ONLY": os.getenv("FORCE_CSV_ONLY", "false"),
        "XLSX_STREAMING": os.getenv("XLSX_STREAMING", "false"),
        "XLSX_MIN_FORMAT": os.getenv("XLSX_MIN_FORMAT", "false"),

        # CSV settings
        "CSV_DELIMITER": os.getenv("CSV_DELIMITER", ";"),
        "CSV_ENCODING": os.getenv("CSV_ENCODING", "utf-8-sig"),
        "CSV_QUOTING": os.getenv("CSV_QUOTING", "minimal"),

        # Excel formatting
        "EXCEL_HEADER_COLOR": os.getenv("EXCEL_HEADER_COLOR", "00365E"),
        "EXCEL_HEADER_FONT_COLOR": os.getenv("EXCEL_HEADER_FONT_COLOR", "FFFFFF"),
        "EXCEL_HEADER_FONT_SIZE": int(os.getenv("EXCEL_HEADER_FONT_SIZE", 11)),

        # Other
        "DEBUG": os.getenv("DEBUG", "false"),
        "OLAP_ASCII_LOGS": os.getenv("OLAP_ASCII_LOGS", "false"),
        "ADOMD_DLL_PATH": os.getenv("ADOMD_DLL_PATH", "./lib"),
    }

    # Перевизначення з профілю (якщо є)
    if profile_config:
        # Period settings
        if "period" in profile_config:
            period_cfg = profile_config["period"]
            if period_cfg.get("type") == "manual":
                config["YEAR_WEEK_START"] = period_cfg.get("start")
                config["YEAR_WEEK_END"] = period_cfg.get("end")

        # Export settings
        if "export" in profile_config:
            export_cfg = profile_config["export"]
            if "format" in export_cfg:
                config["EXPORT_FORMAT"] = export_cfg["format"].upper()
            if "streaming" in export_cfg:
                config["XLSX_STREAMING"] = "true" if export_cfg["streaming"] else "false"
            if "min_format" in export_cfg:
                config["XLSX_MIN_FORMAT"] = "true" if export_cfg["min_format"] else "false"

        # Filter settings
        if "filter" in profile_config:
            filter_cfg = profile_config["filter"]
            if "fg1_name" in filter_cfg:
                config["FILTER_FG1_NAME"] = filter_cfg["fg1_name"]

        # Connection settings
        if "connection" in profile_config:
            conn_cfg = profile_config["connection"]
            if "timeout" in conn_cfg:
                config["QUERY_TIMEOUT"] = conn_cfg["timeout"]

    # Перевизначення з CLI аргументів (найвищий пріоритет)
    if args.format:
        config["EXPORT_FORMAT"] = args.format.upper()

    if args.filter:
        config["FILTER_FG1_NAME"] = args.filter

    if args.timeout is not None:
        config["QUERY_TIMEOUT"] = args.timeout

    if args.debug:
        config["DEBUG"] = "true"

    # Compression (нова опція)
    config["COMPRESS"] = "none"
    if args.compress:
        config["COMPRESS"] = args.compress
    elif profile_config and "export" in profile_config:
        compress_val = profile_config["export"].get("compress", "none")
        config["COMPRESS"] = compress_val

    return config


def get_env_value(key: str, config: dict, default: Any = None) -> Any:
    """
    Отримання значення конфігурації з fallback на .env та default.

    Args:
        key: Ключ конфігурації
        config: Словник конфігурації
        default: Значення за замовчуванням

    Returns:
        Значення конфігурації
    """
    if key in config and config[key] is not None:
        return config[key]
    return os.getenv(key, default)
