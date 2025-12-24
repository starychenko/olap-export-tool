import os
import sys
from pathlib import Path
import time
import datetime
from colorama import Fore

from .utils import (
    print_header,
    print_info,
    print_warning,
    print_error,
    print_success,
    format_time,
    ensure_dir,
)
from .connection import connect_to_olap, get_connection_string, AUTH_SSPI
from .queries import get_available_weeks, generate_year_week_pairs, run_dax_query
from .auth import delete_credentials, get_current_windows_user, auth_username
from .progress import TimeTracker, countdown_timer, animation_running
from .cli import parse_arguments, merge_config, validate_arguments
from . import periods
from .compression import compress_files
from .profiles import load_profile, print_profiles_list
from .scheduler import start_scheduler, daemon_mode


CURRENT_YEAR = datetime.datetime.now().year
CURRENT_WEEK = datetime.datetime.now().isocalendar()[1]


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv

    # Парсинг CLI аргументів
    args = parse_arguments()

    # Валідація аргументів
    if not validate_arguments(args):
        return 1

    print_header("OLAP ЕКСПОРТ ДАНИХ - НАЛАШТУВАННЯ")

    # Legacy: обробка clear_credentials
    if args.clear_credentials:
        if delete_credentials():
            print_success("Збережені облікові дані успішно видалено")
        else:
            print_error("Не вдалося видалити збережені облікові дані")
        return 0

    # Обробка --list-profiles
    if args.list_profiles:
        print_profiles_list()
        return 0

    # Обробка daemon режиму
    if args.daemon:
        if not args.profile:
            print_error("Режим daemon вимагає вказання профілю (--profile)")
            return 1
        return daemon_mode([args.profile])

    # Обробка планувальника
    if args.schedule:
        if not args.profile:
            print_error("Планувальник вимагає вказання профілю (--profile)")
            return 1
        return start_scheduler(args.profile, args.schedule)

    # Завантаження профілю
    profile_config = {}
    if args.profile:
        profile_config = load_profile(args.profile)
        if not profile_config:
            return 1

    # Об'єднання конфігурації (CLI > Profile > .env)
    config = merge_config(args, profile_config)

    start_period = config.get("YEAR_WEEK_START")
    end_period = config.get("YEAR_WEEK_END")

    connection_string, auth_details = get_connection_string()
    connection = connect_to_olap(connection_string, auth_details)
    if not connection:
        print_error("Не вдалося підключитися до OLAP. Програма завершує роботу.")
        return 1

    available_weeks = get_available_weeks(connection)

    # Визначення періоду з урахуванням CLI аргументів та профілю
    year_week_pairs = None

    # Пріоритет 1: Автоматичні періоди з CLI
    if args.last_weeks:
        calculated_weeks = periods.calculate_last_weeks(args.last_weeks)
        year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
    elif args.current_month:
        calculated_weeks = periods.calculate_current_month()
        year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
    elif args.last_month:
        calculated_weeks = periods.calculate_last_month()
        year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
    elif args.current_quarter:
        calculated_weeks = periods.calculate_current_quarter()
        year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
    elif args.last_quarter:
        calculated_weeks = periods.calculate_last_quarter()
        year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
    elif args.year_to_date:
        calculated_weeks = periods.calculate_year_to_date()
        year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
    elif args.rolling_weeks:
        calculated_weeks = periods.calculate_rolling_weeks(args.rolling_weeks)
        year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
    # Пріоритет 2: Ручні періоди з CLI
    elif args.period:
        # Формат: 2025-01:2025-12
        try:
            start_period, end_period = args.period.split(":")
            year_week_pairs = generate_year_week_pairs(start_period, end_period, available_weeks)
        except ValueError:
            print_error(f"Невірний формат --period. Використовуйте формат YYYY-WW:YYYY-WW")
            return 1
    elif args.start and args.end:
        year_week_pairs = generate_year_week_pairs(args.start, args.end, available_weeks)
    # Пріоритет 3: Періоди з профілю
    elif profile_config and "period" in profile_config:
        period_cfg = profile_config["period"]
        period_type = period_cfg.get("type")

        if period_type == "auto":
            auto_type = period_cfg.get("auto_type")
            auto_value = period_cfg.get("auto_value")

            print_info(f"Використання періоду з профілю: {auto_type}")

            if auto_type == "last-weeks" and auto_value:
                calculated_weeks = periods.calculate_last_weeks(auto_value)
                year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
            elif auto_type == "current-month":
                calculated_weeks = periods.calculate_current_month()
                year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
            elif auto_type == "last-month":
                calculated_weeks = periods.calculate_last_month()
                year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
            elif auto_type == "current-quarter":
                calculated_weeks = periods.calculate_current_quarter()
                year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
            elif auto_type == "last-quarter":
                calculated_weeks = periods.calculate_last_quarter()
                year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
            elif auto_type == "year-to-date":
                calculated_weeks = periods.calculate_year_to_date()
                year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)
            elif auto_type == "rolling-weeks" and auto_value:
                calculated_weeks = periods.calculate_rolling_weeks(auto_value)
                year_week_pairs = periods.filter_by_available_weeks(calculated_weeks, available_weeks)

        elif period_type == "manual":
            manual_start = period_cfg.get("start")
            manual_end = period_cfg.get("end")
            if manual_start and manual_end:
                year_week_pairs = generate_year_week_pairs(manual_start, manual_end, available_weeks)

    # Пріоритет 4: Періоди з .env
    elif start_period and end_period:
        year_week_pairs = generate_year_week_pairs(
            start_period, end_period, available_weeks
        )

    # Fallback: поточний тиждень
    if not year_week_pairs:
        print_warning(
            "Не вдалося згенерувати список періодів. Використовується поточний тиждень."
        )
        year_num = CURRENT_YEAR
        week_nums = [CURRENT_WEEK]
        year_week_pairs = [(year_num, week) for week in week_nums]

    filter_fg1_name = config.get("FILTER_FG1_NAME")

    result_dir = Path("result")
    ensure_dir(result_dir)
    for year, _ in set((year, 0) for year, _ in year_week_pairs):
        ensure_dir(result_dir / str(year))

    query_timeout = config.get("QUERY_TIMEOUT", 30)

    print_header("OLAP ЕКСПОРТ ДАНИХ - ПОЧАТОК РОБОТИ")
    print_info("Налаштування:")
    print(f"   {Fore.CYAN}OLAP сервер:    {Fore.WHITE}{os.getenv('OLAP_SERVER')}")
    print(f"   {Fore.CYAN}База даних:     {Fore.WHITE}{os.getenv('OLAP_DATABASE')}")
    print(f"   {Fore.CYAN}Фільтр:         {Fore.WHITE}{filter_fg1_name}")

    auth_method = os.getenv("OLAP_AUTH_METHOD", AUTH_SSPI).upper()
    if auth_method == AUTH_SSPI:
        print(
            f"   {Fore.CYAN}Автентифікація: {Fore.WHITE}Windows (SSPI) як користувач {get_current_windows_user()}"
        )
    else:
        user = auth_username or os.getenv("OLAP_USER", "Невідомий користувач")
        print(
            f"   {Fore.CYAN}Автентифікація: {Fore.WHITE}Логін/пароль як користувач {user} через OleDbConnection"
        )

    if start_period and end_period:
        print(
            f"   {Fore.CYAN}Період:         {Fore.WHITE}з {start_period} по {end_period}"
        )
        print(f"   {Fore.CYAN}Кількість періодів: {Fore.WHITE}{len(year_week_pairs)}")
    else:
        print(f"   {Fore.CYAN}Рік:          {Fore.WHITE}{year_num}")
        print(
            f"   {Fore.CYAN}Тижні:          {Fore.WHITE}{', '.join(map(str, week_nums))}"
        )
    print(f"   {Fore.CYAN}Таймаут:        {Fore.WHITE}{query_timeout} секунд")

    start_time = time.time()
    files_created: list[str] = []
    print_info(f"Запуск обробки для {len(year_week_pairs)} тижнів...")
    time_tracker = TimeTracker(len(year_week_pairs))
    for i, (year, week) in enumerate(year_week_pairs):
        if i > 0:
            print(f"\n{Fore.YELLOW}{'-' * 40}")
            print_info(f"Очікування {query_timeout} секунд перед наступним запитом...")
            time_tracker.start_waiting()
            countdown_timer(query_timeout)
            time_tracker.end_waiting()
        reporting_period = f"{year}-{week:02d}"
        print(f"\n{Fore.CYAN}{'-' * 40}")
        if i > 0:
            print(f"{Fore.MAGENTA}{time_tracker.get_progress_info()}")
        print_info(f"Обробка тижня: {reporting_period} ({i+1}/{len(year_week_pairs)})")
        file_path = run_dax_query(connection, reporting_period)
        if file_path:
            files_created.append(file_path)
        time_tracker.update()

    # Стиснення файлів якщо вказано --compress zip
    zip_file_path = None
    if config.get("COMPRESS") == "zip" and files_created:
        print(f"\n{Fore.CYAN}{'-' * 40}")
        print_info("Стиснення файлів у ZIP архів...")
        # Генерація назви архіву на основі періоду
        if len(year_week_pairs) == 1:
            # Один тиждень - використовуємо назву файлу
            zip_file_path = compress_files(files_created, keep_originals=True)
        else:
            # Кілька тижнів - створюємо архів з датою
            first_year, first_week = year_week_pairs[0]
            last_year, last_week = year_week_pairs[-1]
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = f"{first_year}-{first_week:02d}_to_{last_year}-{last_week:02d}_export_{timestamp}.zip"
            zip_output_path = result_dir / str(first_year) / zip_name
            zip_file_path = compress_files(files_created, output_path=str(zip_output_path), keep_originals=True)

    processing_time = time.time() - start_time
    print_header("ПІДСУМОК ОБРОБКИ")
    if len(year_week_pairs) > 1:
        avg_time_per_week = (
            sum(time_tracker.elapsed_times) / len(time_tracker.elapsed_times)
            if time_tracker.elapsed_times
            else 0
        )
        print_info("Деталі часу виконання:")
        print(
            f"   {Fore.CYAN}Загальний час:    {Fore.WHITE}{format_time(processing_time)}"
        )
        print(
            f"   {Fore.CYAN}Середній час:    {Fore.WHITE}{format_time(avg_time_per_week)}"
        )
        if time_tracker.elapsed_times:
            min_time = min(time_tracker.elapsed_times)
            max_time = max(time_tracker.elapsed_times)
            print(
                f"   {Fore.CYAN}Мінімальний час:  {Fore.WHITE}{format_time(min_time)}"
            )
            print(
                f"   {Fore.CYAN}Максимальний час: {Fore.WHITE}{format_time(max_time)}"
            )
    else:
        print_success(f"Обробку завершено за {format_time(processing_time)}")

    print_info(f"Створено файлів: {len(files_created)}")
    if files_created:
        for i, file_path in enumerate(files_created, 1):
            path = Path(file_path)
            file_size_bytes = path.stat().st_size
            if file_size_bytes < 1024 * 1024:
                file_size = f"{file_size_bytes / 1024:.1f} КБ"
            else:
                file_size = f"{file_size_bytes / (1024 * 1024):.2f} МБ"
            print(
                f"   {Fore.CYAN}{i}. {Fore.WHITE}{file_path} {Fore.YELLOW}({file_size})"
            )
    else:
        print_warning("Не було створено жодного файлу")

    if connection:
        connection.close()
        print_info("Підключення до OLAP сервера закрито")
    return 0
