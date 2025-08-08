import os
import sys
from pathlib import Path
import time
import datetime
from colorama import Fore

from .utils import print_header, print_info, print_warning, print_error, print_success, format_time, ensure_dir
from .connection import connect_to_olap, get_connection_string, AUTH_SSPI
from .queries import get_available_weeks, generate_year_week_pairs, run_dax_query
from .auth import delete_credentials, get_current_windows_user, auth_username
from .progress import TimeTracker, countdown_timer, animation_running


CURRENT_YEAR = datetime.datetime.now().year
CURRENT_WEEK = datetime.datetime.now().isocalendar()[1]


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv
    print_header("OLAP ЕКСПОРТ ДАНИХ - НАЛАШТУВАННЯ")

    if len(argv) > 1 and argv[1].lower() == "clear_credentials":
        if delete_credentials():
            print_success("Збережені облікові дані успішно видалено")
        else:
            print_error("Не вдалося видалити збережені облікові дані")
        return 0

    start_period = os.getenv("YEAR_WEEK_START")
    end_period = os.getenv("YEAR_WEEK_END")

    connection_string, auth_details = get_connection_string()
    connection = connect_to_olap(connection_string, auth_details)
    if not connection:
        print_error("Не вдалося підключитися до OLAP. Програма завершує роботу.")
        return 1

    available_weeks = get_available_weeks(connection)

    if start_period and end_period:
        year_week_pairs = generate_year_week_pairs(start_period, end_period, available_weeks)
        if not year_week_pairs:
            print_error("Не вдалося згенерувати список періодів. Використовуються значення за замовчуванням.")
            year_num = CURRENT_YEAR
            week_nums = [CURRENT_WEEK]
            year_week_pairs = [(year_num, week) for week in week_nums]
    else:
        year_num = CURRENT_YEAR
        week_nums = [CURRENT_WEEK]
        year_week_pairs = [(year_num, week) for week in week_nums]

    filter_fg1_name = os.getenv("FILTER_FG1_NAME")

    result_dir = Path("result")
    ensure_dir(result_dir)
    for year, _ in set((year, 0) for year, _ in year_week_pairs):
        ensure_dir(result_dir / str(year))

    query_timeout = int(os.getenv("QUERY_TIMEOUT", 30))

    print_header("OLAP ЕКСПОРТ ДАНИХ - ПОЧАТОК РОБОТИ")
    print_info("Налаштування:")
    print(f"   {Fore.CYAN}OLAP сервер:    {Fore.WHITE}{os.getenv('OLAP_SERVER')}")
    print(f"   {Fore.CYAN}База даних:     {Fore.WHITE}{os.getenv('OLAP_DATABASE')}")
    print(f"   {Fore.CYAN}Фільтр:         {Fore.WHITE}{filter_fg1_name}")

    auth_method = os.getenv("OLAP_AUTH_METHOD", AUTH_SSPI).upper()
    if auth_method == AUTH_SSPI:
        print(f"   {Fore.CYAN}Автентифікація: {Fore.WHITE}Windows (SSPI) як користувач {get_current_windows_user()}")
    else:
        user = auth_username or os.getenv("OLAP_USER", "Невідомий користувач")
        print(f"   {Fore.CYAN}Автентифікація: {Fore.WHITE}Логін/пароль як користувач {user} через OleDbConnection")

    if start_period and end_period:
        print(f"   {Fore.CYAN}Період:         {Fore.WHITE}з {start_period} по {end_period}")
        print(f"   {Fore.CYAN}Кількість періодів: {Fore.WHITE}{len(year_week_pairs)}")
    else:
        print(f"   {Fore.CYAN}Рік:          {Fore.WHITE}{year_num}")
        print(f"   {Fore.CYAN}Тижні:          {Fore.WHITE}{', '.join(map(str, week_nums))}")
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

    processing_time = time.time() - start_time
    print_header("ПІДСУМОК ОБРОБКИ")
    if len(year_week_pairs) > 1:
        avg_time_per_week = (
            sum(time_tracker.elapsed_times) / len(time_tracker.elapsed_times) if time_tracker.elapsed_times else 0
        )
        print_info("Деталі часу виконання:")
        print(f"   {Fore.CYAN}Загальний час:    {Fore.WHITE}{format_time(processing_time)}")
        print(f"   {Fore.CYAN}Середній час:    {Fore.WHITE}{format_time(avg_time_per_week)}")
        if time_tracker.elapsed_times:
            min_time = min(time_tracker.elapsed_times)
            max_time = max(time_tracker.elapsed_times)
            print(f"   {Fore.CYAN}Мінімальний час:  {Fore.WHITE}{format_time(min_time)}")
            print(f"   {Fore.CYAN}Максимальний час: {Fore.WHITE}{format_time(max_time)}")
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
            print(f"   {Fore.CYAN}{i}. {Fore.WHITE}{file_path} {Fore.YELLOW}({file_size})")
    else:
        print_warning("Не було створено жодного файлу")

    if connection:
        connection.close()
        print_info("Підключення до OLAP сервера закрито")
    return 0


