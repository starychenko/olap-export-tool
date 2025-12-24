"""
Модуль планувальника задач з підтримкою daemon режиму.

Використовує бібліотеку schedule для виконання задач за розкладом.
Підтримує простий формат розкладу та cron вирази.
"""

import os
import sys
import time
import signal
import datetime
from pathlib import Path
from typing import Optional, List, Any, TYPE_CHECKING

try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    schedule = None
    SCHEDULE_AVAILABLE = False

if TYPE_CHECKING:
    from schedule import Job

from .utils import print_info, print_warning, print_error, print_success
from .profiles import load_profile


# Глобальний прапор для graceful shutdown
_shutdown_requested = False


def signal_handler(signum, frame):
    """
    Обробник сигналу для graceful shutdown.
    """
    global _shutdown_requested
    print()  # Новий рядок після Ctrl+C
    print_warning("Отримано сигнал завершення. Зупинка планувальника...")
    _shutdown_requested = True


def parse_simple_schedule(schedule_spec: str) -> Optional[Any]:
    """
    Парсинг простого формату розкладу (наприклад, "every monday at 09:00").

    Args:
        schedule_spec: Рядок з описом розкладу

    Returns:
        schedule.Job: Об'єкт задачі або None при помилці
    """
    if not SCHEDULE_AVAILABLE:
        print_error("Бібліотека schedule не встановлена. Виконайте: pip install schedule>=1.2.0")
        return None

    spec_lower = schedule_spec.lower().strip()

    try:
        # Формат: "every monday at 09:00"
        if "every" in spec_lower and "at" in spec_lower:
            parts = spec_lower.split("at")
            day_part = parts[0].strip().replace("every", "").strip()
            time_part = parts[1].strip()

            # Визначення дня тижня або періоду
            if "monday" in day_part:
                job = schedule.every().monday.at(time_part)
            elif "tuesday" in day_part:
                job = schedule.every().tuesday.at(time_part)
            elif "wednesday" in day_part:
                job = schedule.every().wednesday.at(time_part)
            elif "thursday" in day_part:
                job = schedule.every().thursday.at(time_part)
            elif "friday" in day_part:
                job = schedule.every().friday.at(time_part)
            elif "saturday" in day_part:
                job = schedule.every().saturday.at(time_part)
            elif "sunday" in day_part:
                job = schedule.every().sunday.at(time_part)
            elif "day" in day_part:
                job = schedule.every().day.at(time_part)
            else:
                print_error(f"Невідомий день тижня: {day_part}")
                return None

            return job

        # Формат: "every 1 week"
        elif "every" in spec_lower and ("week" in spec_lower or "day" in spec_lower or "hour" in spec_lower):
            parts = spec_lower.split()
            if len(parts) >= 3:
                number = int(parts[1])
                unit = parts[2]

                if "week" in unit:
                    job = schedule.every(number).weeks
                elif "day" in unit:
                    job = schedule.every(number).days
                elif "hour" in unit:
                    job = schedule.every(number).hours
                else:
                    print_error(f"Невідома одиниця часу: {unit}")
                    return None

                return job

        else:
            print_error(f"Невірний формат розкладу: {schedule_spec}")
            print_info("Приклади правильних форматів:")
            print_info("  - every monday at 09:00")
            print_info("  - every day at 18:00")
            print_info("  - every 1 week")
            return None

    except Exception as e:
        print_error(f"Помилка парсингу розкладу '{schedule_spec}': {e}")
        return None


def run_scheduled_task(profile_name: str) -> None:
    """
    Виконання задачі з профілю.

    Args:
        profile_name: Назва профілю для виконання
    """
    from .runner import main
    from .cli import parse_arguments

    print()
    print_info(f"Запуск задачі: {profile_name} о {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Підготовка аргументів для runner
        saved_argv = sys.argv.copy()
        sys.argv = ['olap.py', '--profile', profile_name]

        # Виконання основної функції
        return_code = main()

        # Відновлення argv
        sys.argv = saved_argv

        if return_code == 0:
            print_success(f"Задача '{profile_name}' виконана успішно")
        else:
            print_error(f"Задача '{profile_name}' завершилась з помилкою (код: {return_code})")

    except Exception as e:
        print_error(f"Помилка виконання задачі '{profile_name}': {e}")
    finally:
        # Відновлення argv на випадок помилки
        if 'saved_argv' in locals():
            sys.argv = saved_argv


def start_scheduler(profile_name: str, schedule_spec: str) -> int:
    """
    Запуск планувальника для одного профілю з вказаним розкладом.

    Args:
        profile_name: Назва профілю
        schedule_spec: Розклад у простому форматі

    Returns:
        int: Код завершення (0 = успіх)
    """
    if not SCHEDULE_AVAILABLE:
        print_error("Бібліотека schedule не встановлена")
        print_info("Виконайте: pip install schedule>=1.2.0")
        return 1

    # Реєстрація обробника сигналів
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print_info(f"Планувальник задач - профіль '{profile_name}'")
    print_info(f"Розклад: {schedule_spec}")

    # Парсинг розкладу
    job = parse_simple_schedule(schedule_spec)
    if job is None:
        return 1

    # Прив'язка функції до задачі
    job.do(run_scheduled_task, profile_name)

    print_success("Планувальник запущено. Натисніть Ctrl+C для зупинки")

    # Показ наступного запуску
    if schedule.jobs:
        next_run = schedule.next_run()
        if next_run:
            print_info(f"Наступний запуск: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

    # Основний цикл планувальника
    global _shutdown_requested
    while not _shutdown_requested:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print_error(f"Помилка в циклі планувальника: {e}")
            time.sleep(5)

    print_info("Планувальник зупинено")
    return 0


def daemon_mode(profiles: List[str]) -> int:
    """
    Запуск у daemon режимі з кількома профілями.

    Args:
        profiles: Список назв профілів для виконання

    Returns:
        int: Код завершення (0 = успіх)
    """
    if not SCHEDULE_AVAILABLE:
        print_error("Бібліотека schedule не встановлена")
        print_info("Виконайте: pip install schedule>=1.2.0")
        return 1

    # Реєстрація обробника сигналів
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print_info("Daemon режим - планувальник задач")
    print_info(f"Профілів для обробки: {len(profiles)}")

    # Створення директорії для логів
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Ім'я лог файлу з датою
    log_filename = logs_dir / f"scheduler_{datetime.datetime.now().strftime('%Y-%m-%d')}.log"

    scheduled_count = 0

    # Завантаження профілів та налаштування розкладу
    for profile_name in profiles:
        profile_data = load_profile(profile_name)
        if not profile_data:
            print_warning(f"Не вдалося завантажити профіль '{profile_name}', пропускаємо")
            continue

        # Перевірка наявності розкладу
        if "schedule" not in profile_data:
            print_warning(f"Профіль '{profile_name}' не містить розкладу, пропускаємо")
            continue

        schedule_cfg = profile_data["schedule"]
        if not schedule_cfg.get("enabled", False):
            print_info(f"Розклад для '{profile_name}' вимкнено, пропускаємо")
            continue

        # Отримання розкладу
        schedule_spec = schedule_cfg.get("simple") or schedule_cfg.get("cron")
        if not schedule_spec:
            print_warning(f"Не знайдено розкладу для '{profile_name}', пропускаємо")
            continue

        # Для cron використовуємо простий формат (потрібна конвертація)
        if "cron" in schedule_cfg:
            # Спрощена підтримка cron: "0 9 * * 1" -> "every monday at 09:00"
            print_warning(f"Cron формат для '{profile_name}' не повністю підтримується")
            print_info("Рекомендується використовувати простий формат у полі 'simple'")
            continue

        # Парсинг та додавання задачі
        job = parse_simple_schedule(schedule_spec)
        if job:
            job.do(run_scheduled_task, profile_name)
            scheduled_count += 1
            description = schedule_cfg.get("description", schedule_spec)
            print_success(f"✓ {profile_name}: {description}")

    if scheduled_count == 0:
        print_error("Не вдалося налаштувати жодної задачі")
        print_info("Перевірте налаштування розкладу у профілях")
        return 1

    print_success(f"Налаштовано задач: {scheduled_count}")
    print_info(f"Логи зберігаються у: {log_filename}")

    # Показ наступних запусків
    if schedule.jobs:
        print_info("Наступні запуски:")
        for job in schedule.jobs[:5]:  # Показуємо перші 5
            next_run = job.next_run
            if next_run:
                print(f"  • {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

    print()
    print_success("Daemon режим активний. Натисніть Ctrl+C для зупинки")

    # Основний цикл
    global _shutdown_requested
    while not _shutdown_requested:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print_error(f"Помилка в daemon циклі: {e}")
            # Логування помилки
            with open(log_filename, 'a', encoding='utf-8') as f:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"[{timestamp}] ERROR {e}\n")
            time.sleep(5)

    print_info("Daemon режим зупинено")
    return 0


def stop_scheduler() -> None:
    """
    Зупинка планувальника (очищення всіх задач).
    """
    if schedule is not None:
        schedule.clear()
        print_info("Всі заплановані задачі видалено")
