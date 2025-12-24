"""
Модуль для автоматичних розрахунків періодів на основі ISO тижнів.

Підтримує різні варіанти автоматичних періодів:
- Останні N тижнів
- Поточний/попередній місяць
- Поточний/попередній квартал
- З початку року до сьогодні
- Ковзаючі N тижнів
"""

import datetime
from typing import List, Tuple

from .utils import print_info, print_warning, print_error


def get_iso_week_info(date: datetime.date) -> Tuple[int, int, int]:
    """
    Отримання інформації про ISO тиждень для заданої дати.

    Args:
        date: Дата для обробки

    Returns:
        Tuple[int, int, int]: (iso_year, iso_week, iso_weekday)
    """
    return date.isocalendar()


def calculate_last_weeks(n: int) -> List[Tuple[int, int]]:
    """
    Розрахунок останніх N тижнів (включно з поточним).

    Args:
        n: Кількість тижнів

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    if n < 1:
        print_error(f"Кількість тижнів має бути більше 0, отримано: {n}")
        return []

    today = datetime.date.today()
    weeks = []

    for i in range(n):
        # Віднімаємо i тижнів від сьогодні
        date = today - datetime.timedelta(weeks=i)
        year, week, _ = date.isocalendar()
        weeks.append((year, week))

    # Видаляємо дублікати та сортуємо по зростанню
    unique_weeks = sorted(set(weeks))

    print_info(f"Розраховано останніх {n} тижнів: {len(unique_weeks)} унікальних періодів")
    return unique_weeks


def calculate_current_month() -> List[Tuple[int, int]]:
    """
    Розрахунок всіх тижнів поточного місяця.

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    today = datetime.date.today()
    year = today.year
    month = today.month

    weeks = get_weeks_in_month(year, month)
    print_info(f"Розраховано тижні поточного місяця ({year}-{month:02d}): {len(weeks)} тижнів")
    return weeks


def calculate_last_month() -> List[Tuple[int, int]]:
    """
    Розрахунок всіх тижнів попереднього місяця.

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    today = datetime.date.today()
    # Перехід на попередній місяць
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - datetime.timedelta(days=1)

    year = last_day_prev_month.year
    month = last_day_prev_month.month

    weeks = get_weeks_in_month(year, month)
    print_info(f"Розраховано тижні попереднього місяця ({year}-{month:02d}): {len(weeks)} тижнів")
    return weeks


def calculate_current_quarter() -> List[Tuple[int, int]]:
    """
    Розрахунок всіх тижнів поточного кварталу (Q1-Q4).

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    today = datetime.date.today()
    year = today.year
    month = today.month

    # Визначення поточного кварталу
    quarter = (month - 1) // 3 + 1

    weeks = get_weeks_in_quarter(year, quarter)
    print_info(f"Розраховано тижні поточного кварталу ({year} Q{quarter}): {len(weeks)} тижнів")
    return weeks


def calculate_last_quarter() -> List[Tuple[int, int]]:
    """
    Розрахунок всіх тижнів попереднього кварталу.

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    today = datetime.date.today()
    year = today.year
    month = today.month

    # Визначення попереднього кварталу
    current_quarter = (month - 1) // 3 + 1
    if current_quarter == 1:
        # Якщо зараз Q1, то попередній - Q4 минулого року
        year -= 1
        quarter = 4
    else:
        quarter = current_quarter - 1

    weeks = get_weeks_in_quarter(year, quarter)
    print_info(f"Розраховано тижні попереднього кварталу ({year} Q{quarter}): {len(weeks)} тижнів")
    return weeks


def calculate_year_to_date() -> List[Tuple[int, int]]:
    """
    Розрахунок всіх тижнів з початку року до сьогодні.

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    today = datetime.date.today()
    year = today.year

    # Перший день року
    start_date = datetime.date(year, 1, 1)
    start_year, start_week, _ = start_date.isocalendar()

    # Поточний тиждень
    end_year, end_week, _ = today.isocalendar()

    weeks = []
    current_year = start_year
    current_week = start_week

    # Генерація всіх тижнів від початку року до сьогодні
    while (current_year < end_year) or (current_year == end_year and current_week <= end_week):
        weeks.append((current_year, current_week))
        current_week += 1

        # Обробка переходу на наступний рік (після тижня 52/53)
        # Перевіряємо чи існує тиждень current_week в current_year
        try:
            # Використовуємо четвер тижня для визначення року (ISO 8601)
            test_date = datetime.date.fromisocalendar(current_year, current_week, 4)
        except ValueError:
            # Тиждень не існує в поточному році, переходимо на наступний
            current_week = 1
            current_year += 1

    print_info(f"Розраховано тижні з початку року ({year}): {len(weeks)} тижнів")
    return weeks


def calculate_rolling_weeks(n: int) -> List[Tuple[int, int]]:
    """
    Розрахунок ковзаючого вікна N тижнів (закінчується поточним тижнем).

    Args:
        n: Розмір вікна в тижнях

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    if n < 1:
        print_error(f"Розмір ковзаючого вікна має бути більше 0, отримано: {n}")
        return []

    # calculate_rolling_weeks працює аналогічно до calculate_last_weeks
    # але з більш явною назвою для ковзаючого вікна
    return calculate_last_weeks(n)


def get_weeks_in_month(year: int, month: int) -> List[Tuple[int, int]]:
    """
    Отримання всіх ISO тижнів, які перетинаються з заданим місяцем.

    Тиждень вважається частиною місяця, якщо хоча б один день тижня
    припадає на цей місяць.

    Args:
        year: Рік
        month: Місяць (1-12)

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    # Перший та останній день місяця
    first_day = datetime.date(year, month, 1)

    # Останній день місяця (перший день наступного місяця мінус 1 день)
    if month == 12:
        last_day = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        last_day = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)

    weeks = set()
    current_date = first_day

    # Проходимо по всіх днях місяця та збираємо унікальні тижні
    while current_date <= last_day:
        iso_year, iso_week, _ = current_date.isocalendar()
        weeks.add((iso_year, iso_week))
        current_date += datetime.timedelta(days=1)

    return sorted(weeks)


def get_weeks_in_quarter(year: int, quarter: int) -> List[Tuple[int, int]]:
    """
    Отримання всіх ISO тижнів у заданому кварталі.

    Args:
        year: Рік
        quarter: Номер кварталу (1-4)

    Returns:
        List[Tuple[int, int]]: Список пар (year, week)
    """
    if quarter not in [1, 2, 3, 4]:
        print_error(f"Невірний номер кварталу: {quarter}. Має бути 1-4")
        return []

    # Визначення місяців кварталу
    start_month = (quarter - 1) * 3 + 1  # Q1:1, Q2:4, Q3:7, Q4:10
    end_month = start_month + 2  # Q1:3, Q2:6, Q3:9, Q4:12

    weeks = set()

    # Збираємо тижні з усіх місяців кварталу
    for month in range(start_month, end_month + 1):
        month_weeks = get_weeks_in_month(year, month)
        weeks.update(month_weeks)

    return sorted(weeks)


def filter_by_available_weeks(
    calculated_weeks: List[Tuple[int, int]],
    available_weeks: List[Tuple[int, int]]
) -> List[Tuple[int, int]]:
    """
    Фільтрація розрахованих тижнів по доступним тижням з OLAP кубу.

    Args:
        calculated_weeks: Розраховані тижні
        available_weeks: Доступні тижні з OLAP кубу

    Returns:
        List[Tuple[int, int]]: Відфільтровані тижні
    """
    available_set = set(available_weeks)
    filtered = [(y, w) for y, w in calculated_weeks if (y, w) in available_set]

    if len(filtered) < len(calculated_weeks):
        diff = len(calculated_weeks) - len(filtered)
        print_warning(
            f"Виключено {diff} тижнів, які відсутні у OLAP кубі. "
            f"Залишилось {len(filtered)} тижнів для експорту"
        )

    return filtered
