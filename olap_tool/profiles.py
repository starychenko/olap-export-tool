"""
Модуль для управління профілями конфігурації у форматі YAML.

Профілі дозволяють зберігати набори налаштувань для різних сценаріїв експорту
(щотижневі звіти, місячні звіти, квартальний аналіз тощо).
"""

import os
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

from .utils import print_info, print_warning, print_error, print_success


# Директорія для зберігання профілів
PROFILES_DIR = Path("profiles")


def ensure_profiles_dir() -> None:
    """Створення директорії для профілів якщо не існує."""
    if not PROFILES_DIR.exists():
        PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        print_info(f"Створено директорію профілів: {PROFILES_DIR}")


def load_profile(profile_name: str) -> Optional[Dict[str, Any]]:
    """
    Завантаження профілю з YAML файлу.

    Args:
        profile_name: Назва профілю (без розширення .yaml)

    Returns:
        dict: Конфігурація профілю або None при помилці
    """
    if not YAML_AVAILABLE:
        print_error("PyYAML не встановлено. Виконайте: pip install PyYAML>=6.0.0")
        return None

    ensure_profiles_dir()

    profile_path = get_profile_path(profile_name)

    if not profile_path.exists():
        print_error(f"Профіль '{profile_name}' не знайдено: {profile_path}")
        print_info("Використовуйте --list-profiles для перегляду доступних профілів")
        return None

    try:
        with open(profile_path, 'r', encoding='utf-8') as f:
            profile_data = yaml.safe_load(f)

        if not profile_data:
            print_error(f"Профіль '{profile_name}' порожній або некоректний")
            return None

        print_info(f"Завантажено профіль: {profile_name}")
        if "description" in profile_data:
            print_info(f"  Опис: {profile_data['description']}")

        return profile_data

    except yaml.YAMLError as e:
        print_error(f"Помилка парсингу YAML файлу '{profile_name}': {e}")
        return None
    except Exception as e:
        print_error(f"Помилка завантаження профілю '{profile_name}': {e}")
        return None


def save_profile(profile_name: str, config: Dict[str, Any]) -> bool:
    """
    Збереження профілю у YAML файл.

    Args:
        profile_name: Назва профілю (без розширення .yaml)
        config: Конфігурація для збереження

    Returns:
        bool: True якщо збереження успішне
    """
    if not YAML_AVAILABLE:
        print_error("PyYAML не встановлено. Виконайте: pip install PyYAML>=6.0.0")
        return False

    ensure_profiles_dir()

    profile_path = get_profile_path(profile_name)

    # Додавання метаданих
    if "created" not in config:
        config["created"] = datetime.datetime.now().isoformat()
    config["updated"] = datetime.datetime.now().isoformat()

    try:
        with open(profile_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, sort_keys=False, default_flow_style=False)

        print_success(f"Профіль '{profile_name}' збережено: {profile_path}")
        return True

    except Exception as e:
        print_error(f"Помилка збереження профілю '{profile_name}': {e}")
        return False


def list_profiles() -> List[str]:
    """
    Отримання списку доступних профілів.

    Returns:
        List[str]: Список назв профілів
    """
    ensure_profiles_dir()

    profiles = []
    for file_path in PROFILES_DIR.glob("*.yaml"):
        profile_name = file_path.stem
        profiles.append(profile_name)

    return sorted(profiles)


def delete_profile(profile_name: str) -> bool:
    """
    Видалення профілю.

    Args:
        profile_name: Назва профілю для видалення

    Returns:
        bool: True якщо видалення успішне
    """
    profile_path = get_profile_path(profile_name)

    if not profile_path.exists():
        print_error(f"Профіль '{profile_name}' не знайдено")
        return False

    try:
        profile_path.unlink()
        print_success(f"Профіль '{profile_name}' видалено")
        return True

    except Exception as e:
        print_error(f"Помилка видалення профілю '{profile_name}': {e}")
        return False


def validate_profile(config: Dict[str, Any]) -> tuple[bool, str]:
    """
    Валідація конфігурації профілю.

    Args:
        config: Конфігурація для перевірки

    Returns:
        tuple[bool, str]: (успіх, повідомлення про помилку)
    """
    # Перевірка наявності імені
    if "name" not in config:
        return False, "Профіль повинен містити поле 'name'"

    # Перевірка періоду
    if "period" in config:
        period_cfg = config["period"]
        period_type = period_cfg.get("type")

        if period_type == "auto":
            auto_type = period_cfg.get("auto_type")
            valid_auto_types = [
                "last-weeks", "current-month", "last-month",
                "current-quarter", "last-quarter", "year-to-date", "rolling-weeks"
            ]
            if auto_type not in valid_auto_types:
                return False, f"Невірний auto_type: {auto_type}. Допустимі: {', '.join(valid_auto_types)}"

            if auto_type in ["last-weeks", "rolling-weeks"]:
                if "auto_value" not in period_cfg:
                    return False, f"Для {auto_type} потрібно вказати auto_value"

        elif period_type == "manual":
            if "start" not in period_cfg or "end" not in period_cfg:
                return False, "Для manual періоду потрібні поля 'start' та 'end'"

    # Перевірка формату експорту
    if "export" in config:
        export_cfg = config["export"]
        if "format" in export_cfg:
            fmt = export_cfg["format"].lower()
            if fmt not in ["xlsx", "csv", "both"]:
                return False, f"Невірний формат експорту: {fmt}. Допустимі: xlsx, csv, both"

        if "compress" in export_cfg:
            compress = export_cfg["compress"].lower()
            if compress not in ["zip", "none"]:
                return False, f"Невірний формат стиснення: {compress}. Допустимі: zip, none"

    return True, "OK"


def get_profile_path(profile_name: str) -> Path:
    """
    Отримання повного шляху до файлу профілю.

    Args:
        profile_name: Назва профілю

    Returns:
        Path: Шлях до YAML файлу
    """
    # Видаляємо розширення якщо воно є
    if profile_name.endswith('.yaml'):
        profile_name = profile_name[:-5]

    return PROFILES_DIR / f"{profile_name}.yaml"


def print_profiles_list() -> None:
    """
    Виведення списку доступних профілів з описами.
    """
    if not YAML_AVAILABLE:
        print_error("PyYAML не встановлено. Виконайте: pip install PyYAML>=6.0.0")
        return

    profiles = list_profiles()

    if not profiles:
        print_warning("Профілів не знайдено")
        print_info(f"Створіть профілі у директорії: {PROFILES_DIR}")
        return

    print_info(f"Доступні профілі ({len(profiles)}):")
    print()

    for profile_name in profiles:
        profile_data = load_profile(profile_name)
        if profile_data:
            description = profile_data.get("description", "Без опису")
            print(f"  • {profile_name}")
            print(f"    {description}")

            # Додаткова інформація про період
            if "period" in profile_data:
                period_cfg = profile_data["period"]
                period_type = period_cfg.get("type", "manual")

                if period_type == "auto":
                    auto_type = period_cfg.get("auto_type")
                    auto_value = period_cfg.get("auto_value")
                    if auto_value:
                        print(f"    Період: {auto_type} ({auto_value})")
                    else:
                        print(f"    Період: {auto_type}")
                else:
                    start = period_cfg.get("start")
                    end = period_cfg.get("end")
                    print(f"    Період: {start} - {end}")

            # Формат експорту
            if "export" in profile_data:
                export_cfg = profile_data["export"]
                fmt = export_cfg.get("format", "xlsx").upper()
                compress = export_cfg.get("compress", "none")
                print(f"    Формат: {fmt}, Стиснення: {compress}")

            print()


def create_example_profiles() -> None:
    """
    Створення прикладів профілів для демонстрації.
    """
    if not YAML_AVAILABLE:
        print_error("PyYAML не встановлено. Виконайте: pip install PyYAML>=6.0.0")
        return

    ensure_profiles_dir()

    # Профіль 1: Щотижневий звіт
    weekly_sales = {
        "name": "weekly_sales",
        "description": "Щотижневий звіт по продажам електроніки",
        "period": {
            "type": "auto",
            "auto_type": "last-weeks",
            "auto_value": 1
        },
        "export": {
            "format": "xlsx",
            "compress": "zip",
            "streaming": False,
            "min_format": False
        },
        "filter": {
            "fg1_name": "Споживча електроніка"
        },
        "connection": {
            "timeout": 30
        }
    }

    # Профіль 2: Місячний звіт
    monthly_report = {
        "name": "monthly_report",
        "description": "Місячний звіт за поточний місяць",
        "period": {
            "type": "auto",
            "auto_type": "current-month"
        },
        "export": {
            "format": "both",
            "compress": "zip",
            "streaming": True,
            "min_format": False
        },
        "filter": {
            "fg1_name": "Споживча електроніка"
        },
        "connection": {
            "timeout": 60
        },
        "schedule": {
            "enabled": True,
            "cron": "0 9 1 * *",
            "description": "Кожне 1-ше число місяця о 9:00"
        }
    }

    # Профіль 3: Квартальний аналіз
    quarterly_analysis = {
        "name": "quarterly_analysis",
        "description": "Квартальний аналіз за попередній квартал",
        "period": {
            "type": "auto",
            "auto_type": "last-quarter"
        },
        "export": {
            "format": "xlsx",
            "compress": "zip",
            "streaming": True,
            "min_format": False
        },
        "filter": {
            "fg1_name": "Споживча електроніка"
        },
        "connection": {
            "timeout": 90
        }
    }

    # Збереження профілів
    save_profile("weekly_sales", weekly_sales)
    save_profile("monthly_report", monthly_report)
    save_profile("quarterly_analysis", quarterly_analysis)

    print_success("Створено приклади профілів:")
    print("  • weekly_sales - Щотижневий звіт")
    print("  • monthly_report - Місячний звіт")
    print("  • quarterly_analysis - Квартальний аналіз")
