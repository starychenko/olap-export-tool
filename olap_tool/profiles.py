"""
Модуль для управління профілями конфігурації у форматі YAML.

Профілі дозволяють зберігати набори налаштувань для різних сценаріїв експорту.
Профілі можуть перевизначати будь-яку секцію з config.yaml.
"""

from pathlib import Path
from typing import Optional, List, Dict, Any

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

from .utils import print_info, print_warning, print_error


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

        # Зворотна сумісність: export.streaming -> xlsx.streaming
        if "export" in profile_data:
            export_cfg = profile_data["export"]
            if "streaming" in export_cfg:
                profile_data.setdefault("xlsx", {})
                profile_data["xlsx"]["streaming"] = export_cfg.pop("streaming")
            if "min_format" in export_cfg:
                profile_data.setdefault("xlsx", {})
                profile_data["xlsx"]["min_format"] = export_cfg.pop("min_format")

        # Зворотна сумісність: filter.fg1_name -> query.filter_fg1_name
        if "filter" in profile_data and "fg1_name" in profile_data["filter"]:
            profile_data.setdefault("query", {})
            profile_data["query"]["filter_fg1_name"] = profile_data["filter"]["fg1_name"]

        # Зворотна сумісність: connection.timeout -> query.timeout
        if "connection" in profile_data and "timeout" in profile_data["connection"]:
            profile_data.setdefault("query", {})
            profile_data["query"]["timeout"] = profile_data["connection"]["timeout"]

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


def get_profile_path(profile_name: str) -> Path:
    """
    Отримання повного шляху до файлу профілю.

    Args:
        profile_name: Назва профілю

    Returns:
        Path: Шлях до YAML файлу
    """
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

            # Період
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
            export_fmt = profile_data.get("export", {}).get("format", "xlsx")
            compress = profile_data.get("export", {}).get("compress", "none")
            print(f"    Формат: {export_fmt.upper()}, Стиснення: {compress}")

            print()
