"""
Модуль для стиснення експортованих файлів у ZIP архіви.

Підтримує створення ZIP архівів з підтримкою різних опцій:
- Стиснення окремих файлів або всієї директорії
- Збереження оригінальних файлів (опційно)
- Інформація про коефіцієнт стиснення
"""

import zipfile
import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from .utils import print_info, print_success, print_warning, print_error


def compress_files(
    files: List[str],
    output_path: Optional[str] = None,
    keep_originals: bool = True
) -> Optional[str]:
    """
    Стиснення списку файлів у ZIP архів.

    Args:
        files: Список шляхів до файлів для стиснення
        output_path: Шлях до вихідного ZIP файлу (опційно)
        keep_originals: Зберігати оригінальні файли після стиснення

    Returns:
        str: Шлях до створеного ZIP архіву або None при помилці
    """
    if not files:
        print_warning("Немає файлів для стиснення")
        return None

    # Визначення вихідного шляху
    if output_path is None:
        # Автоматична генерація назви на основі першого файлу
        first_file = Path(files[0])
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = str(first_file.parent / f"{first_file.stem}_export_{timestamp}.zip")

    output_path_obj = Path(output_path)

    try:
        # Створення ZIP архіву
        total_original_size = 0
        file_count = 0

        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zipf:
            for file_path in files:
                file_obj = Path(file_path)

                if not file_obj.exists():
                    print_warning(f"Файл не знайдено, пропускаємо: {file_path}")
                    continue

                if not file_obj.is_file():
                    print_warning(f"Не є файлом, пропускаємо: {file_path}")
                    continue

                # Додаємо файл до архіву (використовуємо лише ім'я файлу без шляху)
                zipf.write(file_path, arcname=file_obj.name)
                total_original_size += file_obj.stat().st_size
                file_count += 1

        if file_count == 0:
            print_error("Жоден файл не був доданий до архіву")
            if output_path_obj.exists():
                output_path_obj.unlink()  # Видаляємо порожній архів
            return None

        # Інформація про стиснення
        compressed_size = output_path_obj.stat().st_size
        compression_ratio = (1 - compressed_size / total_original_size) * 100 if total_original_size > 0 else 0

        # Форматування розмірів
        original_size_str = _format_file_size(total_original_size)
        compressed_size_str = _format_file_size(compressed_size)

        print_success(f"Створено архів: {output_path}")
        print_info(f"  Файлів у архіві: {file_count}")
        print_info(f"  Оригінальний розмір: {original_size_str}")
        print_info(f"  Розмір архіву: {compressed_size_str}")
        print_info(f"  Ступінь стиснення: {compression_ratio:.1f}%")

        # Видалення оригінальних файлів якщо потрібно
        if not keep_originals:
            for file_path in files:
                file_obj = Path(file_path)
                if file_obj.exists() and file_obj.is_file():
                    file_obj.unlink()
            print_info(f"Видалено {file_count} оригінальних файлів")

        return str(output_path_obj)

    except Exception as e:
        print_error(f"Помилка при створенні ZIP архіву: {e}")
        return None


def compress_directory(
    directory: Path,
    pattern: str = "*",
    keep_originals: bool = True,
    output_path: Optional[str] = None
) -> Optional[str]:
    """
    Стиснення файлів з директорії за шаблоном.

    Args:
        directory: Директорія для пошуку файлів
        pattern: Шаблон для відбору файлів (напр. "*.xlsx", "*.csv")
        keep_originals: Зберігати оригінальні файли
        output_path: Шлях до вихідного ZIP файлу (опційно)

    Returns:
        str: Шлях до створеного ZIP архіву або None при помилці
    """
    directory_obj = Path(directory)

    if not directory_obj.exists():
        print_error(f"Директорія не знайдена: {directory}")
        return None

    if not directory_obj.is_dir():
        print_error(f"Шлях не є директорією: {directory}")
        return None

    # Пошук файлів за шаблоном
    files = list(directory_obj.glob(pattern))

    if not files:
        print_warning(f"Не знайдено файлів за шаблоном '{pattern}' у директорії {directory}")
        return None

    # Конвертуємо Path об'єкти у строки
    file_paths = [str(f) for f in files if f.is_file()]

    # Генерація назви архіву якщо не вказано
    if output_path is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = str(directory_obj / f"archive_{timestamp}.zip")

    return compress_files(file_paths, output_path, keep_originals)


def get_compression_info(zip_path: str) -> Optional[dict]:
    """
    Отримання детальної інформації про ZIP архів.

    Args:
        zip_path: Шлях до ZIP архіву

    Returns:
        dict: Словник з інформацією про архів або None при помилці
    """
    zip_file = Path(zip_path)

    if not zip_file.exists():
        print_error(f"ZIP архів не знайдено: {zip_path}")
        return None

    try:
        info = {
            "path": str(zip_file),
            "size": zip_file.stat().st_size,
            "size_formatted": _format_file_size(zip_file.stat().st_size),
            "files": [],
            "total_original_size": 0,
            "total_compressed_size": 0,
            "compression_ratio": 0.0
        }

        with zipfile.ZipFile(zip_path, 'r') as zipf:
            for item in zipf.filelist:
                file_info = {
                    "filename": item.filename,
                    "original_size": item.file_size,
                    "compressed_size": item.compress_size,
                    "compression_ratio": (1 - item.compress_size / item.file_size) * 100 if item.file_size > 0 else 0
                }
                info["files"].append(file_info)
                info["total_original_size"] += item.file_size
                info["total_compressed_size"] += item.compress_size

        # Загальний коефіцієнт стиснення
        if info["total_original_size"] > 0:
            info["compression_ratio"] = (1 - info["total_compressed_size"] / info["total_original_size"]) * 100

        info["file_count"] = len(info["files"])
        info["total_original_size_formatted"] = _format_file_size(info["total_original_size"])
        info["total_compressed_size_formatted"] = _format_file_size(info["total_compressed_size"])

        return info

    except Exception as e:
        print_error(f"Помилка при читанні ZIP архіву: {e}")
        return None


def _format_file_size(size_bytes: int) -> str:
    """
    Форматування розміру файлу у зручний формат.

    Args:
        size_bytes: Розмір у байтах

    Returns:
        str: Відформатований розмір (КБ, МБ, ГБ)
    """
    if size_bytes < 1024:
        return f"{size_bytes} Б"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} КБ"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} МБ"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} ГБ"
