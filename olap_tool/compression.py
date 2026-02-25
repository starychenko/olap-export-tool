"""
Модуль для стиснення експортованих файлів у ZIP архіви.
"""

import zipfile
import datetime
from pathlib import Path
from typing import List, Optional

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

    if output_path is None:
        first_file = Path(files[0])
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = str(first_file.parent / f"{first_file.stem}_export_{timestamp}.zip")

    output_path_obj = Path(output_path)

    try:
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

                zipf.write(file_path, arcname=file_obj.name)
                total_original_size += file_obj.stat().st_size
                file_count += 1

        if file_count == 0:
            print_error("Жоден файл не був доданий до архіву")
            if output_path_obj.exists():
                output_path_obj.unlink()
            return None

        compressed_size = output_path_obj.stat().st_size
        compression_ratio = (1 - compressed_size / total_original_size) * 100 if total_original_size > 0 else 0

        original_size_str = _format_file_size(total_original_size)
        compressed_size_str = _format_file_size(compressed_size)

        print_success(f"Створено архів: {output_path}")
        print_info(f"  Файлів у архіві: {file_count}")
        print_info(f"  Оригінальний розмір: {original_size_str}")
        print_info(f"  Розмір архіву: {compressed_size_str}")
        print_info(f"  Ступінь стиснення: {compression_ratio:.1f}%")

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


def _format_file_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} Б"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} КБ"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} МБ"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} ГБ"
