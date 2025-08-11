import csv
import math
import os
from pathlib import Path
from typing import Iterable, Tuple

import numpy as np
import pandas as pd
import xlsxwriter  # type: ignore

from .utils import print_progress, convert_dotnet_to_python
from . import progress


def export_csv_stream(
    cursor, csv_path: Path, delimiter: str, encoding: str, quoting_mode: str
) -> int:
    if quoting_mode == "all":
        quoting = csv.QUOTE_ALL
    elif quoting_mode == "nonnumeric":
        quoting = csv.QUOTE_NONNUMERIC
    else:
        quoting = csv.QUOTE_MINIMAL

    raw_columns = [desc[0] for desc in cursor.description]
    renamed_columns: list[str] = []
    potential_names: dict[str, bool] = {}
    for col in raw_columns:
        import re

        match = re.match(r"(\w+)\[([^\]]+)\]", col)
        if match:
            column_name = match.group(2)
            potential_names[column_name] = (
                False if column_name in potential_names else True
            )
        else:
            column_name = col.strip("[]")
            potential_names[column_name] = (
                False if column_name in potential_names else True
            )
    for col in raw_columns:
        import re

        match = re.match(r"(\w+)\[([^\]]+)\]", col)
        if match:
            column_name = match.group(2)
            if potential_names.get(column_name, True):
                renamed_columns.append(column_name)
            else:
                renamed_columns.append(col)
        else:
            renamed_columns.append(col.strip("[]"))

    row_count = 0
    with open(csv_path, "w", encoding=encoding, newline="") as f:
        writer = csv.writer(f, delimiter=delimiter, quoting=quoting)
        writer.writerow(renamed_columns)
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            converted_row = []
            for val in row:
                if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    val = None
                converted_row.append(val)
            writer.writerow(converted_row)
            row_count += 1
    return row_count


def export_xlsx_dataframe(df: pd.DataFrame, file_path: Path, sheet_name: str) -> int:
    print_progress(f"Експорт даних у Excel-файл {file_path}...")
    file_path_str = str(file_path)
    workbook = xlsxwriter.Workbook(file_path_str, {"constant_memory": True})
    worksheet = workbook.add_worksheet(sheet_name)
    header_format = workbook.add_format(
        {
            "bold": True,
            "font_name": "Arial",
            "font_size": int(os.getenv("EXCEL_HEADER_FONT_SIZE", 11)),
            "font_color": os.getenv("EXCEL_HEADER_FONT_COLOR", "FFFFFF"),
            "bg_color": os.getenv("EXCEL_HEADER_COLOR", "00365E"),
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
            "border": 1,
        }
    )
    worksheet.write_row(0, 0, list(df.columns), header_format)

    streaming = os.getenv("XLSX_STREAMING", "false").lower() in ("true", "1", "yes")
    if streaming:
        for row_idx, row_data in enumerate(df.itertuples(index=False), start=1):
            safe_row = []
            for cell_value in row_data:
                if isinstance(cell_value, float) and (
                    math.isnan(cell_value) or math.isinf(cell_value)
                ):
                    safe_row.append(None)
                else:
                    safe_row.append(cell_value)
            worksheet.write_row(row_idx, 0, safe_row)
    else:
        values = df.values.tolist()
        for row_idx, row_data in enumerate(values, start=1):
            safe_row = []
            for cell_value in row_data:
                if isinstance(cell_value, float) and (
                    math.isnan(cell_value) or math.isinf(cell_value)
                ):
                    safe_row.append(None)
                else:
                    safe_row.append(cell_value)
            worksheet.write_row(row_idx, 0, safe_row)

    for col_num, column in enumerate(df.columns):
        max_length = max(
            len(str(column)),
            (df.iloc[:, col_num].astype(str).str.len().max() if len(df) > 0 else 0),
        )
        column_width = min(max_length + 2, 100)
        worksheet.set_column(col_num, col_num, column_width)

    worksheet.freeze_panes(1, 0)
    workbook.close()
    return Path(file_path_str).stat().st_size


def export_xlsx_stream(cursor, file_path: Path, sheet_name: str) -> Tuple[int, int]:
    """
    Стрімінговий експорт у XLSX без проміжного DataFrame.
    Повертає (row_count, file_size_bytes).
    """
    file_path_str = str(file_path)
    # constant_memory зменшує пік пам'яті на великих наборах
    workbook = xlsxwriter.Workbook(file_path_str, {"constant_memory": True})
    worksheet = workbook.add_worksheet(sheet_name)

    # Заголовки з легким форматуванням
    min_format = os.getenv("XLSX_MIN_FORMAT", "false").lower() in ("true", "1", "yes")
    header_cells = [desc[0] for desc in cursor.description]

    # Перейменування колонок аналогічно CSV‑стріму: уникаємо конфліктів
    import re as _re

    pattern = _re.compile(r"(\w+)\[([^\]]+)\]")
    potential_names: dict[str, bool] = {}
    for col in header_cells:
        match = pattern.match(col)
        column_name = match.group(2) if match else col.strip("[]")
        potential_names[column_name] = False if column_name in potential_names else True

    renamed_columns: list[str] = []
    for col in header_cells:
        match = pattern.match(col)
        if match:
            column_name = match.group(2)
            renamed_columns.append(
                column_name if potential_names.get(column_name, True) else col
            )
        else:
            renamed_columns.append(col.strip("[]"))

    if min_format:
        worksheet.write_row(0, 0, renamed_columns)
    else:
        header_format = workbook.add_format(
            {
                "bold": True,
                "font_name": "Arial",
                "font_size": int(os.getenv("EXCEL_HEADER_FONT_SIZE", 11)),
                "font_color": os.getenv("EXCEL_HEADER_FONT_COLOR", "FFFFFF"),
                "bg_color": os.getenv("EXCEL_HEADER_COLOR", "00365E"),
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True,
                "border": 1,
            }
        )
        worksheet.write_row(0, 0, renamed_columns, header_format)

    # Рядки даних: пишемо по одному, чистимо NaN/Inf → None
    row_count = 0
    row_idx = 1
    # Спінер із лічильником рядків
    stop_event = __import__("threading").Event()
    spinner_thread = __import__("threading").Thread(
        target=progress.streaming_spinner,
        args=(
            f"Експорт даних у Excel-файл {file_path} (streaming)",
            stop_event,
            lambda: row_count,
        ),
    )
    spinner_thread.start()
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        safe_row = []
        for val in row:
            py_val = convert_dotnet_to_python(val)
            if isinstance(py_val, float) and (math.isnan(py_val) or math.isinf(py_val)):
                py_val = None
            safe_row.append(py_val)
        worksheet.write_row(row_idx, 0, safe_row)
        row_idx += 1
        row_count += 1
    # Зупиняємо спінер
    stop_event.set()
    try:
        spinner_thread.join(timeout=1.0)
    except Exception:
        pass

    # У швидкому режимі пропускаємо автоширину та freeze panes
    if not min_format:
        worksheet.freeze_panes(1, 0)

    workbook.close()
    file_size_bytes = Path(file_path_str).stat().st_size
    return row_count, file_size_bytes
