import csv
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import xlsxwriter  # type: ignore

if TYPE_CHECKING:
    from ..core.config import ExcelHeaderConfig, XlsxConfig


class CsvStreamWriter:
    def __init__(self, file_path: Path, delimiter: str, encoding: str, quoting_mode: str):
        self.file_path = file_path
        self.delimiter = delimiter
        self.encoding = encoding
        if quoting_mode == "all":
            self.quoting = csv.QUOTE_ALL
        elif quoting_mode == "nonnumeric":
            self.quoting = csv.QUOTE_NONNUMERIC
        else:
            self.quoting = csv.QUOTE_MINIMAL
        self.is_first = True
        self.row_count = 0

    def write_chunk(self, df: pd.DataFrame):
        # inf → NaN (to_csv з na_rep="" запише як порожній рядок)
        df_clean = df.replace([np.inf, -np.inf], np.nan)
        df_clean.to_csv(
            str(self.file_path),
            mode='w' if self.is_first else 'a',
            sep=self.delimiter,
            encoding=self.encoding,
            index=False,
            header=self.is_first,
            quoting=self.quoting,  # type: ignore[arg-type]
            na_rep=""
        )
        self.is_first = False
        self.row_count += len(df)

    def close(self):
        pass


class XlsxStreamWriter:
    def __init__(self, file_path: Path, sheet_name: str, excel_header: "ExcelHeaderConfig", xlsx_config: "XlsxConfig"):
        self.file_path_str = str(file_path)
        self.xlsx_config = xlsx_config
        # nan_inf_to_errors: NaN/Inf записуються як порожні клітинки замість помилки
        self.workbook = xlsxwriter.Workbook(self.file_path_str, {
            "constant_memory": True,
            "nan_inf_to_errors": True,
        })
        self.worksheet = self.workbook.add_worksheet(sheet_name)

        if not xlsx_config.min_format:
            self.header_format = self.workbook.add_format({
                "bold": True,
                "font_name": "Arial",
                "font_size": excel_header.font_size,
                "font_color": excel_header.font_color,
                "bg_color": excel_header.color,
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True,
                "border": 1,
            })
        else:
            self.header_format = None

        self.is_first = True
        self.row_idx = 1
        self.row_count = 0
        self.col_max_lengths: dict[int, int] = {}

    def write_chunk(self, df: pd.DataFrame):
        if self.is_first:
            columns = list(df.columns)
            if self.header_format:
                self.worksheet.write_row(0, 0, columns, self.header_format)
            else:
                self.worksheet.write_row(0, 0, columns)
            if not self.xlsx_config.min_format:
                for col_idx, col_name in enumerate(columns):
                    self.col_max_lengths[col_idx] = len(str(col_name))
            self.is_first = False

        # Ширина колонок — vectorized через pandas
        if not self.xlsx_config.min_format:
            for col_idx in range(len(df.columns)):
                series = df.iloc[:, col_idx]
                max_len = int(series.astype(str).str.len().max())
                if col_idx not in self.col_max_lengths or max_len > self.col_max_lengths[col_idx]:
                    self.col_max_lengths[col_idx] = max_len

        # DataFrame → list of lists; NaN/Inf залишаються як float('nan')/float('inf')
        # xlsxwriter з nan_inf_to_errors=True запише їх як порожні клітинки
        rows = df.values.tolist()
        for row in rows:
            self.worksheet.write_row(self.row_idx, 0, row)
            self.row_idx += 1

        self.row_count += len(df)

    def close(self):
        from ..core.utils import print_error
        try:
            if not self.xlsx_config.min_format:
                for col_idx, max_len in self.col_max_lengths.items():
                    column_width = min(max_len + 2, 100)
                    self.worksheet.set_column(col_idx, col_idx, column_width)
                self.worksheet.freeze_panes(1, 0)

            self.workbook.close()
        except Exception as e:
            print_error(f"Помилка при збереженні XLSX файлу {self.file_path_str}: {e}")
            return self.row_count, 0
        return self.row_count, Path(self.file_path_str).stat().st_size
