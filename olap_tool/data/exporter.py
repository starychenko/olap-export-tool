import csv
import math
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

import pandas as pd
import xlsxwriter  # type: ignore

from ..core.utils import print_progress, convert_dotnet_to_python
from ..core import progress

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
        df_replaced = df.replace([math.inf, -math.inf], None)
        df_replaced.to_csv(
            str(self.file_path),
            mode='w' if self.is_first else 'a',
            sep=self.delimiter,
            encoding=self.encoding,
            index=False,
            header=self.is_first,
            quoting=self.quoting,
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
        self.workbook = xlsxwriter.Workbook(self.file_path_str, {"constant_memory": True})
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
        self.col_max_lengths = {}
        
    def write_chunk(self, df: pd.DataFrame):
        if self.is_first:
            if self.header_format:
                self.worksheet.write_row(0, 0, list(df.columns), self.header_format)
            else:
                self.worksheet.write_row(0, 0, list(df.columns))
            self.is_first = False
            
        for row_data in df.itertuples(index=False):
            safe_row = []
            for col_idx, cell_value in enumerate(row_data):
                if isinstance(cell_value, float) and (math.isnan(cell_value) or math.isinf(cell_value)):
                    safe_row.append(None)
                else:
                    safe_row.append(cell_value)
                    
                # Track max length for column sizing if needed
                if not self.xlsx_config.min_format:
                    str_len = len(str(cell_value)) if cell_value is not None else 0
                    if col_idx not in self.col_max_lengths or str_len > self.col_max_lengths[col_idx]:
                        self.col_max_lengths[col_idx] = str_len
                        
            self.worksheet.write_row(self.row_idx, 0, safe_row)
            self.row_idx += 1
            
        self.row_count += len(df)

    def close(self):
        if not self.xlsx_config.min_format:
            # We must apply columns widths based on tracked lengths
            for col_idx, max_len in self.col_max_lengths.items():
                # We need to consider the header length as well, but we don't have access to the exact header string length here easily unless we tracked it.
                # Just use max_len + 2, capped at 100.
                column_width = min(max_len + 2, 100)
                self.worksheet.set_column(col_idx, col_idx, column_width)
            self.worksheet.freeze_panes(1, 0)
            
        self.workbook.close()
        return self.row_count, Path(self.file_path_str).stat().st_size
