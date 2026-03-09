"""
Analytics Sink абстракція.

Всі аналітичні сховища реалізують AnalyticsSink:
  - ClickHouseSink  — адаптер навколо clickhouse_export.py
  - DuckDBSink      — HTTP REST API (https://analytics.lwhs.xyz)
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import ClickHouseConfig
    from .config import DuckDBConfig


# ---------------------------------------------------------------------------
# Shared utilities (перенесено з clickhouse_export.py)
# ---------------------------------------------------------------------------

def _safe_column_name(name: str) -> str:
    """Перетворює назву колонки у безпечний SQL-ідентифікатор."""
    safe = re.sub(r"[^\w]", "_", name, flags=re.UNICODE)
    safe = re.sub(r"_+", "_", safe).strip("_")
    if not safe:
        safe = "col"
    if safe[0].isdigit():
        safe = "c_" + safe
    return safe


def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Оброблює inf/NaN та перетворює колонки на безпечні імена."""
    df = df.copy()
    df.rename(columns={col: _safe_column_name(col) for col in df.columns}, inplace=True)
    float_cols = df.select_dtypes(include=["float64", "float32"]).columns
    if len(float_cols) > 0:
        df[float_cols] = df[float_cols].replace([np.inf, -np.inf], np.nan)
    return df


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class AnalyticsSink(ABC):
    """Інтерфейс для аналітичного сховища."""

    @abstractmethod
    def setup(self, df: pd.DataFrame) -> None:
        """Створити схему/таблицю якщо не існує."""

    @abstractmethod
    def delete_period(self, year: int, week: int) -> None:
        """Видалити рядки за (year_num, week_num) для ідемпотентності."""

    @abstractmethod
    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        """Вставити рядки. Повертає кількість завантажених рядків."""

    @abstractmethod
    def close(self) -> None:
        """Закрити з'єднання/ресурси."""


# ---------------------------------------------------------------------------
# ClickHouse sink
# ---------------------------------------------------------------------------

class ClickHouseSink(AnalyticsSink):
    """
    Адаптер навколо clickhouse_export.py.
    Підтримує batch-режим: якщо client передано ззовні — не закриває з'єднання.
    """

    def __init__(self, config: "ClickHouseConfig", client=None):
        self._config = config
        self._client = client          # зовнішній клієнт (batch-режим)
        self._own_client = client is None
        self._schema: dict | None = None

    def setup(self, df: pd.DataFrame) -> None:
        from .clickhouse_export import (
            create_client, ensure_database, ensure_table, get_table_schema,
        )
        from .utils import print_progress
        if self._own_client:
            print_progress(
                f"Підключення до ClickHouse ({self._config.host}:{self._config.port})..."
            )
            self._client = create_client(self._config)
        ensure_database(self._client, self._config.database)
        ensure_table(self._client, self._config.database, self._config.table, df)
        self._schema = get_table_schema(
            self._client, self._config.database, self._config.table
        )

    def delete_period(self, year: int, week: int) -> None:
        from .clickhouse_export import _delete_period
        _delete_period(
            self._client, self._config.database, self._config.table,
            year, week, schema=self._schema,
        )

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        from .clickhouse_export import export_to_clickhouse, get_table_schema
        if self._schema is None:
            self._schema = get_table_schema(
                self._client, self._config.database, self._config.table
            )
        return export_to_clickhouse(
            df, self._config,
            year=year, week=week,
            client=self._client,
            schema=self._schema,
        )

    def close(self) -> None:
        if self._own_client and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None


# ---------------------------------------------------------------------------
# DuckDB sink (HTTP REST API)
# ---------------------------------------------------------------------------

def _pandas_dtype_to_duck(dtype) -> str:
    """Конвертує pandas dtype у DuckDB SQL тип."""
    dtype_str = str(dtype)
    if dtype_str.startswith("int") or dtype_str.startswith("uint"):
        return "BIGINT"
    if dtype_str.startswith("float"):
        return "DOUBLE"
    if dtype_str in ("bool", "boolean"):
        return "BOOLEAN"
    if dtype_str.startswith("datetime"):
        return "TIMESTAMP"
    if dtype_str.startswith("date"):
        return "DATE"
    return "VARCHAR"


def _duck_value(v) -> str:
    """Серіалізує Python-значення у SQL-літерал для DuckDB VALUES."""
    import math
    import pandas as pd

    # None, NaT та float NaN → NULL
    if v is None:
        return "NULL"
    try:
        if pd.isnull(v):
            return "NULL"
    except (TypeError, ValueError):
        pass

    # bool перед int (bool є підкласом int)
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"

    # Числа (Python int/float та numpy scalar types)
    try:
        import numpy as np
        if isinstance(v, (np.integer, np.floating)):
            if isinstance(v, np.floating) and math.isnan(float(v)):
                return "NULL"
            return str(v.item())  # .item() конвертує у Python native type
    except ImportError:
        pass

    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return "NULL"
        return str(v)

    # Рядки та datetime — екрануємо одинарні лапки
    return "'" + str(v).replace("'", "''") + "'"


class DuckDBSink(AnalyticsSink):
    """
    Завантажує DataFrame у DuckDB через REST API.

    API:
      POST /execute  {"statements": [...]}   — DDL/DML
      POST /query    {"sql": "..."}          — SELECT (для DESCRIBE)

    Ідемпотентність: DELETE WHERE year_num=X AND week_num=Y → batch INSERT.
    """

    def __init__(self, config: "DuckDBConfig"):
        import threading
        self._config = config
        self._session = self._make_session()
        self._schema: dict[str, str] | None = None
        self._schema_lock = threading.Lock()

    def _make_session(self):
        import requests
        s = requests.Session()
        s.headers.update({
            "X-API-Key": self._config.api_key,
            "Content-Type": "application/json",
        })
        return s

    def _execute(self, statements: list[str]) -> dict:
        resp = self._session.post(
            f"{self._config.url}/execute",
            json={"statements": statements},
            timeout=600,
        )
        resp.raise_for_status()
        return resp.json()

    def _query(self, sql: str) -> dict:
        resp = self._session.post(
            f"{self._config.url}/query",
            json={"sql": sql},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def setup(self, df: pd.DataFrame) -> None:
        from .utils import print_progress, print_warning
        print_progress(f"Перевірка таблиці DuckDB `{self._config.table}`...")
        cols_ddl = ", ".join(
            f'"{col}" {_pandas_dtype_to_duck(df[col].dtype)}'
            for col in df.columns
        )
        self._execute([
            f'CREATE TABLE IF NOT EXISTS "{self._config.table}" ({cols_ddl})'
        ])
        self._refresh_schema()
        with self._schema_lock:
            schema = dict(self._schema)  # type: ignore[arg-type]
        for col in df.columns:
            if col not in schema:
                dtype = _pandas_dtype_to_duck(df[col].dtype)
                try:
                    self._execute([
                        f'ALTER TABLE "{self._config.table}" '
                        f'ADD COLUMN IF NOT EXISTS "{col}" {dtype}'
                    ])
                    with self._schema_lock:
                        if self._schema is not None:
                            self._schema[col] = dtype
                except Exception as e:
                    print_warning(f"Не вдалося додати колонку `{col}`: {e} — пропускаємо")

    def _refresh_schema(self) -> None:
        result = self._query(f'DESCRIBE "{self._config.table}"')
        col_idx = result["columns"].index("column_name")
        type_idx = result["columns"].index("column_type")
        with self._schema_lock:
            self._schema = {row[col_idx]: row[type_idx] for row in result["rows"]}

    def delete_period(self, year: int, week: int) -> None:
        if self._schema is None:
            self._refresh_schema()
        with self._schema_lock:
            schema: dict[str, str] = dict(self._schema) if self._schema is not None else {}
        # Видаляємо тільки якщо обидва ключі є в схемі — інакше ризик знищити весь рік
        if "year_num" in schema and "week_num" in schema:
            self._execute([
                f'DELETE FROM "{self._config.table}" '
                f'WHERE year_num = {year} AND week_num = {week}'
            ])

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        from .utils import print_progress, print_success, print_error
        if df is None or len(df) == 0:
            return 0

        df = sanitize_df(df)  # замінює inf/-inf → NaN перед серіалізацією

        with self._schema_lock:
            schema = dict(self._schema) if self._schema else {}
        if schema:
            cols = [c for c in df.columns if c in schema]
            df = df[cols]

        if df.empty:
            return 0

        col_list = ", ".join(f'"{c}"' for c in df.columns)
        batch = self._config.batch_size
        total = len(df)

        print_progress(f"Завантаження {total} рядків у DuckDB...")

        try:
            for start in range(0, total, batch):
                chunk = df.iloc[start:start + batch]
                rows_sql = ", ".join(
                    "(" + ", ".join(_duck_value(v) for v in row) + ")"
                    for row in chunk.itertuples(index=False, name=None)
                )
                self._execute([
                    f'INSERT INTO "{self._config.table}" ({col_list}) VALUES {rows_sql}'
                ])

            print_success(
                f"Дані завантажено у DuckDB: `{self._config.table}` "
                f"({total} рядків, тиждень {year}-{week:02d})"
            )
            return total
        except Exception as e:
            print_error(f"Помилка при завантаженні у DuckDB: {e}")
            return 0

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass
