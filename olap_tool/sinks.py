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
