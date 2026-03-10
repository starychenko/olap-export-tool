"""
Analytics Sink базовий модуль.

Містить:
  - _safe_column_name() — утиліта для безпечних SQL-ідентифікаторів
  - sanitize_df()       — очищення DataFrame перед завантаженням
  - AnalyticsSink       — абстрактний базовий клас для всіх аналітичних сховищ
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Shared utilities (перенесено з sinks.py)
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
