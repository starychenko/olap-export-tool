"""
ClickHouse sink — поєднує ClickHouseSink та clickhouse_export логіку.

Містить:
  - _pandas_dtype_to_ch()   — маппінг pandas dtype → ClickHouse тип
  - ensure_database()       — CREATE DATABASE IF NOT EXISTS
  - ensure_table()          — CREATE TABLE IF NOT EXISTS зі схемою з DataFrame
  - get_table_schema()      — читає {col: ch_type} з system.columns
  - _coerce_col_to_ch_type() — конвертує Series у тип CH-стовпця
  - _align_df_to_table()    — вирівнює DataFrame під схему таблиці
  - _delete_period()        — lightweight DELETE за (year_num, week_num)
  - create_client()         — фабрика clickhouse_connect клієнта
  - export_to_clickhouse()  — головна функція завантаження
  - ClickHouseSink          — реалізація AnalyticsSink для ClickHouse
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pandas as pd

from .base import AnalyticsSink, sanitize_df, _safe_column_name  # noqa: F401

if TYPE_CHECKING:
    from ..config import ClickHouseConfig


# ---------------------------------------------------------------------------
# Type mapping: pandas dtype -> ClickHouse type
# ---------------------------------------------------------------------------

def _pandas_dtype_to_ch(dtype) -> str:
    """Конвертує pandas dtype у ClickHouse-тип."""
    dtype_str = str(dtype)
    if dtype_str.startswith("int"):
        return "Int64"
    if dtype_str.startswith("uint"):
        return "UInt64"
    if dtype_str.startswith("float"):
        return "Float64"
    if dtype_str in ("bool", "boolean"):
        return "UInt8"
    if dtype_str.startswith("datetime"):
        return "DateTime"
    if dtype_str.startswith("date"):
        return "Date"
    # object, string, category → String
    return "Nullable(String)"


# ---------------------------------------------------------------------------
# Database & table management
# ---------------------------------------------------------------------------

def ensure_database(client, database: str) -> None:
    """Створює базу даних якщо не існує."""
    client.command(f"CREATE DATABASE IF NOT EXISTS `{database}`")


def _build_create_table_sql(database: str, table: str, df: pd.DataFrame) -> str:
    """Генерує DDL для створення таблиці зі схемою з DataFrame."""
    columns_ddl = [f"    `{col}` {_pandas_dtype_to_ch(df[col].dtype)}" for col in df.columns]
    columns_str = ",\n".join(columns_ddl)

    # Використовуємо year_num/week_num як ключ сортування якщо вони є в DataFrame.
    order_cols = [c for c in ("year_num", "week_num") if c in df.columns]
    order_by = ", ".join(f"`{c}`" for c in order_cols) if order_cols else "tuple()"

    return (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}`\n"
        f"(\n{columns_str}\n"
        f") ENGINE = MergeTree()\n"
        f"ORDER BY ({order_by})"
    )


def ensure_table(client, database: str, table: str, df: pd.DataFrame) -> None:
    """Створює таблицю якщо не існує, зі схемою з DataFrame."""
    client.command(_build_create_table_sql(database, table, df))


def get_table_schema(client, database: str, table: str) -> dict[str, str]:
    """Повертає {column_name: ch_type} для існуючої таблиці."""
    result = client.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = {db:String} AND table = {tbl:String}",
        parameters={"db": database, "tbl": table},
    )
    return {row[0]: row[1] for row in result.result_rows}


# ---------------------------------------------------------------------------
# Schema alignment: приводимо DataFrame під схему таблиці
# ---------------------------------------------------------------------------

def _coerce_col_to_ch_type(series: pd.Series, ch_type: str) -> pd.Series:
    """Конвертує pandas Series у тип, сумісний із ClickHouse-стовпцем."""
    if "Int" in ch_type or "UInt" in ch_type:
        # Nullable Int64 — без проміжного float64, точність не втрачається
        return pd.to_numeric(series, errors="coerce").astype(pd.Int64Dtype())
    if "Float" in ch_type:
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if "String" in ch_type:
        # Векторизована конвертація: astype(str) → виправляємо NaN-позиції → object
        null_mask = series.isna()
        result = series.astype(str).astype(object)
        result[null_mask] = None
        return result
    if "DateTime" in ch_type or "Date" in ch_type:
        return pd.to_datetime(series, errors="coerce")
    return series


def _align_df_to_table(
    client, database: str, table: str, df: pd.DataFrame,
    schema: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Вирівнює DataFrame під схему ClickHouse-таблиці:
    - Пропускає колонки яких немає в таблиці
    - Конвертує типи під реальну CH-схему
    - Додає нові колонки до таблиці якщо їх ще немає

    schema: якщо передано — не робить зайвий запит до system.columns.
    """
    from ..utils import print_warning

    if schema is None:
        schema = get_table_schema(client, database, table)

    # Нові колонки в df яких ще немає в таблиці — додаємо через ALTER TABLE
    for col in (col for col in df.columns if col not in schema):
        ch_type = _pandas_dtype_to_ch(df[col].dtype)
        try:
            client.command(
                f"ALTER TABLE `{database}`.`{table}` "
                f"ADD COLUMN IF NOT EXISTS `{col}` {ch_type}"
            )
            schema[col] = ch_type
        except Exception as e:
            print_warning(f"Не вдалося додати колонку `{col}`: {e} — пропускаємо")

    # Залишаємо тільки колонки що є в таблиці, конвертуємо типи
    aligned_cols = [col for col in df.columns if col in schema]
    df_aligned = df[aligned_cols].copy()

    for col in aligned_cols:
        try:
            df_aligned[col] = _coerce_col_to_ch_type(df_aligned[col], schema[col])
        except Exception:
            pass  # якщо конвертація не вдалася — залишаємо як є

    return df_aligned


# ---------------------------------------------------------------------------
# Upsert: delete existing week then insert
# ---------------------------------------------------------------------------

def _delete_period(
    client, database: str, table: str, year: int, week: int,
    schema: Optional[dict] = None,
) -> None:
    """
    Видаляє рядки за (year, week) перед вставкою для ідемпотентності.
    Використовує lightweight DELETE (ClickHouse 22.8+) — не мутація,
    виконується швидко і не блокує паралельні потоки.
    """
    if schema is None:
        schema = get_table_schema(client, database, table)

    conditions = []
    if "year_num" in schema:
        conditions.append(f"year_num = {year}")
    if "week_num" in schema:
        conditions.append(f"week_num = {week}")

    if conditions:
        where = " AND ".join(conditions)
        client.command(f"DELETE FROM `{database}`.`{table}` WHERE {where}")


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def create_client(config: "ClickHouseConfig"):
    """Створює та повертає clickhouse_connect клієнт."""
    try:
        import clickhouse_connect
    except ImportError:
        raise ImportError(
            "clickhouse-connect не встановлено. "
            "Виконайте: pip install clickhouse-connect"
        )
    return clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        secure=config.secure,
        connect_timeout=30,
        send_receive_timeout=600,
        compress="lz4",
    )


# ---------------------------------------------------------------------------
# Main export function
# ---------------------------------------------------------------------------

def export_to_clickhouse(
    df: pd.DataFrame,
    config: "ClickHouseConfig",
    year: int,
    week: int,
    client=None,
    schema: Optional[dict] = None,
    silent: bool = False,
) -> int:
    """
    Завантажує DataFrame у ClickHouse.

    Args:
        client:  Якщо передано — DDL пропускається, з'єднання не закривається.
                 Використовується для batch-режиму (thread-local клієнти).
        schema:  Якщо передано — пропускає запит до system.columns.
        silent:  Якщо True — не друкує progress/success повідомлення.
                 Помилки та попередження виводяться завжди.

    Returns:
        Кількість завантажених рядків.
    """
    from ..utils import print_success, print_warning, print_error, print_progress

    def _log(fn, msg):
        if not silent:
            fn(msg)

    if df is None or len(df) == 0:
        _log(print_warning, "DataFrame порожній — пропускаємо завантаження у ClickHouse")
        return 0

    own_client = client is None
    if own_client:
        _log(print_progress, f"Підключення до ClickHouse ({config.host}:{config.port})...")
        try:
            client = create_client(config)
        except Exception as e:
            print_error(f"Не вдалося підключитися до ClickHouse: {e}")
            return 0

    df_clean = sanitize_df(df)

    try:
        if own_client:
            _log(print_progress, f"Перевірка бази даних `{config.database}`...")
            ensure_database(client, config.database)
            _log(print_progress, f"Перевірка таблиці `{config.database}`.`{config.table}`...")
            ensure_table(client, config.database, config.table, df_clean)

        if schema is None:
            schema = get_table_schema(client, config.database, config.table)

        _log(print_progress, f"Очищення даних за {year}-{week:02d}...")
        _delete_period(client, config.database, config.table, year, week, schema=schema)

        df_clean = _align_df_to_table(
            client, config.database, config.table, df_clean, schema=schema
        )

        row_count = len(df_clean)
        _log(print_progress, f"Завантаження {row_count} рядків у ClickHouse...")
        client.insert_df(
            table=config.table,
            df=df_clean,
            database=config.database,
        )

        _log(
            print_success,
            f"Дані завантажено у ClickHouse: "
            f"`{config.database}`.`{config.table}` "
            f"({row_count} рядків, тиждень {year}-{week:02d})",
        )
        return row_count

    except Exception as e:
        print_error(f"Помилка при завантаженні у ClickHouse: {e}")
        return 0
    finally:
        if own_client and client is not None:
            try:
                client.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# ClickHouse sink
# ---------------------------------------------------------------------------

class ClickHouseSink(AnalyticsSink):
    """
    Адаптер навколо clickhouse_export логіки (тепер вбудованої у цей модуль).
    Підтримує batch-режим: якщо client передано ззовні — не закриває з'єднання.
    """

    def __init__(self, config: "ClickHouseConfig", client=None):
        self._config = config
        self._client = client          # зовнішній клієнт (batch-режим)
        self._own_client = client is None
        self._schema: dict | None = None

    def setup(self, df: pd.DataFrame) -> None:
        from ..utils import print_progress
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
        _delete_period(
            self._client, self._config.database, self._config.table,
            year, week, schema=self._schema,
        )

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
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
