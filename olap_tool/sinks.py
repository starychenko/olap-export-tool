"""
Analytics Sink абстракція.

Всі аналітичні сховища реалізують AnalyticsSink:
  - ClickHouseSink  — адаптер навколо clickhouse_export.py
  - DuckDBSink      — HTTP REST API (https://analytics.lwhs.xyz)
"""
from __future__ import annotations

import datetime
import io
import re
import threading
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import ClickHouseConfig
    from .config import DuckDBConfig
    from .config import PostgreSQLConfig


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



_EXCEL_EPOCH = datetime.date(1899, 12, 30)
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?$")


def _to_excel_serial(v) -> int | None:
    """Конвертує datetime/date/datetime-рядок в Excel serial number (int)."""
    if isinstance(v, datetime.datetime):
        return (v.date() - _EXCEL_EPOCH).days
    if isinstance(v, datetime.date):
        return (v - _EXCEL_EPOCH).days
    if isinstance(v, str) and _DT_RE.match(v):
        try:
            dt = datetime.datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S")
            return (dt.date() - _EXCEL_EPOCH).days
        except ValueError:
            return None
    return None


def _normalize_bigint_date_cols(df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
    """Конвертує рядкові datetime-колонки у BIGINT-схемі до Excel serial number."""
    for col in df.columns:
        if schema.get(col) != "BIGINT":
            continue
        # Перевіряємо рядковий dtype (object або pd.StringDtype з calamine)
        dtype_str = str(df[col].dtype)
        if dtype_str not in ("object", "str", "string"):
            continue
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]
        if not isinstance(first, str) or not _DT_RE.match(first):
            continue
        df = df.copy()
        df[col] = df[col].apply(
            lambda v: _to_excel_serial(v) if pd.notna(v) else None
        )
    return df


def _align_df_to_schema(df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
    """Приводить типи DataFrame до відповідності схеми DuckDB перед Parquet-upload.

    - VARCHAR у схемі → конвертує числові колонки до рядка
    - BIGINT у схемі + рядкові datetime → вже оброблено _normalize_bigint_date_cols
    """
    df = df.copy()
    for col in df.columns:
        duck_type = schema.get(col, "VARCHAR")
        dtype_str = str(df[col].dtype)
        if duck_type == "VARCHAR" and dtype_str not in ("object", "str", "string"):
            # int64/float64 → str (напр. articul: 31262066 → '31262066')
            def _to_str(v):
                if pd.isnull(v):
                    return None
                if isinstance(v, float) and v == int(v):
                    return str(int(v))
                return str(v)
            df[col] = df[col].apply(_to_str)
    return df


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
        })
        return s

    def _execute(self, statements: list[str]) -> dict:
        resp = self._session.post(
            f"{self._config.url}/execute",
            json={"statements": statements},
            timeout=600,
        )
        if not resp.ok:
            raise Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")
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
            else:
                # Якщо схема BIGINT, але в DataFrame є нечислові рядки → змінюємо на VARCHAR
                if schema.get(col) == "BIGINT":
                    dtype_str = str(df[col].dtype)
                    if dtype_str in ("object", "str", "string"):
                        sample = df[col].dropna()
                        if not sample.empty and isinstance(sample.iloc[0], str) and not _DT_RE.match(str(sample.iloc[0])):
                            try:
                                self._execute([
                                    f'ALTER TABLE "{self._config.table}" '
                                    f'ALTER COLUMN "{col}" TYPE VARCHAR'
                                ])
                                with self._schema_lock:
                                    if self._schema is not None:
                                        self._schema[col] = "VARCHAR"
                                print_warning(f"Колонку `{col}` змінено BIGINT → VARCHAR (нечислові значення)")
                            except Exception as e:
                                print_warning(f"Не вдалося змінити тип `{col}`: {e}")

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

    def _upload_parquet(self, df: pd.DataFrame, _retries: int = 3) -> int:
        """Завантажує DataFrame у DuckDB через /upload (Parquet, mode=append)."""
        import io
        import time as _time
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        parquet_bytes = buf.getvalue()

        last_exc: Exception | None = None
        for attempt in range(_retries):
            try:
                resp = self._session.post(
                    f"{self._config.url}/upload",
                    files={"file": ("data.parquet", parquet_bytes, "application/octet-stream")},
                    data={"table": self._config.table, "mode": "append"},
                    timeout=120,
                )
                if not resp.ok:
                    err = Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")
                    # 4xx (крім 429 Too Many Requests) — одразу piднімаємо, без retry
                    if 400 <= resp.status_code < 500 and resp.status_code != 429:
                        raise err
                    last_exc = err
                else:
                    return resp.json().get("total_rows", 0)
            except Exception as exc:
                last_exc = exc
            if attempt < _retries - 1:
                _time.sleep(2 ** attempt)
        raise last_exc  # type: ignore[misc]

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        if df is None or len(df) == 0:
            return 0

        df = sanitize_df(df)  # замінює inf/-inf → NaN перед серіалізацією

        with self._schema_lock:
            schema = dict(self._schema) if self._schema else {}
        if schema:
            cols = [c for c in df.columns if c in schema]
            df = df[cols]
            df = _normalize_bigint_date_cols(df, schema)
            df = _align_df_to_schema(df, schema)

        if df.empty:
            return 0

        self._upload_parquet(df)
        return len(df)

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# PostgreSQL sink (psycopg2 + COPY FROM STDIN)
# ---------------------------------------------------------------------------

def _pandas_dtype_to_pg(dtype) -> str:
    """Конвертує pandas dtype у PostgreSQL SQL тип."""
    dtype_str = str(dtype)
    if dtype_str.startswith("int") or dtype_str.startswith("uint"):
        return "BIGINT"
    if dtype_str.startswith("float"):
        return "DOUBLE PRECISION"
    if dtype_str in ("bool", "boolean"):
        return "BOOLEAN"
    if dtype_str.startswith("datetime"):
        return "TIMESTAMP"
    if dtype_str.startswith("date"):
        return "DATE"
    return "TEXT"

class PostgreSQLSink(AnalyticsSink):
    """
    Завантажує DataFrame у PostgreSQL через COPY FROM STDIN.

    Ідемпотентність: DELETE WHERE year_num=X AND week_num=Y → COPY FROM STDIN CSV.
    SSL: sslmode=require (шифрування без перевірки self-signed сертифікату).

    NOT thread-safe: psycopg2-з'єднання не підтримують спільне використання між
    потоками. Для batch-скриптів з threading створюйте окремий екземпляр на кожен потік.
    """

    def __init__(self, config: "PostgreSQLConfig"):
        self._config = config
        self._conn = None
        self._schema: dict[str, str] | None = None
        self._schema_lock = threading.Lock()

    def _get_conn(self):
        """Повертає активне з'єднання, створює нове якщо потрібно."""
        import psycopg2
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self._config.host,
                port=self._config.port,
                dbname=self._config.database,
                user=self._config.user,
                password=self._config.password,
                sslmode=self._config.ssl_mode,
            )
            self._conn.autocommit = False
        return self._conn

    def _full_table(self) -> str:
        """Повертає повну назву таблиці з схемою: "schema"."table"."""
        return f'"{self._config.schema}"."{self._config.table}"'

    def _refresh_schema(self) -> None:
        """Читає поточну схему таблиці з information_schema."""
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (self._config.schema, self._config.table),
            )
            rows = cur.fetchall()
        with self._schema_lock:
            self._schema = {row[0]: row[1] for row in rows}

    def setup(self, df: pd.DataFrame) -> None:
        from .utils import print_progress, print_warning
        print_progress(
            f"Перевірка таблиці PostgreSQL {self._full_table()} "
            f"({self._config.host}:{self._config.port})..."
        )
        conn = self._get_conn()
        cols_ddl = ", ".join(
            f'"{col}" {_pandas_dtype_to_pg(df[col].dtype)}'
            for col in df.columns
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {self._full_table()} ({cols_ddl})"
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        self._refresh_schema()
        with self._schema_lock:
            schema = dict(self._schema) if self._schema is not None else {}

        # Додаємо нові колонки яких немає в таблиці
        for col in df.columns:
            if col not in schema:
                dtype = _pandas_dtype_to_pg(df[col].dtype)
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'ALTER TABLE {self._full_table()} '
                            f'ADD COLUMN IF NOT EXISTS "{col}" {dtype}'
                        )
                    conn.commit()
                    with self._schema_lock:
                        if self._schema is not None:
                            self._schema[col] = dtype
                except Exception as e:
                    conn.rollback()
                    print_warning(f"Не вдалося додати колонку `{col}`: {e} — пропускаємо")

    def delete_period(self, year: int, week: int) -> None:
        if self._schema is None:
            self._refresh_schema()
        with self._schema_lock:
            schema = dict(self._schema) if self._schema is not None else {}
        if "year_num" not in schema or "week_num" not in schema:
            return
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self._full_table()} "
                    f"WHERE year_num = %s AND week_num = %s",
                    (year, week),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        if df is None or len(df) == 0:
            return 0

        # Фільтруємо до колонок що є в таблиці
        with self._schema_lock:
            schema = dict(self._schema) if self._schema else {}
        if schema:
            cols = [c for c in df.columns if c in schema]
            df = df[cols]

        if df.empty:
            return 0

        # DataFrame → CSV у пам'яті; \N як sentinel для NULL
        # (порожній рядок '' зберігається як '', а не як NULL)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)

        col_list = ", ".join(f'"{c}"' for c in df.columns)
        copy_sql = (
            f"COPY {self._full_table()} ({col_list}) "
            r"FROM STDIN WITH (FORMAT CSV, NULL '\N')"
        )

        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.copy_expert(copy_sql, buf)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return len(df)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
