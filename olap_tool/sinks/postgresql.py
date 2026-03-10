"""
PostgreSQL sink — завантаження DataFrame через COPY FROM STDIN.

Ідемпотентність: DELETE WHERE year_num=X AND week_num=Y → COPY FROM STDIN CSV.
SSL: sslmode=require (шифрування без перевірки self-signed сертифікату).

NOT thread-safe: psycopg2-з'єднання не підтримують спільне використання між
потоками. Для batch-скриптів з threading створюйте окремий екземпляр на кожен потік.
"""
from __future__ import annotations

import io
import threading
from typing import TYPE_CHECKING

import pandas as pd

from .base import AnalyticsSink, sanitize_df

if TYPE_CHECKING:
    from ..config import PostgreSQLConfig


# ---------------------------------------------------------------------------
# Утиліти для типів
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


# ---------------------------------------------------------------------------
# PostgreSQL sink
# ---------------------------------------------------------------------------

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
        from ..utils import print_progress, print_warning
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
