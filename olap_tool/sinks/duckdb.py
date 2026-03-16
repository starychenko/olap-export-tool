"""
DuckDB sink — завантаження DataFrame через HTTP REST API.

API:
  POST /execute  {"statements": [...]}   — DDL/DML
  POST /query    {"sql": "..."}          — SELECT (для DESCRIBE)
  POST /upload   multipart/form-data     — Parquet upload (mode=append)
"""
from __future__ import annotations

import datetime
import re
import threading
from typing import TYPE_CHECKING

import pandas as pd

from .base import AnalyticsSink

if TYPE_CHECKING:
    from ..core.config import DuckDBConfig


# ---------------------------------------------------------------------------
# Утиліти для типів та конвертації
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
    cols_to_convert = []
    for col in df.columns:
        if schema.get(col) != "BIGINT":
            continue
        dtype_str = str(df[col].dtype)
        if dtype_str not in ("object", "str", "string"):
            continue
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]
        if isinstance(first, str) and _DT_RE.match(first):
            cols_to_convert.append(col)

    if not cols_to_convert:
        return df

    df = df.copy()
    for col in cols_to_convert:
        df[col] = df[col].apply(
            lambda v: _to_excel_serial(v) if pd.notna(v) else None
        )
    return df


def _numeric_to_str(v):
    """Конвертує числове значення в рядок (float без .0, NaN/inf → None)."""
    if pd.isnull(v):
        return None
    if isinstance(v, float):
        import math
        if not math.isfinite(v):
            return None
        if v == int(v):
            return str(int(v))
    return str(v)


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
            df[col] = df[col].apply(_numeric_to_str)
    return df


# ---------------------------------------------------------------------------
# DuckDB sink
# ---------------------------------------------------------------------------

class DuckDBSink(AnalyticsSink):
    """
    Завантажує DataFrame у DuckDB через REST API.

    API:
      POST /execute  {"statements": [...]}   — DDL/DML
      POST /query    {"sql": "..."}          — SELECT (для DESCRIBE)

    Ідемпотентність: DELETE WHERE year_num=X AND week_num=Y → batch INSERT.
    """

    def __init__(self, config: "DuckDBConfig", *, silent: bool = False):
        self._config = config
        self._session = self._make_session()
        self._schema: dict[str, str] | None = None
        self._schema_lock = threading.Lock()
        self._silent = silent

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
        from ..core.utils import print_progress, print_warning
        if not self._silent:
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
            schema = dict(self._schema)
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
        try:
            col_idx = result["columns"].index("column_name")
            type_idx = result["columns"].index("column_type")
        except (KeyError, ValueError) as exc:
            raise RuntimeError(
                f"Несподіваний формат відповіді DESCRIBE від DuckDB API: {exc}. "
                f"Колонки відповіді: {result.get('columns', '?')}"
            ) from exc
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
            except Exception as exc:
                # Мережева помилка — ретраїмо
                last_exc = exc
                if attempt < _retries - 1:
                    _time.sleep(2 ** attempt)
                continue

            if resp.ok:
                return resp.json().get("total_rows", 0)

            # 4xx (крім 429) — клієнтська помилка, ретрай не допоможе
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                raise Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")

            # 5xx або 429 — серверна помилка, ретраїмо
            last_exc = Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")
            if attempt < _retries - 1:
                _time.sleep(2 ** attempt)

        raise last_exc  # type: ignore[misc]

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        if df is None or len(df) == 0:
            return 0

        # sanitize_df НЕ викликаємо — caller (_flush_to_sinks) вже sanitize зробив.
        # inf/-inf вже замінено на NaN у sanitize_df (base.py).

        with self._schema_lock:
            schema = dict(self._schema) if self._schema else {}
        if schema:
            cols = [c for c in df.columns if c in schema]
            df = df[cols]
            df = _normalize_bigint_date_cols(df, schema)
            df = _align_df_to_schema(df, schema)

        if df.empty:
            return 0

        return self._upload_parquet(df)

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass
