# DuckDB Analytics Sink Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Додати DuckDB як аналітичне сховище поряд із ClickHouse через абстракцію `AnalyticsSink`.

**Architecture:** Новий файл `olap_tool/sinks.py` містить ABC `AnalyticsSink` та дві реалізації: `ClickHouseSink` (адаптер навколо існуючого `clickhouse_export.py`) і `DuckDBSink` (HTTP REST API через `requests`). `queries.py` отримує `list[AnalyticsSink]` замість `ch_config`. `runner.py` будує список активних sinks із конфігу.

**Tech Stack:** Python 3.8-3.13, requests (вже є в проекті або додаємо), pandas, існуючий `clickhouse_export.py` як-є.

---

### Task 1: Створити `sinks.py` з ABC та перенести `sanitize_df`

**Files:**
- Create: `olap_tool/sinks.py`
- Modify: `olap_tool/clickhouse_export.py` (додати реекспорт)

**Step 1: Створити `olap_tool/sinks.py` з ABC і shared utility**

```python
"""
Analytics Sink абстракція.

Всі аналітичні сховища реалізують AnalyticsSink:
  - ClickHouseSink  — адаптер навколо clickhouse_export.py
  - DuckDBSink      — HTTP REST API (https://analytics.lwhs.xyz)
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    pass


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
```

**Step 2: Оновити `olap_tool/clickhouse_export.py` — замінити локальні визначення на імпорт із sinks**

У `clickhouse_export.py` замінити:
```python
# ВИДАЛИТИ ці дві функції (рядки ~49-76):
def _safe_column_name(name: str) -> str: ...
def sanitize_df(df: pd.DataFrame) -> pd.DataFrame: ...
```
Додати на початку файлу після наявних імпортів:
```python
from .sinks import sanitize_df, _safe_column_name  # shared utilities
```

**Step 3: Перевірити що `import_xlsx_to_clickhouse.py` далі працює**

```bash
python import_xlsx_to_clickhouse.py --dry-run
```
Очікується: список файлів без помилок імпорту.

**Step 4: Commit**

```bash
git add olap_tool/sinks.py olap_tool/clickhouse_export.py
git commit -m "refactor: виносимо sanitize_df/_safe_column_name у sinks.py"
```

---

### Task 2: `ClickHouseSink` адаптер

**Files:**
- Modify: `olap_tool/sinks.py`

**Step 1: Додати `ClickHouseSink` у кінець `sinks.py`**

```python
# ---------------------------------------------------------------------------
# ClickHouse sink
# ---------------------------------------------------------------------------

class ClickHouseSink(AnalyticsSink):
    """
    Адаптер навколо clickhouse_export.py.
    Підтримує batch-режим: якщо client передано ззовні — не закриває з'єднання.
    """

    def __init__(self, config: "ClickHouseConfig", client=None):
        from .config import ClickHouseConfig  # noqa: F401 (type check)
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
        from .clickhouse_export import (
            export_to_clickhouse, get_table_schema,
        )
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
```

**Step 2: Перевірити синтаксис**

```bash
python -c "from olap_tool.sinks import ClickHouseSink; print('OK')"
```
Очікується: `OK`

**Step 3: Commit**

```bash
git add olap_tool/sinks.py
git commit -m "feat: ClickHouseSink адаптер у sinks.py"
```

---

### Task 3: `DuckDBConfig` у `config.py`

**Files:**
- Modify: `olap_tool/config.py`

**Step 1: Додати `DuckDBConfig` dataclass після `ClickHouseConfig` (~рядок 93)**

```python
@dataclass
class DuckDBConfig:
    """Налаштування підключення до DuckDB REST API."""
    enabled: bool = False
    url: str = "https://analytics.lwhs.xyz"
    api_key: str = ""
    table: str = "sales"
    batch_size: int = 1000
```

**Step 2: Додати `duckdb` поле до `AppConfig` (~рядок 112)**

```python
@dataclass
class AppConfig:
    secrets: SecretsConfig = field(default_factory=SecretsConfig)
    query: QueryConfig = field(default_factory=QueryConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    xlsx: XlsxConfig = field(default_factory=XlsxConfig)
    csv: CsvConfig = field(default_factory=CsvConfig)
    excel_header: ExcelHeaderConfig = field(default_factory=ExcelHeaderConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
    duckdb: DuckDBConfig = field(default_factory=DuckDBConfig)   # NEW
```

**Step 3: Додати `load_duckdb_from_env()` після `load_clickhouse_from_env()` (~рядок 164)**

```python
def load_duckdb_from_env() -> DuckDBConfig:
    """Читає налаштування DuckDB REST API з os.environ."""
    try:
        batch_size = int(os.getenv("DUCK_BATCH_SIZE", "1000"))
    except (ValueError, TypeError):
        batch_size = 1000
    return DuckDBConfig(
        enabled=_parse_bool(os.getenv("DUCK_ENABLED", "false"), False),
        url=os.getenv("DUCK_URL", "https://analytics.lwhs.xyz"),
        api_key=os.getenv("DUCK_API_KEY", ""),
        table=os.getenv("DUCK_TABLE", "sales"),
        batch_size=batch_size,
    )
```

**Step 4: Розширити `apply_profile()` секцією `duckdb` (~рядок 241)**

Знайти рядок:
```python
for section in ("query", "export", "xlsx", "csv", "excel_header", "paths", "display", "clickhouse"):
```
Замінити на:
```python
for section in ("query", "export", "xlsx", "csv", "excel_header", "paths", "display", "clickhouse", "duckdb"):
```

**Step 5: Розширити `build_config()` — додати DuckDB env merge (~рядок 327, після блоку `ch_env`)**

```python
    # DuckDB: аналогічно ClickHouse — env задає defaults, profile може перевизначити
    duck_env = load_duckdb_from_env()
    duck_env_dict = {f.name: getattr(duck_env, f.name) for f in dataclass_fields(duck_env)}
    base.setdefault("duckdb", {})
    for k, v in duck_env_dict.items():
        base["duckdb"].setdefault(k, v)
```

**Step 6: Додати `duckdb` до фінального `AppConfig(...)` у `build_config()`**

```python
    return AppConfig(
        secrets=secrets,
        query=_build_section(QueryConfig, base, "query"),
        export=_build_section(ExportConfig, base, "export"),
        xlsx=_build_section(XlsxConfig, base, "xlsx"),
        csv=_build_section(CsvConfig, base, "csv"),
        excel_header=_build_section(ExcelHeaderConfig, base, "excel_header"),
        paths=_build_section(PathsConfig, base, "paths"),
        display=_build_section(DisplayConfig, base, "display"),
        clickhouse=_build_section(ClickHouseConfig, base, "clickhouse"),
        duckdb=_build_section(DuckDBConfig, base, "duckdb"),   # NEW
    )
```

**Step 7: Перевірити синтаксис**

```bash
python -c "from olap_tool.config import build_config; c = build_config(); print(c.duckdb)"
```
Очікується: `DuckDBConfig(enabled=False, url='https://analytics.lwhs.xyz', ...)`

**Step 8: Commit**

```bash
git add olap_tool/config.py
git commit -m "feat: DuckDBConfig dataclass та load_duckdb_from_env у config.py"
```

---

### Task 4: `DuckDBSink` реалізація

**Files:**
- Modify: `olap_tool/sinks.py`

**Step 1: Додати type mapping і HTTP-клієнт у `sinks.py`**

Додати після блоку `ClickHouseSink`:

```python
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
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    # str, datetime, etc. — екрануємо одинарні лапки
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
        from .config import DuckDBConfig  # noqa: F401
        self._config = config
        self._session = self._make_session()
        self._schema: dict[str, str] | None = None   # {col: duck_type}

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
        from .utils import print_progress
        print_progress(f"Перевірка таблиці DuckDB `{self._config.table}`...")
        cols_ddl = ", ".join(
            f'"{col}" {_pandas_dtype_to_duck(df[col].dtype)}'
            for col in df.columns
        )
        self._execute([
            f'CREATE TABLE IF NOT EXISTS "{self._config.table}" ({cols_ddl})'
        ])

        # Зчитуємо реальну схему і додаємо нові колонки (schema evolution)
        self._refresh_schema()
        for col in df.columns:
            if col not in self._schema:
                dtype = _pandas_dtype_to_duck(df[col].dtype)
                try:
                    self._execute([
                        f'ALTER TABLE "{self._config.table}" '
                        f'ADD COLUMN IF NOT EXISTS "{col}" {dtype}'
                    ])
                    self._schema[col] = dtype
                except Exception as e:
                    from .utils import print_warning
                    print_warning(f"Не вдалося додати колонку `{col}`: {e} — пропускаємо")

    def _refresh_schema(self) -> None:
        result = self._query(f'DESCRIBE "{self._config.table}"')
        # result: {"columns": ["column_name", "column_type", ...], "rows": [...]}
        col_idx = result["columns"].index("column_name")
        type_idx = result["columns"].index("column_type")
        self._schema = {row[col_idx]: row[type_idx] for row in result["rows"]}

    def delete_period(self, year: int, week: int) -> None:
        if self._schema is None:
            self._refresh_schema()
        conditions = []
        if "year_num" in self._schema:
            conditions.append(f"year_num = {year}")
        if "week_num" in self._schema:
            conditions.append(f"week_num = {week}")
        if conditions:
            where = " AND ".join(conditions)
            self._execute([f'DELETE FROM "{self._config.table}" WHERE {where}'])

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        from .utils import print_progress, print_success, print_error
        if df is None or len(df) == 0:
            return 0

        # Залишаємо тільки колонки що є в схемі
        if self._schema:
            cols = [c for c in df.columns if c in self._schema]
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
```

**Step 2: Перевірити синтаксис**

```bash
python -c "from olap_tool.sinks import DuckDBSink, ClickHouseSink, AnalyticsSink; print('OK')"
```
Очікується: `OK`

**Step 3: Commit**

```bash
git add olap_tool/sinks.py
git commit -m "feat: DuckDBSink реалізація через REST API"
```

---

### Task 5: Рефакторинг `queries.py` — замінити `ch_config` на `sinks`

**Files:**
- Modify: `olap_tool/queries.py`

**Step 1: Змінити сигнатуру `run_dax_query()`**

Знайти (~рядок 76):
```python
    ch_config: "ClickHouseConfig | None" = None,
```
Замінити на:
```python
    sinks: "list | None" = None,
```

**Step 2: Замінити блок ClickHouse export (~рядок 342-349)**

Знайти:
```python
        # ClickHouse export (якщо enabled або формат CH/CLICKHOUSE)
        if ch_config is not None and (ch_config.enabled or ch_only):
            from .clickhouse_export import export_to_clickhouse
            export_to_clickhouse(df, ch_config, year=year_num, week=week_num)

        if ch_only:
            # Не повертаємо файловий шлях — даних у файлі немає
            return None
```
Замінити на:
```python
        # Analytics sinks (ClickHouse, DuckDB, тощо)
        if sinks:
            for sink in sinks:
                try:
                    sink.setup(df)
                    sink.delete_period(year_num, week_num)
                    sink.insert(df, year=year_num, week=week_num)
                except Exception as e:
                    from .utils import print_error
                    print_error(f"Помилка sink {type(sink).__name__}: {e}")

        if ch_only:
            return None
```

**Step 3: Знайти і виправити де `ch_only` визначається (~рядок 184)**

`ch_only` має розширитися на DuckDB. Знайти:
```python
        ch_only = export_format in ("CH", "CLICKHOUSE")
```
Замінити на:
```python
        ch_only = export_format in ("CH", "CLICKHOUSE", "DUCK", "DUCKDB")
```

**Step 4: Перевірити синтаксис**

```bash
python -c "from olap_tool.queries import run_dax_query; print('OK')"
```
Очікується: `OK`

**Step 5: Commit**

```bash
git add olap_tool/queries.py
git commit -m "refactor: queries.py — ch_config замінено на sinks: list[AnalyticsSink]"
```

---

### Task 6: Рефакторинг `runner.py` — побудова списку sinks

**Files:**
- Modify: `olap_tool/runner.py`

**Step 1: Додати імпорт sinks на початку файлу (після існуючих імпортів)**

```python
from .sinks import ClickHouseSink, DuckDBSink
```

**Step 2: Замінити визначення `ch_only` та побудову sinks (~рядок 235)**

Знайти блок:
```python
        # ClickHouse налаштування
        if config.clickhouse.enabled or config.export.format.upper() in ("CH", "CLICKHOUSE"):
            print(...)
            print(...)
```
Замінити на:
```python
        export_format = config.export.format.upper()
        ch_only = export_format in ("CH", "CLICKHOUSE", "DUCK", "DUCKDB")

        # ClickHouse налаштування
        if config.clickhouse.enabled or export_format in ("CH", "CLICKHOUSE"):
            print(
                f"   {Fore.CYAN}ClickHouse:      {Fore.WHITE}{config.clickhouse.host}:{config.clickhouse.port}"
            )
            print(
                f"   {Fore.CYAN}CH Database:     {Fore.WHITE}{config.clickhouse.database}.{config.clickhouse.table}"
            )

        # DuckDB налаштування
        if config.duckdb.enabled or export_format in ("DUCK", "DUCKDB"):
            print(
                f"   {Fore.CYAN}DuckDB:          {Fore.WHITE}{config.duckdb.url}"
            )
            print(
                f"   {Fore.CYAN}DuckDB Table:    {Fore.WHITE}{config.duckdb.table}"
            )
```

**Step 3: Побудувати список sinks перед циклом по тижнях (~рядок 244, перед `start_time = time.time()`)**

```python
        # Побудова списку активних analytics sinks
        sinks = []
        if config.clickhouse.enabled or export_format in ("CH", "CLICKHOUSE"):
            sinks.append(ClickHouseSink(config.clickhouse))
        if config.duckdb.enabled or export_format in ("DUCK", "DUCKDB"):
            sinks.append(DuckDBSink(config.duckdb))
```

**Step 4: Змінити виклик `run_dax_query()` (~рядок 259)**

Знайти:
```python
            file_path = run_dax_query(
                connection, reporting_period,
                config.query, config.export, config.xlsx,
                config.csv, config.excel_header, config.paths,
                ch_config=config.clickhouse,
            )
```
Замінити на:
```python
            file_path = run_dax_query(
                connection, reporting_period,
                config.query, config.export, config.xlsx,
                config.csv, config.excel_header, config.paths,
                sinks=sinks,
            )
```

**Step 5: Закрити sinks у `finally` блоці (~рядок 326)**

Знайти:
```python
    finally:
        # Bug fix: з'єднання завжди закривається
        if connection:
```
Додати перед `if connection:`:
```python
        for sink in sinks if 'sinks' in locals() else []:
            try:
                sink.close()
            except Exception:
                pass
```

**Step 6: Перевірити синтаксис**

```bash
python -c "from olap_tool.runner import main; print('OK')"
```
Очікується: `OK`

**Step 7: Commit**

```bash
git add olap_tool/runner.py
git commit -m "refactor: runner.py — побудова списку AnalyticsSink, підтримка --format duck"
```

---

### Task 7: Оновити `.env.example`

**Files:**
- Modify: `.env.example`

**Step 1: Додати секцію DuckDB у кінець `.env.example`**

```
# ============================================================
# DuckDB REST API (опційно — для завантаження даних у DuckDB)
# ============================================================

# Увімкнення завантаження у DuckDB паралельно з Excel/CSV
DUCK_ENABLED=false

# URL REST API
DUCK_URL=https://analytics.lwhs.xyz

# API ключ автентифікації (X-API-Key header)
DUCK_API_KEY=

# Таблиця для завантаження
DUCK_TABLE=sales

# Кількість рядків на один INSERT statement (batch size)
DUCK_BATCH_SIZE=1000
```

**Step 2: Commit**

```bash
git add .env.example
git commit -m "docs: додано DUCK_* змінні у .env.example"
```

---

### Task 8: Скрипт `import_xlsx_to_duckdb.py`

**Files:**
- Create: `import_xlsx_to_duckdb.py`

**Step 1: Створити скрипт**

```python
#!/usr/bin/env python
"""
Імпорт існуючих Excel-файлів у DuckDB через REST API (паралельний режим).

Використання:
    python import_xlsx_to_duckdb.py                       # всі файли, 4 воркери
    python import_xlsx_to_duckdb.py --workers 8           # 8 паралельних воркерів
    python import_xlsx_to_duckdb.py --year 2025           # тільки 2025 рік
    python import_xlsx_to_duckdb.py --year 2025 --week 10 # тільки тиждень 10
    python import_xlsx_to_duckdb.py --dry-run             # показати файли без завантаження
"""

import sys
import argparse
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
except Exception:
    pass

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from olap_tool.config import load_duckdb_from_env
from olap_tool.sinks import DuckDBSink, sanitize_df
from olap_tool.utils import init_utils

init_utils(ascii_logs=False)

console = Console()

# ---------------------------------------------------------------------------
# Excel engine: calamine (Rust) з fallback на openpyxl
# ---------------------------------------------------------------------------
try:
    import python_calamine  # noqa: F401
    _EXCEL_ENGINE = "calamine"
except ImportError:
    _EXCEL_ENGINE = "openpyxl"


# ---------------------------------------------------------------------------
# Файловий пошук (аналог import_xlsx_to_clickhouse.py)
# ---------------------------------------------------------------------------

def find_xlsx_files(
    base_dir: Path,
    year: Optional[int],
    week: Optional[int],
) -> list[tuple[Path, int, int]]:
    pattern = re.compile(r"^(\d{4})-(\d{2})\.xlsx$")
    results = []
    for f in sorted(base_dir.rglob("*.xlsx")):
        m = pattern.match(f.name)
        if not m:
            continue
        y, w = int(m.group(1)), int(m.group(2))
        if year is not None and y != year:
            continue
        if week is not None and w != week:
            continue
        results.append((f, y, w))
    return results


def _read_excel(file_path: Path, sheet) -> pd.DataFrame:
    try:
        return pd.read_excel(str(file_path), sheet_name=sheet, engine=_EXCEL_ENGINE)
    except Exception:
        if _EXCEL_ENGINE != "openpyxl":
            return pd.read_excel(str(file_path), sheet_name=sheet, engine="openpyxl")
        raise


# ---------------------------------------------------------------------------
# Worker — використовує спільний DuckDBSink (requests.Session thread-safe)
# ---------------------------------------------------------------------------

def process_file(
    file_path: Path,
    year: int,
    week: int,
    sink: DuckDBSink,
) -> tuple[int, bool, float]:
    """Повертає (row_count, success, elapsed_sec)."""
    t0 = time.monotonic()
    try:
        df = _read_excel(file_path, 0)
    except Exception:
        return 0, False, time.monotonic() - t0

    if df.empty:
        return 0, True, time.monotonic() - t0

    df = sanitize_df(df)
    df["year_num"] = year
    df["week_num"] = week

    try:
        sink.delete_period(year, week)
        rows = sink.insert(df, year=year, week=week)
        return rows, True, time.monotonic() - t0
    except Exception:
        return 0, False, time.monotonic() - t0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Паралельний імпорт Excel файлів OLAP-експорту у DuckDB"
    )
    parser.add_argument("--dir",     default="result", help="Базова директорія")
    parser.add_argument("--year",    type=int, default=None, help="Фільтр за роком")
    parser.add_argument("--week",    type=int, default=None, help="Фільтр за тижнем")
    parser.add_argument("--workers", type=int, default=4,    help="Паралельних воркерів")
    parser.add_argument("--dry-run", action="store_true",    help="Показати файли без завантаження")
    args = parser.parse_args()

    base_dir = Path(args.dir)
    if not base_dir.exists():
        console.print(f"[red]❌ Директорія не знайдена: {base_dir}[/red]")
        return 1

    cfg = load_duckdb_from_env()

    info = Table.grid(padding=(0, 2))
    info.add_column(style="cyan")
    info.add_column(style="white")
    info.add_row("Директорія",  str(base_dir.resolve()))
    info.add_row("DuckDB URL",  cfg.url)
    info.add_row("Таблиця",     cfg.table)
    info.add_row("Excel engine", _EXCEL_ENGINE)
    if args.year:
        info.add_row("Рік", str(args.year))
    if args.week:
        info.add_row("Тиждень", str(args.week))
    if not args.dry_run:
        info.add_row("Воркери", str(args.workers))

    console.print()
    console.print(Panel(
        info,
        title="[bold cyan]ІМПОРТ EXCEL → DUCKDB[/bold cyan]",
        border_style="cyan",
        expand=False,
    ))
    console.print()

    files = find_xlsx_files(base_dir, args.year, args.week)
    if not files:
        console.print("[yellow]⚠️  Файлів не знайдено за вказаними параметрами[/yellow]")
        return 0

    console.print(f"  [cyan]Знайдено файлів:[/cyan] [white bold]{len(files)}[/white bold]\n")

    if args.dry_run:
        for i, (fp, y, w) in enumerate(files, 1):
            console.print(f"  [dim]{i:>4}.[/dim] [white]{fp}[/white]  [yellow]({y}-{w:02d})[/yellow]")
        console.print(f"\n[yellow]DRY RUN завершено. Файлів: {len(files)}[/yellow]")
        return 0

    # Ініціалізація: CREATE TABLE з першого файлу
    with console.status("[cyan]Ініціалізація DuckDB...[/cyan]", spinner="dots"):
        try:
            sink = DuckDBSink(cfg)
            df_init = _read_excel(files[0][0], 0)
            if not df_init.empty:
                df_init = sanitize_df(df_init)
                df_init["year_num"] = files[0][1]
                df_init["week_num"] = files[0][2]
                sink.setup(df_init)
        except Exception as e:
            console.print(f"[red]❌ Помилка ініціалізації: {e}[/red]")
            return 1

    console.print("  [green]✅ Ініціалізовано[/green]\n")

    total = len(files)
    total_rows = 0
    errors = 0
    start_time = time.monotonic()

    progress = Progress(
        SpinnerColumn(),
        BarColumn(bar_width=36),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TextColumn("[dim]•[/dim]"),
        TimeElapsedColumn(),
        TextColumn("[dim]•[/dim] ETA"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )
    task_id = progress.add_task("", total=total)

    with progress:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(process_file, fp, y, w, sink): (fp, y, w)
                for fp, y, w in files
            }
            for future in as_completed(futures):
                fp, y, w = futures[future]
                try:
                    rows, success, elapsed = future.result()
                except Exception as e:
                    rows, success, elapsed = 0, False, 0.0
                    progress.console.print(
                        f"  [red]❌ {y}-{w:02d}[/red]  [dim]{fp.name}[/dim]  [red]{e}[/red]"
                    )

                total_rows += rows
                if not success:
                    errors += 1

                icon = "[green]✅[/green]" if success else "[red]❌[/red]"
                rows_str = f"[white]{rows:>7,}[/white] рядків" if rows > 0 else "[dim]    порожній[/dim]"
                progress.console.print(
                    f"  {icon} [cyan]{y}-{w:02d}[/cyan]  {rows_str}  [dim]{elapsed:.1f}с[/dim]"
                )

                elapsed_total = time.monotonic() - start_time
                rate = (total_rows / elapsed_total) if elapsed_total > 0 else 0
                progress.update(
                    task_id,
                    advance=1,
                    description=(
                        f"[white bold]{total_rows:,}[/white bold] рядків  "
                        f"[dim]{rate:,.0f} рядків/с[/dim]"
                    ),
                )

    sink.close()

    elapsed_total = time.monotonic() - start_time
    rate_files = total / elapsed_total if elapsed_total > 0 else 0
    rate_rows  = total_rows / elapsed_total if elapsed_total > 0 else 0

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="cyan")
    summary.add_column(style="white bold")
    summary.add_row("Файлів оброблено",   f"{total - errors}/{total}")
    summary.add_row("Рядків завантажено", f"{total_rows:,}")
    summary.add_row("Час",                f"{elapsed_total:.1f} с")
    summary.add_row("Швидкість",          f"{rate_files:.1f} файл/с  ·  {rate_rows:,.0f} рядків/с")
    if errors:
        summary.add_row("[red]Помилок[/red]", f"[red]{errors}[/red]")

    border = "green" if not errors else "yellow"
    title  = "[bold green]✅ Імпорт завершено[/bold green]" if not errors else "[bold yellow]⚠️  Завершено з помилками[/bold yellow]"

    console.print()
    console.print(Panel(summary, title=title, border_style=border, expand=False))
    console.print()

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

**Step 2: Перевірити синтаксис**

```bash
python -c "import import_xlsx_to_duckdb; print('OK')"
```
Очікується: `OK`

**Step 3: Тест dry-run**

```bash
python import_xlsx_to_duckdb.py --dry-run
```
Очікується: список файлів або "Файлів не знайдено".

**Step 4: Commit**

```bash
git add import_xlsx_to_duckdb.py
git commit -m "feat: import_xlsx_to_duckdb.py — паралельний batch-імпорт XLSX у DuckDB"
```

---

### Task 9: Фінальна перевірка та README

**Files:**
- Modify: `README.md` (якщо є секція ClickHouse — додати DuckDB поряд)

**Step 1: Smoke test — синтаксис всього пакету**

```bash
python -c "
from olap_tool.sinks import AnalyticsSink, ClickHouseSink, DuckDBSink, sanitize_df
from olap_tool.config import build_config, load_duckdb_from_env
from olap_tool.queries import run_dax_query
from olap_tool.runner import main
print('Всі імпорти OK')
"
```
Очікується: `Всі імпорти OK`

**Step 2: Перевірити що `import_xlsx_to_clickhouse.py` ще працює**

```bash
python import_xlsx_to_clickhouse.py --dry-run
```
Очікується: список файлів без помилок (регресія).

**Step 3: Знайти секцію ClickHouse у README і додати DuckDB**

Додати після ClickHouse-секції в README:
```markdown
### DuckDB (REST API)

Для завантаження у DuckDB додайте у `.env`:
```
DUCK_ENABLED=true
DUCK_URL=https://analytics.lwhs.xyz
DUCK_API_KEY=<your-key>
DUCK_TABLE=sales
```

Або використайте формат:
```bash
python olap.py --format duck
```

Batch-імпорт існуючих XLSX:
```bash
python import_xlsx_to_duckdb.py --year 2025 --workers 4
```

**Step 4: Commit**

```bash
git add README.md
git commit -m "docs: DuckDB інтеграція у README"
```
