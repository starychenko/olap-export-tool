# OLAP Export Tool — TUI + Реструктуризація Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Реорганізувати пакет по підпакетах (`core/`, `connection/`, `data/`, `sinks/`), об'єднати batch-скрипти імпорту в `scripts/import_xlsx.py`, та реалізувати повноцінний Textual TUI як основний інтерфейс.

**Architecture:** Плаский `olap_tool/` розбивається на 4 підпакети. `olap_tool/__init__.py` реекспортує публічні символи для зворотної сумісності. `olap.py` без аргументів запускає Textual TUI; з аргументами — звичайний CLI. TUI виконує операції у Textual Worker з перехопленням stdout через `TUIStream`.

**Tech Stack:** Python 3.8–3.13, Textual ≥ 0.70, colorama, Rich (вже у залежностях через Textual).

---

## Chunk 1: Restructure sinks/ package

### Task 1.1: Створити sinks/base.py

**Files:**
- Create: `olap_tool/sinks/__init__.py`
- Create: `olap_tool/sinks/base.py`

- [ ] **Step 1: Написати smoke-тест (повинен ВПАСТИ)**

```bash
python -c "from olap_tool.sinks.base import AnalyticsSink, sanitize_df; print('OK')"
```
Очікується: `ModuleNotFoundError`

- [ ] **Step 2: Створити `olap_tool/sinks/__init__.py` (порожній)**

```python
# Реекспорти — будуть додані після створення підмодулів
```

- [ ] **Step 3: Створити `olap_tool/sinks/base.py`**

Скопіювати з `olap_tool/sinks.py` наступні блоки:
- всі `import` на початку (numpy, pandas, abc, io, re, threading, datetime)
- функції `_safe_column_name()` та `sanitize_df()`
- клас `AnalyticsSink` (ABC) з усіма abstractmethod

```python
"""
Analytics Sink абстракція та спільні утиліти.
"""
from __future__ import annotations

import datetime
import io
import re
import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd


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


class AnalyticsSink(ABC):
    """ABC для всіх аналітичних сховищ."""

    @abstractmethod
    def setup(self, df: pd.DataFrame) -> None:
        """CREATE TABLE IF NOT EXISTS на основі схеми df."""
        ...

    @abstractmethod
    def delete_period(self, year: int, week: int) -> None:
        """Ідемпотентне видалення рядків для year_num/week_num."""
        ...

    @abstractmethod
    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        """Вставка рядків. Повертає кількість вставлених рядків."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Закриття з'єднання/сесії."""
        ...
```

- [ ] **Step 4: Запустити smoke-тест (повинен ПРОЙТИ)**

```bash
python -c "from olap_tool.sinks.base import AnalyticsSink, sanitize_df; print('OK')"
```
Очікується: `OK`

- [ ] **Step 5: Commit**

```bash
git add olap_tool/sinks/__init__.py olap_tool/sinks/base.py
git commit -m "refactor: створити sinks/base.py з ABC та sanitize_df"
```

---

### Task 1.2: Створити sinks/clickhouse.py

**Files:**
- Create: `olap_tool/sinks/clickhouse.py`

- [ ] **Step 1: Smoke-тест (повинен ВПАСТИ)**

```bash
python -c "from olap_tool.sinks.clickhouse import ClickHouseSink; print('OK')"
```

- [ ] **Step 2: Створити `olap_tool/sinks/clickhouse.py`**

Об'єднати `ClickHouseSink` з `olap_tool/sinks.py` та всю логіку з `olap_tool/clickhouse_export.py`. Файл `clickhouse_export.py` зникає після цього кроку.

```python
"""
ClickHouse sink — поєднує ClickHouseSink та clickhouse_export логіку.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .base import AnalyticsSink, sanitize_df

if TYPE_CHECKING:
    from ..core.config import ClickHouseConfig
```

Далі скопіювати з `clickhouse_export.py`:
- `_pandas_dtype_to_ch()` (маппінг pandas dtype → ClickHouse тип)
- `_ensure_database()`, `_ensure_table()`, `_align_schema()`, `_coerce_df_to_schema()`
- Всю логіку `export_to_clickhouse()`

Далі скопіювати клас `ClickHouseSink` з `sinks.py` (рядки де він визначений), замінивши:
```python
# Старо (sinks.py):
from .clickhouse_export import (
    _pandas_dtype_to_ch,
    _ensure_database,
    _ensure_table,
    _align_schema,
    _coerce_df_to_schema,
    _get_client,
    export_to_clickhouse,
)
# Ново — все в одному файлі, немає зовнішніх імпортів
```

- [ ] **Step 3: Запустити smoke-тест**

```bash
python -c "from olap_tool.sinks.clickhouse import ClickHouseSink; print('OK')"
```
Очікується: `OK`

- [ ] **Step 4: Commit**

```bash
git add olap_tool/sinks/clickhouse.py
git commit -m "refactor: створити sinks/clickhouse.py (поглинає clickhouse_export.py)"
```

---

### Task 1.3: Створити sinks/duckdb.py та sinks/postgresql.py

**Files:**
- Create: `olap_tool/sinks/duckdb.py`
- Create: `olap_tool/sinks/postgresql.py`

- [ ] **Step 1: Smoke-тест (повинен ВПАСТИ)**

```bash
python -c "from olap_tool.sinks.duckdb import DuckDBSink; from olap_tool.sinks.postgresql import PostgreSQLSink; print('OK')"
```

- [ ] **Step 2: Створити `olap_tool/sinks/duckdb.py`**

Скопіювати клас `DuckDBSink` з `olap_tool/sinks.py`. Замінити всі відносні імпорти:

```python
"""DuckDB sink — HTTP REST API."""
from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pandas as pd

from .base import AnalyticsSink, sanitize_df

if TYPE_CHECKING:
    from ..core.config import DuckDBConfig

# Далі повний клас DuckDBSink як є в sinks.py
```

- [ ] **Step 3: Створити `olap_tool/sinks/postgresql.py`**

Скопіювати клас `PostgreSQLSink` з `olap_tool/sinks.py`. Замінити імпорти:

```python
"""PostgreSQL sink — psycopg2 COPY FROM STDIN."""
from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pandas as pd

from .base import AnalyticsSink, sanitize_df

if TYPE_CHECKING:
    from ..core.config import PostgreSQLConfig

# Далі повний клас PostgreSQLSink як є в sinks.py
```

- [ ] **Step 4: Оновити `olap_tool/sinks/__init__.py`**

```python
"""Analytics sinks package."""
from .base import AnalyticsSink, sanitize_df
from .clickhouse import ClickHouseSink
from .duckdb import DuckDBSink
from .postgresql import PostgreSQLSink

__all__ = [
    "AnalyticsSink",
    "sanitize_df",
    "ClickHouseSink",
    "DuckDBSink",
    "PostgreSQLSink",
]
```

- [ ] **Step 5: Запустити smoke-тест**

```bash
python -c "from olap_tool.sinks import AnalyticsSink, sanitize_df, ClickHouseSink, DuckDBSink, PostgreSQLSink; print('OK')"
```
Очікується: `OK`

- [ ] **Step 6: Commit**

```bash
git add olap_tool/sinks/duckdb.py olap_tool/sinks/postgresql.py olap_tool/sinks/__init__.py
git commit -m "refactor: створити sinks/duckdb.py, sinks/postgresql.py, оновити __init__.py"
```

---

### Task 1.4: Видалити старі файли sinks.py та clickhouse_export.py

**Files:**
- Delete: `olap_tool/sinks.py`
- Delete: `olap_tool/clickhouse_export.py`

- [ ] **Step 1: Перевірити що нічого більше не імпортує старі модулі**

```bash
grep -r "from .sinks import\|from olap_tool.sinks import\|from .clickhouse_export\|from olap_tool.clickhouse_export" --include="*.py" /c/git/olap-export-tool/
```
Очікується: знайти тільки `runner.py` та `import_xlsx_to_*.py` (вони будуть оновлені в наступних чанках)

- [ ] **Step 2: Видалити файли**

```bash
git rm olap_tool/sinks.py olap_tool/clickhouse_export.py
```

- [ ] **Step 3: Тимчасово оновити `olap_tool/runner.py` — тільки рядок імпорту sinks**

Знайти рядок:
```python
from .sinks import ClickHouseSink, DuckDBSink, PostgreSQLSink
```
Замінити на:
```python
from .sinks import ClickHouseSink, DuckDBSink, PostgreSQLSink  # буде оновлено в Chunk 2
```
(Поки залишаємо `.sinks` — це тимчасово спрацює поки runner.py ще в olap_tool/)

- [ ] **Step 4: Перевірити що старий CLI ще працює**

```bash
python -c "from olap_tool.runner import main; print('OK')"
```
Очікується: `OK` (або помилка тільки через відсутність .NET, але не ImportError)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: видалити sinks.py та clickhouse_export.py"
```

---

## Chunk 2: Restructure core/, connection/, data/

### Task 2.1: Створити підпакети та перенести файли

**Files:**
- Create: `olap_tool/core/__init__.py`
- Create: `olap_tool/connection/__init__.py`
- Create: `olap_tool/data/__init__.py`
- Move + update: 9 файлів з `olap_tool/` в підпакети

- [ ] **Step 1: Створити порожні `__init__.py` для підпакетів**

```bash
touch olap_tool/core/__init__.py olap_tool/connection/__init__.py olap_tool/data/__init__.py
```

- [ ] **Step 2: Перенести файли до `core/`**

Скопіювати (не видаляти ще) ці файли в `olap_tool/core/`:
- `config.py`, `cli.py`, `runner.py`, `periods.py`, `profiles.py`
- `scheduler.py`, `compression.py`, `progress.py`, `utils.py`

```bash
cp olap_tool/config.py olap_tool/core/config.py
cp olap_tool/cli.py olap_tool/core/cli.py
cp olap_tool/runner.py olap_tool/core/runner.py
cp olap_tool/periods.py olap_tool/core/periods.py
cp olap_tool/profiles.py olap_tool/core/profiles.py
cp olap_tool/scheduler.py olap_tool/core/scheduler.py
cp olap_tool/compression.py olap_tool/core/compression.py
cp olap_tool/progress.py olap_tool/core/progress.py
cp olap_tool/utils.py olap_tool/core/utils.py
```

- [ ] **Step 3: Перенести файли до `connection/`**

```bash
cp olap_tool/connection.py olap_tool/connection/connection.py
cp olap_tool/auth.py olap_tool/connection/auth.py
cp olap_tool/security.py olap_tool/connection/security.py
cp olap_tool/prompt.py olap_tool/connection/prompt.py
```

- [ ] **Step 4: Перенести файли до `data/`**

```bash
cp olap_tool/queries.py olap_tool/data/queries.py
cp olap_tool/exporter.py olap_tool/data/exporter.py
```

- [ ] **Step 5: Commit копій (checkpoint)**

```bash
git add olap_tool/core/ olap_tool/connection/ olap_tool/data/
git commit -m "refactor: копії файлів у підпакети (імпорти ще не оновлені)"
```

---

### Task 2.2: Оновити імпорти в core/

**Files:**
- Modify: `olap_tool/core/runner.py`
- Verify: `olap_tool/core/config.py`, `cli.py`, `utils.py`, `progress.py`, `compression.py`, `periods.py`, `profiles.py`, `scheduler.py`

- [ ] **Step 1: Перевірити які файли core/ потребують змін**

```bash
grep -n "^from \." olap_tool/core/runner.py
```

- [ ] **Step 2: Оновити `olap_tool/core/runner.py` — тільки блок імпортів**

Знайти старі відносні імпорти та замінити:

```python
# СТАРО:
from .connection import connect_to_olap, get_connection_string, AUTH_SSPI
from .queries import get_available_weeks, generate_year_week_pairs, run_dax_query
from .auth import delete_credentials, get_current_windows_user, auth_username
from .sinks import ClickHouseSink, DuckDBSink, PostgreSQLSink

# НОВО:
from ..connection.connection import connect_to_olap, get_connection_string, AUTH_SSPI
from ..data.queries import get_available_weeks, generate_year_week_pairs, run_dax_query
from ..connection.auth import delete_credentials, get_current_windows_user, auth_username
from ..sinks import ClickHouseSink, DuckDBSink, PostgreSQLSink
```

Всі інші імпорти в runner.py (`from .utils`, `from .config`, `from .progress`, `from .cli`, `from . import periods`, `from .compression`, `from .profiles`, `from .scheduler`) залишаються БЕЗ ЗМІН — вони в тому ж пакеті `core/`.

- [ ] **Step 3: Оновити TYPE_CHECKING імпорти в `olap_tool/core/progress.py`**

```python
# progress.py imports from .utils — залишається незмінним (обидва в core/)
```

- [ ] **Step 4: Smoke-тест для core/**

```bash
python -c "from olap_tool.core.config import build_config, AppConfig; print('OK')"
python -c "from olap_tool.core.utils import print_info, print_error; print('OK')"
```
Очікується: `OK`

- [ ] **Step 5: Commit**

```bash
git add olap_tool/core/runner.py
git commit -m "refactor: оновити імпорти в core/runner.py"
```

---

### Task 2.3: Оновити імпорти в connection/

**Files:**
- Modify: `olap_tool/connection/connection.py`
- Modify: `olap_tool/connection/auth.py`
- Modify: `olap_tool/connection/security.py`
- Modify: `olap_tool/connection/prompt.py`

- [ ] **Step 1: Оновити `olap_tool/connection/connection.py`**

```python
# СТАРО:
from .auth import (save_credentials, load_credentials, ...)
from .prompt import prompt_credentials
from .utils import (print_info, print_info_detail, ...)

# НОВО:
from .auth import (save_credentials, load_credentials, ...)     # залишається
from .prompt import prompt_credentials                           # залишається
from ..core.utils import (print_info, print_info_detail, ...)  # ЗМІНИТИ
```

- [ ] **Step 2: Оновити `olap_tool/connection/auth.py`**

```python
# СТАРО:
from .security import (get_machine_id, ...)
from .utils import print_info, print_error

# НОВО:
from .security import (get_machine_id, ...)    # залишається
from ..core.utils import print_info, print_error  # ЗМІНИТИ
```

- [ ] **Step 3: Оновити `olap_tool/connection/security.py`**

```python
# СТАРО:
from .utils import print_info, print_warning, print_error

# НОВО:
from ..core.utils import print_info, print_warning, print_error
```

- [ ] **Step 4: Оновити `olap_tool/connection/prompt.py`**

```python
# СТАРО:
from .utils import ...   # (будь-які utils імпорти)

# НОВО:
from ..core.utils import ...
```

- [ ] **Step 5: Smoke-тест для connection/**

```bash
python -c "from olap_tool.connection.auth import save_credentials, load_credentials; print('OK')"
python -c "from olap_tool.connection.security import get_machine_id; print('OK')"
```
Очікується: `OK`

- [ ] **Step 6: Commit**

```bash
git add olap_tool/connection/
git commit -m "refactor: оновити імпорти в connection/"
```

---

### Task 2.4: Оновити імпорти в data/

**Files:**
- Modify: `olap_tool/data/queries.py`
- Modify: `olap_tool/data/exporter.py`

- [ ] **Step 1: Оновити `olap_tool/data/queries.py`**

```python
# СТАРО:
from .utils import (print_info, print_warning, ...)
from .exporter import export_csv_stream, export_xlsx_dataframe, export_xlsx_stream
from . import progress
# TYPE_CHECKING:
from .config import QueryConfig, ExportConfig, XlsxConfig, CsvConfig, ExcelHeaderConfig, PathsConfig

# НОВО:
from ..core.utils import (print_info, print_warning, ...)
from .exporter import export_csv_stream, export_xlsx_dataframe, export_xlsx_stream  # залишається
from ..core import progress
# TYPE_CHECKING:
from ..core.config import QueryConfig, ExportConfig, XlsxConfig, CsvConfig, ExcelHeaderConfig, PathsConfig
```

Також оновити TYPE_CHECKING імпорт `AnalyticsSink`:
```python
if TYPE_CHECKING:
    from ..sinks.base import AnalyticsSink
```

- [ ] **Step 2: Оновити `olap_tool/data/exporter.py`**

```python
# СТАРО:
from .utils import print_progress, convert_dotnet_to_python
from . import progress
# TYPE_CHECKING:
from .config import ExcelHeaderConfig, XlsxConfig

# НОВО:
from ..core.utils import print_progress, convert_dotnet_to_python
from ..core import progress
# TYPE_CHECKING:
from ..core.config import ExcelHeaderConfig, XlsxConfig
```

- [ ] **Step 3: Smoke-тест для data/**

```bash
python -c "from olap_tool.data.exporter import export_csv_stream; print('OK')"
```
Очікується: `OK` (або помилка тільки від відсутності .NET, але не ImportError)

- [ ] **Step 4: Commit**

```bash
git add olap_tool/data/
git commit -m "refactor: оновити імпорти в data/"
```

---

### Task 2.5: Оновити sinks/ — виправити імпорт config

**Files:**
- Modify: `olap_tool/sinks/clickhouse.py`
- Modify: `olap_tool/sinks/duckdb.py`
- Modify: `olap_tool/sinks/postgresql.py`

- [ ] **Step 1: Оновити TYPE_CHECKING імпорти в кожному sink-файлі**

У кожному файлі замінити:
```python
# СТАРО (якщо є):
from ..config import ClickHouseConfig  # або DuckDBConfig / PostgreSQLConfig

# НОВО:
from ..core.config import ClickHouseConfig  # або відповідний тип
```

- [ ] **Step 2: Smoke-тест всіх sinks з новою структурою**

```bash
python -c "from olap_tool.sinks import AnalyticsSink, ClickHouseSink, DuckDBSink, PostgreSQLSink; print('OK')"
```
Очікується: `OK`

- [ ] **Step 3: Commit**

```bash
git add olap_tool/sinks/
git commit -m "refactor: виправити config-імпорти в sinks/"
```

---

### Task 2.6: Оновити olap_tool/__init__.py та видалити старі файли

**Files:**
- Modify: `olap_tool/__init__.py`
- Delete: старі файли з кореня olap_tool/

- [ ] **Step 1: Оновити `olap_tool/__init__.py`**

```python
"""
OLAP Export Tool package.

Реекспортує публічні символи для зворотної сумісності.
"""
from .core.runner import main
from .sinks import AnalyticsSink, sanitize_df, ClickHouseSink, DuckDBSink, PostgreSQLSink

__all__ = [
    "main",
    "AnalyticsSink",
    "sanitize_df",
    "ClickHouseSink",
    "DuckDBSink",
    "PostgreSQLSink",
]
```

- [ ] **Step 2: Видалити старі файли з кореня olap_tool/**

```bash
git rm olap_tool/config.py olap_tool/cli.py olap_tool/runner.py
git rm olap_tool/periods.py olap_tool/profiles.py olap_tool/scheduler.py
git rm olap_tool/compression.py olap_tool/progress.py olap_tool/utils.py
git rm olap_tool/connection.py olap_tool/auth.py olap_tool/security.py olap_tool/prompt.py
git rm olap_tool/queries.py olap_tool/exporter.py
```

- [ ] **Step 3: Фінальний smoke-тест всієї структури**

```bash
python -c "from olap_tool import main; print('main OK')"
python -c "from olap_tool.core.runner import main; print('core.runner OK')"
python -c "from olap_tool.connection.connection import AUTH_SSPI; print('connection OK')"
python -c "from olap_tool.data.queries import generate_year_week_pairs; print('data OK')"
python -c "from olap_tool.sinks import ClickHouseSink; print('sinks OK')"
```
Очікується: всі `OK`

- [ ] **Step 4: Оновити olap.py (тимчасово — для сумісності до Chunk 4)**

```python
import sys
import os
from dotenv import load_dotenv

load_dotenv()

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

from olap_tool.core.runner import main
sys.exit(main())
```

- [ ] **Step 5: Commit**

```bash
git add olap_tool/__init__.py olap.py
git commit -m "refactor: завершити реструктуризацію olap_tool/, оновити __init__.py та olap.py"
```

---

## Chunk 3: scripts/import_xlsx.py

### Task 3.1: Створити об'єднаний скрипт імпорту

**Files:**
- Create: `scripts/__init__.py` (порожній)
- Create: `scripts/import_xlsx.py`
- Delete: `import_xlsx_to_clickhouse.py`, `import_xlsx_to_duckdb.py`

- [ ] **Step 1: Smoke-тест (повинен ВПАСТИ)**

```bash
python scripts/import_xlsx.py --help
```
Очікується: `No such file or directory`

- [ ] **Step 2: Створити `scripts/__init__.py`**

```python
```

- [ ] **Step 3: Створити `scripts/import_xlsx.py`**

Об'єднання логіки з обох старих скриптів. Спільна частина (file discovery, Excel reading, Rich UI, ThreadPoolExecutor) — одна реалізація:

```python
#!/usr/bin/env python3
"""
Паралельний імпорт XLSX файлів в аналітичне сховище.

Використання:
  python scripts/import_xlsx.py --target ch   --dir result/ --workers 4
  python scripts/import_xlsx.py --target duck --year 2025 --week 10
  python scripts/import_xlsx.py --target pg   --dry-run
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Додаємо корінь проєкту в sys.path щоб імпортувати olap_tool
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

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
)
from rich.table import Table

from olap_tool.core.config import ClickHouseConfig, DuckDBConfig, PostgreSQLConfig
from olap_tool.sinks import ClickHouseSink, DuckDBSink, PostgreSQLSink, AnalyticsSink

console = Console()

# --- Спільні утиліти ---

FILENAME_PATTERN = re.compile(r"^(\d{4})-(\d{2})\.xlsx$")


def find_xlsx_files(
    directory: str,
    year_filter: int | None,
    week_filter: int | None,
) -> list[tuple[Path, int, int]]:
    """Знаходить XLSX файли формату YYYY-WW.xlsx з опційною фільтрацією."""
    result = []
    for path in sorted(Path(directory).rglob("*.xlsx")):
        m = FILENAME_PATTERN.match(path.name)
        if not m:
            continue
        year, week = int(m.group(1)), int(m.group(2))
        if year_filter and year != year_filter:
            continue
        if week_filter and week != week_filter:
            continue
        result.append((path, year, week))
    return result


def read_excel(path: Path, sheet: int = 0) -> pd.DataFrame:
    """Читає XLSX через calamine (швидко) або openpyxl (fallback)."""
    try:
        return pd.read_excel(path, sheet_name=sheet, engine="calamine")
    except Exception:
        return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")


# --- ClickHouse специфіка ---

_ch_local = threading.local()


def _get_ch_client(cfg: ClickHouseConfig):
    """Повертає thread-local ClickHouse клієнт."""
    import clickhouse_connect
    if not hasattr(_ch_local, "client"):
        _ch_local.client = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            username=cfg.user,
            password=cfg.password,
            secure=cfg.secure,
        )
    return _ch_local.client


def _close_ch_clients():
    """Закриває thread-local клієнти після завершення пула."""
    if hasattr(_ch_local, "client"):
        try:
            _ch_local.client.close()
        except Exception:
            pass


# --- Головна логіка ---

def build_sink(target: str) -> AnalyticsSink:
    """Будує sink на основі --target та змінних середовища."""
    if target in ("ch", "clickhouse"):
        cfg = ClickHouseConfig(
            host=os.getenv("CH_HOST", "localhost"),
            port=int(os.getenv("CH_PORT", "8123")),
            database=os.getenv("CH_DATABASE", "default"),
            table=os.getenv("CH_TABLE", "olap_data"),
            user=os.getenv("CH_USER", "default"),
            password=os.getenv("CH_PASSWORD", ""),
            secure=os.getenv("CH_SECURE", "false").lower() == "true",
            enabled=True,
        )
        return ClickHouseSink(cfg)
    elif target in ("duck", "duckdb"):
        cfg = DuckDBConfig(
            url=os.getenv("DUCK_URL", "https://analytics.lwhs.xyz"),
            api_key=os.getenv("DUCK_API_KEY", ""),
            table=os.getenv("DUCK_TABLE", "olap_data"),
            batch_size=int(os.getenv("DUCK_BATCH_SIZE", "10000")),
            enabled=True,
        )
        return DuckDBSink(cfg)
    elif target in ("pg", "postgresql"):
        cfg = PostgreSQLConfig(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432")),
            database=os.getenv("PG_DATABASE", "postgres"),
            schema=os.getenv("PG_SCHEMA", "public"),
            table=os.getenv("PG_TABLE", "olap_data"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", ""),
            sslmode=os.getenv("PG_SSLMODE", "require"),
            enabled=True,
        )
        return PostgreSQLSink(cfg)
    else:
        raise ValueError(f"Невідомий target: {target}")


def process_file(
    args_tuple: tuple[Path, int, int, AnalyticsSink, bool, int],
) -> tuple[str, int]:
    """Обробляє один файл: read → delete → insert."""
    path, year, week, sink, dry_run, sheet = args_tuple
    df = read_excel(path, sheet)
    if dry_run:
        return str(path.name), len(df)
    sink.delete_period(year, week)
    rows = sink.insert(df, year, week)
    return str(path.name), rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Паралельний імпорт XLSX в аналітичне сховище"
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=["ch", "clickhouse", "duck", "duckdb", "pg", "postgresql"],
        help="Ціль: ch/clickhouse, duck/duckdb, pg/postgresql",
    )
    parser.add_argument("--dir", default="result", help="Директорія з XLSX файлами")
    parser.add_argument("--year", type=int, help="Фільтр по року")
    parser.add_argument("--week", type=int, help="Фільтр по тижню")
    parser.add_argument("--sheet", type=int, default=0, help="Індекс листа Excel (0=перший)")
    parser.add_argument("--workers", type=int, default=4, help="Кількість потоків")
    parser.add_argument("--dry-run", action="store_true", help="Тільки читання, без запису")
    args = parser.parse_args()

    console.print(Panel(f"[bold]Імпорт XLSX → {args.target.upper()}[/bold]", expand=False))

    files = find_xlsx_files(args.dir, args.year, args.week)
    if not files:
        console.print("[yellow]Файли не знайдено[/yellow]")
        return

    console.print(f"Знайдено файлів: [bold]{len(files)}[/bold]")

    # Ініціалізація sink (setup на першому файлі)
    sink = build_sink(args.target)
    first_df = read_excel(files[0][0], args.sheet)
    sink.setup(first_df)

    total_rows = 0
    results_table = Table("Файл", "Рядків", title="Результати")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Обробка...", total=len(files))

        work_items = [
            (path, year, week, sink, args.dry_run, args.sheet)
            for path, year, week in files
        ]

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process_file, item): item for item in work_items}
            for future in as_completed(futures):
                try:
                    name, rows = future.result()
                    total_rows += rows
                    results_table.add_row(name, str(rows))
                except Exception as exc:
                    item = futures[future]
                    results_table.add_row(str(item[0].name), f"[red]ПОМИЛКА: {exc}[/red]")
                finally:
                    progress.advance(task)

    sink.close()

    console.print(results_table)
    mode = "[yellow]DRY RUN[/yellow]" if args.dry_run else "[green]записано[/green]"
    console.print(
        Panel(
            f"Файлів: [bold]{len(files)}[/bold]  |  "
            f"Рядків: [bold]{total_rows:,}[/bold]  |  {mode}",
            title="Підсумок",
            expand=False,
        )
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Перевірити що скрипт відкривається без помилок**

```bash
python scripts/import_xlsx.py --help
```
Очікується: вивід usage/help

- [ ] **Step 5: Видалити старі скрипти**

```bash
git rm import_xlsx_to_clickhouse.py import_xlsx_to_duckdb.py
```

- [ ] **Step 6: Commit**

```bash
git add scripts/
git commit -m "feat: scripts/import_xlsx.py — об'єднаний імпорт XLSX (CH/DuckDB/PG)"
```

---

## Chunk 4: TUI implementation

### Task 4.1: Підготовка — залежності та log handler

**Files:**
- Modify: `requirements.txt`
- Modify: `olap_tool/core/utils.py`

- [ ] **Step 1: Додати textual до `requirements.txt`**

Додати після рядка з `rich`:
```
textual>=0.70.0
```

- [ ] **Step 2: Встановити textual**

```bash
pip install "textual>=0.70.0"
```

- [ ] **Step 3: Додати TUIStream та stdout-redirect в `olap_tool/core/utils.py`**

Додати в кінець файлу:

```python
# ---------------------------------------------------------------------------
# TUI stdout redirect
# ---------------------------------------------------------------------------
import re as _re

_ANSI_ESCAPE = _re.compile(r"\x1b\[[0-9;]*m")


class TUIStream:
    """
    Замінює sys.stdout під час роботи TUI.
    Перехоплює всі print() виклики та пише чистий текст у Textual RichLog.
    Потокобезпечний через call_from_thread.
    """

    def __init__(self, app, log_widget):
        self._app = app
        self._log = log_widget
        self._buf = ""

    def write(self, text: str) -> None:
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            clean = _ANSI_ESCAPE.sub("", line)
            if clean:
                self._app.call_from_thread(self._log.write, clean)

    def flush(self) -> None:
        pass

    def fileno(self):
        import io as _io
        raise _io.UnsupportedOperation("no fileno")
```

- [ ] **Step 4: Smoke-тест**

```bash
python -c "from olap_tool.core.utils import TUIStream; print('OK')"
```
Очікується: `OK`

- [ ] **Step 5: Commit**

```bash
git add requirements.txt olap_tool/core/utils.py
git commit -m "feat: додати textual до залежностей, TUIStream у utils.py"
```

---

### Task 4.2: TUI package — app.py та main_menu.py

**Files:**
- Create: `olap_tool/tui/__init__.py`
- Create: `olap_tool/tui/screens/__init__.py`
- Create: `olap_tool/tui/widgets/__init__.py`
- Create: `olap_tool/tui/app.py`
- Create: `olap_tool/tui/screens/main_menu.py`

- [ ] **Step 1: Smoke-тест (повинен ВПАСТИ)**

```bash
python -c "from olap_tool.tui.app import OlapApp; print('OK')"
```

- [ ] **Step 2: Створити порожні `__init__.py`**

```bash
touch olap_tool/tui/__init__.py olap_tool/tui/screens/__init__.py olap_tool/tui/widgets/__init__.py
```

- [ ] **Step 3: Створити `olap_tool/tui/screens/main_menu.py`**

```python
"""Головний екран меню."""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Footer, Header, ListItem, ListView, Label


MENU_ITEMS = [
    ("export", "Експорт з OLAP куба"),
    ("import", "Імпорт XLSX в аналітику"),
    ("quit", "Вийти"),
]


class MainMenuScreen(Screen):
    """Головне меню програми."""

    BINDINGS = [("q", "quit", "Вийти")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield ListView(
            *[ListItem(Label(label), id=item_id) for item_id, label in MENU_ITEMS],
            id="main-menu",
        )
        yield Footer()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        item_id = event.item.id
        if item_id == "export":
            from .olap_export import OlapExportScreen
            self.app.push_screen(OlapExportScreen())
        elif item_id == "import":
            from .xlsx_import import XlsxImportScreen
            self.app.push_screen(XlsxImportScreen())
        elif item_id == "quit":
            self.app.exit()

    def action_quit(self) -> None:
        self.app.exit()
```

- [ ] **Step 4: Створити `olap_tool/tui/app.py`**

```python
"""Головний Textual застосунок."""
from textual.app import App

from .screens.main_menu import MainMenuScreen

CSS = """
Screen {
    background: $surface;
}

ListView {
    width: 60;
    margin: 2 4;
    border: solid $primary;
}

ListItem {
    padding: 1 2;
}

ListItem:hover {
    background: $primary 20%;
}

ListItem.--highlight {
    background: $primary;
    color: $text;
}

#log-panel {
    height: 1fr;
    border: solid $accent;
    margin: 1;
}

.form-container {
    width: 1fr;
    height: auto;
    border: solid $primary;
    margin: 1;
    padding: 1;
}

Label.field-label {
    margin-top: 1;
    color: $text-muted;
}

Button {
    margin: 1 0;
}
"""


class OlapApp(App):
    """OLAP Export Tool — головний застосунок."""

    TITLE = "OLAP Export Tool"
    SUB_TITLE = "v2.0"
    CSS = CSS
    BINDINGS = [("q", "quit", "Вийти")]

    def on_mount(self) -> None:
        self.push_screen(MainMenuScreen())
```

- [ ] **Step 5: Smoke-тест**

```bash
python -c "from olap_tool.tui.app import OlapApp; print('OK')"
```
Очікується: `OK`

- [ ] **Step 6: Commit**

```bash
git add olap_tool/tui/
git commit -m "feat: TUI app.py та main_menu.py"
```

---

### Task 4.3: TUI — екран OLAP Export

**Files:**
- Create: `olap_tool/tui/screens/olap_export.py`

- [ ] **Step 1: Smoke-тест (повинен ВПАСТИ)**

```bash
python -c "from olap_tool.tui.screens.olap_export import OlapExportScreen; print('OK')"
```

- [ ] **Step 2: Створити `olap_tool/tui/screens/olap_export.py`**

```python
"""Екран експорту даних з OLAP куба."""
from __future__ import annotations

import sys
from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    Footer,
    Header,
    Input,
    Label,
    RichLog,
    Select,
    SelectionList,
)
from textual.worker import Worker, get_current_worker

from ...core.utils import TUIStream


def _list_profiles() -> list[str]:
    """Повертає список доступних профілів."""
    profiles_dir = Path("profiles")
    if not profiles_dir.exists():
        return []
    return [p.stem for p in sorted(profiles_dir.glob("*.yaml"))]


FORMAT_OPTIONS = [
    ("xlsx", "XLSX"),
    ("csv", "CSV"),
    ("both", "XLSX + CSV"),
    ("ch", "ClickHouse"),
    ("duck", "DuckDB"),
    ("pg", "PostgreSQL"),
]

PERIOD_OPTIONS = [
    ("last-weeks", "Останні N тижнів"),
    ("current-month", "Поточний місяць"),
    ("last-month", "Попередній місяць"),
    ("current-quarter", "Поточний квартал"),
    ("last-quarter", "Попередній квартал"),
    ("year-to-date", "З початку року"),
    ("manual", "Ручний діапазон"),
]

COMPRESS_OPTIONS = [
    ("none", "Без стиснення"),
    ("zip", "ZIP архів"),
]


class OlapExportScreen(Screen):
    """Екран: Експорт з OLAP куба."""

    BINDINGS = [("escape", "pop_screen", "Назад")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(classes="form-container", id="export-form"):
                yield Label("Профіль:", classes="field-label")
                profiles = _list_profiles()
                yield Select(
                    [(p, p) for p in profiles] or [("(немає профілів)", "")],
                    id="profile-select",
                    allow_blank=True,
                    prompt="(без профілю)",
                )

                yield Label("Формат:", classes="field-label")
                yield Select(FORMAT_OPTIONS, id="format-select", value="xlsx")

                yield Label("Період:", classes="field-label")
                yield Select(PERIOD_OPTIONS, id="period-type-select", value="last-weeks")

                yield Label("Значення (N тижнів або YYYY-WW:YYYY-WW):", classes="field-label")
                yield Input(placeholder="4", id="period-value-input", value="4")

                yield Label("Стиснення:", classes="field-label")
                yield Select(COMPRESS_OPTIONS, id="compress-select", value="none")

                yield Button("Запустити", variant="primary", id="run-btn")
                yield Button("Скасувати", variant="error", id="cancel-btn", disabled=True)

            with Vertical(id="log-panel"):
                yield RichLog(id="export-log", highlight=True, markup=True, wrap=True)
        yield Footer()

    def _build_argv(self) -> list[str]:
        """Будує список аргументів CLI з форми."""
        argv = ["olap.py"]

        profile = self.query_one("#profile-select", Select).value
        if profile and profile != Select.BLANK:
            argv += ["--profile", str(profile)]

        fmt = self.query_one("#format-select", Select).value
        if fmt:
            argv += ["--format", str(fmt)]

        period_type = self.query_one("#period-type-select", Select).value
        period_value = self.query_one("#period-value-input", Input).value.strip()

        if period_type == "last-weeks":
            argv += ["--last-weeks", period_value or "4"]
        elif period_type == "current-month":
            argv.append("--current-month")
        elif period_type == "last-month":
            argv.append("--last-month")
        elif period_type == "current-quarter":
            argv.append("--current-quarter")
        elif period_type == "last-quarter":
            argv.append("--last-quarter")
        elif period_type == "year-to-date":
            argv.append("--year-to-date")
        elif period_type == "manual" and period_value:
            argv += ["--period", period_value]

        compress = self.query_one("#compress-select", Select).value
        if compress and compress != "none":
            argv += ["--compress", str(compress)]

        return argv

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run-btn":
            self._start_export()
        elif event.button.id == "cancel-btn":
            self._cancel_export()

    def _start_export(self) -> None:
        log = self.query_one("#export-log", RichLog)
        log.clear()
        argv = self._build_argv()
        log.write(f"[dim]Команда: {' '.join(argv)}[/dim]")

        self.query_one("#run-btn", Button).disabled = True
        self.query_one("#cancel-btn", Button).disabled = False

        # run_worker з корутиною — виконується в asyncio event loop
        # runner_main() блокує потік, тому _do_export запускає його в executor
        self._worker = self.run_worker(
            self._do_export(argv), exclusive=True, name="olap-export"
        )
        # Примітка: _do_export використовує run_in_executor для блокуючого виклику

    def _cancel_export(self) -> None:
        if hasattr(self, "_worker") and self._worker.state.is_running:
            self._worker.cancel()

    async def _do_export(self, argv: list[str]) -> None:
        """Виконує експорт у окремому потоці (executor) з перехопленням stdout."""
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._run_export_sync, argv)

    def _run_export_sync(self, argv: list[str]) -> None:
        """Синхронний блок — виконується в executor потоці."""
        from ...core.runner import main as runner_main
        log = self.query_one("#export-log", RichLog)
        stream = TUIStream(self.app, log)
        old_stdout = sys.stdout
        sys.stdout = stream
        old_argv = sys.argv
        sys.argv = argv
        try:
            result = runner_main()
            msg = (
                "[bold green]✓ Завершено успішно[/bold green]"
                if result == 0
                else f"[bold red]✗ Завершено з кодом {result}[/bold red]"
            )
            self.app.call_from_thread(log.write, msg)
        except Exception as exc:
            self.app.call_from_thread(log.write, f"[bold red]✗ Помилка: {exc}[/bold red]")
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            self.app.call_from_thread(self._on_export_done)

    def _on_export_done(self) -> None:
        self.query_one("#run-btn", Button).disabled = False
        self.query_one("#cancel-btn", Button).disabled = True
```

- [ ] **Step 3: Smoke-тест**

```bash
python -c "from olap_tool.tui.screens.olap_export import OlapExportScreen; print('OK')"
```
Очікується: `OK`

- [ ] **Step 4: Commit**

```bash
git add olap_tool/tui/screens/olap_export.py
git commit -m "feat: TUI екран OlapExportScreen"
```

---

### Task 4.4: TUI — екран XLSX Import

**Files:**
- Create: `olap_tool/tui/screens/xlsx_import.py`

- [ ] **Step 1: Smoke-тест (повинен ВПАСТИ)**

```bash
python -c "from olap_tool.tui.screens.xlsx_import import XlsxImportScreen; print('OK')"
```

- [ ] **Step 2: Створити `olap_tool/tui/screens/xlsx_import.py`**

```python
"""Екран імпорту XLSX файлів в аналітичне сховище."""
from __future__ import annotations

import sys
from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    Footer,
    Header,
    Input,
    Label,
    RadioButton,
    RadioSet,
    RichLog,
)

from ...core.utils import TUIStream


class XlsxImportScreen(Screen):
    """Екран: Імпорт XLSX в аналітику."""

    BINDINGS = [("escape", "pop_screen", "Назад")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(classes="form-container", id="import-form"):
                yield Label("Ціль:", classes="field-label")
                with RadioSet(id="target-radio"):
                    yield RadioButton("ClickHouse", id="target-ch", value=True)
                    yield RadioButton("DuckDB", id="target-duck")
                    yield RadioButton("PostgreSQL", id="target-pg")

                yield Label("Директорія з XLSX:", classes="field-label")
                yield Input(placeholder="result/", id="dir-input", value="result/")

                yield Label("Рік (опційно):", classes="field-label")
                yield Input(placeholder="2025", id="year-input")

                yield Label("Тиждень (опційно):", classes="field-label")
                yield Input(placeholder="10", id="week-input")

                yield Label("Workers:", classes="field-label")
                yield Input(placeholder="4", id="workers-input", value="4")

                yield Checkbox("Dry Run (без запису)", id="dry-run-check")

                yield Button("Запустити", variant="primary", id="run-btn")
                yield Button("Скасувати", variant="error", id="cancel-btn", disabled=True)

            with Vertical(id="log-panel"):
                yield RichLog(id="import-log", highlight=True, markup=True, wrap=True)
        yield Footer()

    def _get_target(self) -> str:
        radio = self.query_one("#target-radio", RadioSet)
        pressed = radio.pressed_button
        if pressed and pressed.id:
            return pressed.id.replace("target-", "")
        return "ch"

    def _build_script_args(self) -> list[str]:
        """Будує список аргументів для scripts/import_xlsx.py."""
        target = self._get_target()
        directory = self.query_one("#dir-input", Input).value.strip() or "result/"
        year = self.query_one("#year-input", Input).value.strip()
        week = self.query_one("#week-input", Input).value.strip()
        workers = self.query_one("#workers-input", Input).value.strip() or "4"
        dry_run = self.query_one("#dry-run-check", Checkbox).value

        args = ["scripts/import_xlsx.py", "--target", target, "--dir", directory, "--workers", workers]
        if year:
            args += ["--year", year]
        if week:
            args += ["--week", week]
        if dry_run:
            args.append("--dry-run")
        return args

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run-btn":
            self._start_import()
        elif event.button.id == "cancel-btn":
            if hasattr(self, "_worker"):
                self._worker.cancel()

    def _start_import(self) -> None:
        log = self.query_one("#import-log", RichLog)
        log.clear()
        script_args = self._build_script_args()
        log.write(f"[dim]Команда: python {' '.join(script_args)}[/dim]")

        self.query_one("#run-btn", Button).disabled = True
        self.query_one("#cancel-btn", Button).disabled = False

        self._worker = self.run_worker(
            self._do_import(script_args), exclusive=True, name="xlsx-import"
        )

    async def _do_import(self, script_args: list[str]) -> None:
        """Виконує імпорт через scripts/import_xlsx.main()."""
        log = self.query_one("#import-log", RichLog)
        stream = TUIStream(self.app, log)
        old_stdout = sys.stdout
        sys.stdout = stream
        try:
            # Запускаємо scripts/import_xlsx.main() з підміненим sys.argv
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "import_xlsx", Path("scripts/import_xlsx.py")
            )
            mod = importlib.util.module_from_spec(spec)
            old_argv = sys.argv
            sys.argv = script_args
            try:
                spec.loader.exec_module(mod)
                mod.main()
                self.app.call_from_thread(
                    log.write, "[bold green]✓ Імпорт завершено[/bold green]"
                )
            finally:
                sys.argv = old_argv
        except SystemExit:
            pass
        except Exception as exc:
            self.app.call_from_thread(
                log.write, f"[bold red]✗ Помилка: {exc}[/bold red]"
            )
        finally:
            sys.stdout = old_stdout
            self.app.call_from_thread(self._on_done)

    def _on_done(self) -> None:
        self.query_one("#run-btn", Button).disabled = False
        self.query_one("#cancel-btn", Button).disabled = True
```

- [ ] **Step 3: Smoke-тест**

```bash
python -c "from olap_tool.tui.screens.xlsx_import import XlsxImportScreen; print('OK')"
```
Очікується: `OK`

- [ ] **Step 4: Commit**

```bash
git add olap_tool/tui/screens/xlsx_import.py
git commit -m "feat: TUI екран XlsxImportScreen"
```

---

### Task 4.5: Фінал — оновити olap.py

**Files:**
- Modify: `olap.py`

- [ ] **Step 1: Оновити `olap.py`**

```python
#!/usr/bin/env python3
"""
OLAP Export Tool — точка входу.

Без аргументів → запускає Textual TUI.
З аргументами → CLI режим (сумісний з попередньою поведінкою).
"""
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# UTF-8 консоль на Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

if len(sys.argv) == 1:
    from olap_tool.tui.app import OlapApp
    OlapApp().run()
else:
    from olap_tool.core.runner import main
    sys.exit(main())
```

- [ ] **Step 2: Перевірити що CLI режим ще запускається**

```bash
python olap.py --help
```
Очікується: виведення help без помилок

- [ ] **Step 3: Перевірити що TUI запускається (Ctrl+C для виходу)**

```bash
python olap.py
```
Очікується: відкривається Textual вікно з головним меню

- [ ] **Step 4: Оновити `CLAUDE.md` — розділ про структуру**

Оновити секцію Architecture → Key files:
```
- `olap_tool/core/` — config, cli, runner, utils, progress, periods, profiles, scheduler, compression
- `olap_tool/connection/` — connection, auth, security, prompt
- `olap_tool/data/` — queries, exporter
- `olap_tool/sinks/` — base (ABC), clickhouse, duckdb, postgresql
- `olap_tool/tui/` — Textual TUI (app, screens, widgets)
- `scripts/import_xlsx.py` — об'єднаний batch-імпорт
```

- [ ] **Step 5: Фінальний повний smoke-тест**

```bash
python -c "
from olap_tool import main
from olap_tool.core.runner import main as core_main
from olap_tool.core.config import AppConfig
from olap_tool.core.utils import TUIStream
from olap_tool.connection.connection import AUTH_SSPI
from olap_tool.data.queries import generate_year_week_pairs
from olap_tool.sinks import AnalyticsSink, ClickHouseSink, DuckDBSink, PostgreSQLSink
from olap_tool.tui.app import OlapApp
from olap_tool.tui.screens.main_menu import MainMenuScreen
from olap_tool.tui.screens.olap_export import OlapExportScreen
from olap_tool.tui.screens.xlsx_import import XlsxImportScreen
print('ALL OK')
"
```
Очікується: `ALL OK`

- [ ] **Step 6: Commit**

```bash
git add olap.py CLAUDE.md
git commit -m "feat: TUI детектування в olap.py — без аргументів запускає Textual"
```

---

## Підсумок змін

| Старий шлях | Новий шлях |
|------------|-----------|
| `olap_tool/sinks.py` | `olap_tool/sinks/base.py` + `clickhouse.py` + `duckdb.py` + `postgresql.py` |
| `olap_tool/clickhouse_export.py` | поглинуто в `olap_tool/sinks/clickhouse.py` |
| `olap_tool/config.py` | `olap_tool/core/config.py` |
| `olap_tool/runner.py` | `olap_tool/core/runner.py` |
| `olap_tool/utils.py` | `olap_tool/core/utils.py` |
| `olap_tool/cli.py` | `olap_tool/core/cli.py` |
| `olap_tool/connection.py` | `olap_tool/connection/connection.py` |
| `olap_tool/auth.py` | `olap_tool/connection/auth.py` |
| `olap_tool/queries.py` | `olap_tool/data/queries.py` |
| `olap_tool/exporter.py` | `olap_tool/data/exporter.py` |
| `import_xlsx_to_clickhouse.py` + `import_xlsx_to_duckdb.py` | `scripts/import_xlsx.py` |
| *(новий)* | `olap_tool/tui/app.py` + `screens/` + `widgets/` |
