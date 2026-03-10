# PostgreSQL Sink Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Додати `PostgreSQLSink` як третій аналітичний sink для завантаження даних з OLAP у PostgreSQL через `COPY FROM STDIN`.

**Architecture:** Новий `PostgreSQLConfig` dataclass у `config.py` — за тим самим патерном що і `ClickHouseConfig`/`DuckDBConfig`. `PostgreSQLSink` у `sinks.py` реалізує `AnalyticsSink` ABC з bulk-завантаженням через `psycopg2.copy_expert`. `runner.py` та `cli.py` отримують мінімальні точкові зміни за існуючим патерном.

**Tech Stack:** Python 3.8-3.13, psycopg2-binary, pandas, numpy

---

## Chunk 1: Конфігурація та залежності

### Task 1: `PostgreSQLConfig` dataclass + env loading (`config.py`)

**Files:**
- Modify: `olap_tool/config.py`

- [ ] **Step 1: Додати `PostgreSQLConfig` dataclass після `DuckDBConfig` (~рядок 103)**

```python
@dataclass
class PostgreSQLConfig:
    """Налаштування підключення до PostgreSQL."""
    enabled: bool = False
    host: str = "localhost"
    port: int = 5432
    database: str = "analytics"
    user: str = "analytics"
    password: str = ""
    schema: str = "public"
    table: str = "sales"
    ssl_mode: str = "require"
```

- [ ] **Step 2: Додати `postgresql: PostgreSQLConfig` до `AppConfig` (після поля `duckdb`)**

```python
postgresql: PostgreSQLConfig = field(default_factory=PostgreSQLConfig)
```

- [ ] **Step 3: Додати `load_postgres_from_env()` після `load_duckdb_from_env()`**

```python
def load_postgres_from_env() -> PostgreSQLConfig:
    """Читає налаштування PostgreSQL з os.environ."""
    try:
        pg_port = int(os.getenv("PG_PORT", "5432"))
    except (ValueError, TypeError):
        pg_port = 5432
    return PostgreSQLConfig(
        enabled=_parse_bool(os.getenv("PG_ENABLED", "false"), False),
        host=os.getenv("PG_HOST", "localhost"),
        port=pg_port,
        database=os.getenv("PG_DATABASE", "analytics"),
        user=os.getenv("PG_USER", "analytics"),
        password=os.getenv("PG_PASSWORD", ""),
        schema=os.getenv("PG_SCHEMA", "public"),
        table=os.getenv("PG_TABLE", "sales"),
        ssl_mode=os.getenv("PG_SSL_MODE", "require"),
    )
```

- [ ] **Step 4: Оновити `apply_profile()` — додати `"postgresql"` до списку секцій (~рядок 267)**

Знайти рядок:
```python
for section in ("query", "export", "xlsx", "csv", "excel_header", "paths", "display", "clickhouse", "duckdb"):
```
Замінити на:
```python
for section in ("query", "export", "xlsx", "csv", "excel_header", "paths", "display", "clickhouse", "duckdb", "postgresql"):
```

- [ ] **Step 5: Оновити `build_config()` — додати завантаження PG env (після блоку DuckDB, перед `return AppConfig(...)`)**

```python
# PostgreSQL: env задає defaults, profile може перевизначити
pg_env = load_postgres_from_env()
pg_env_dict = {f.name: getattr(pg_env, f.name) for f in dataclass_fields(pg_env)}
base.setdefault("postgresql", {})
for k, v in pg_env_dict.items():
    base["postgresql"].setdefault(k, v)
```

- [ ] **Step 6: Оновити `return AppConfig(...)` — додати `postgresql` поле**

```python
postgresql=_build_section(PostgreSQLConfig, base, "postgresql"),
```

- [ ] **Step 7: Commit**

```bash
git add olap_tool/config.py
git commit -m "feat: PostgreSQLConfig dataclass та env loading"
```

---

### Task 2: Залежності та `.env.example`

**Files:**
- Modify: `requirements.txt`
- Modify: `.env.example`

- [ ] **Step 1: Додати `psycopg2-binary` до `requirements.txt` (після рядка `requests>=2.28.0`)**

```
psycopg2-binary>=2.9.0      # Для завантаження даних у PostgreSQL через COPY FROM STDIN
```

- [ ] **Step 2: Додати секцію PostgreSQL до `.env.example` (в кінець файлу)**

```
# ============================================================
# PostgreSQL (опційно — для завантаження даних у PostgreSQL)
# ============================================================

# Увімкнення завантаження у PostgreSQL паралельно з Excel/CSV
PG_ENABLED=false

# Адреса та порт PostgreSQL
# З роботи (через інтернет): PG_HOST=db.lwhs.xyz, PG_PORT=54321
# З дому (LAN/VPN):          PG_HOST=192.168.1.111, PG_PORT=5432
PG_HOST=localhost
PG_PORT=5432

# База даних та автентифікація
PG_DATABASE=analytics
PG_USER=analytics
PG_PASSWORD=

# Schema та таблиця
PG_SCHEMA=public
PG_TABLE=sales

# SSL режим: require | verify-ca | verify-full | disable
# require — шифрування без перевірки сертифікату (підходить для self-signed)
PG_SSL_MODE=require
```

- [ ] **Step 3: Встановити psycopg2-binary**

```bash
pip install psycopg2-binary
```

Очікуваний вивід: `Successfully installed psycopg2-binary-2.9.x`

- [ ] **Step 4: Commit**

```bash
git add requirements.txt .env.example
git commit -m "feat: psycopg2-binary залежність та PG_* змінні у .env.example"
```

---

## Chunk 2: PostgreSQLSink

### Task 3: `PostgreSQLSink` (`sinks.py`)

**Files:**
- Modify: `olap_tool/sinks.py`

- [ ] **Step 1: Додати TYPE_CHECKING імпорт для `PostgreSQLConfig` (у блок `if TYPE_CHECKING`)**

```python
if TYPE_CHECKING:
    from .config import ClickHouseConfig
    from .config import DuckDBConfig
    from .config import PostgreSQLConfig
```

- [ ] **Step 2: Додати helper `_pandas_dtype_to_pg()` після `_pandas_dtype_to_duck()` (~рядок 152)**

```python
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
```

- [ ] **Step 3: Додати клас `PostgreSQLSink` в кінець `sinks.py`**

```python
# ---------------------------------------------------------------------------
# PostgreSQL sink (psycopg2 + COPY FROM STDIN)
# ---------------------------------------------------------------------------

class PostgreSQLSink(AnalyticsSink):
    """
    Завантажує DataFrame у PostgreSQL через COPY FROM STDIN.

    Ідемпотентність: DELETE WHERE year_num=X AND week_num=Y → COPY FROM STDIN CSV.
    SSL: sslmode=require (шифрування без перевірки self-signed сертифікату).
    """

    def __init__(self, config: "PostgreSQLConfig"):
        self._config = config
        self._conn = None
        self._schema: dict[str, str] | None = None

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
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self._full_table()} ({cols_ddl})"
            )
        conn.commit()
        self._refresh_schema()

        # Додаємо нові колонки яких немає в таблиці
        for col in df.columns:
            if col not in (self._schema or {}):
                dtype = _pandas_dtype_to_pg(df[col].dtype)
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'ALTER TABLE {self._full_table()} '
                            f'ADD COLUMN IF NOT EXISTS "{col}" {dtype}'
                        )
                    conn.commit()
                    if self._schema is not None:
                        self._schema[col] = dtype
                except Exception as e:
                    conn.rollback()
                    print_warning(f"Не вдалося додати колонку `{col}`: {e} — пропускаємо")

    def delete_period(self, year: int, week: int) -> None:
        if self._schema is None:
            self._refresh_schema()
        schema = self._schema or {}
        if "year_num" not in schema or "week_num" not in schema:
            return
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._full_table()} "
                f"WHERE year_num = %s AND week_num = %s",
                (year, week),
            )
        conn.commit()

    def insert(self, df: pd.DataFrame, year: int, week: int) -> int:
        import io
        if df is None or len(df) == 0:
            return 0

        df = sanitize_df(df)

        # Фільтруємо до колонок що є в таблиці
        if self._schema:
            cols = [c for c in df.columns if c in self._schema]
            df = df[cols]

        if df.empty:
            return 0

        # DataFrame → CSV у пам'яті (None → порожній рядок = NULL у COPY)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="")
        buf.seek(0)

        col_list = ", ".join(f'"{c}"' for c in df.columns)
        copy_sql = (
            f"COPY {self._full_table()} ({col_list}) "
            f"FROM STDIN WITH (FORMAT CSV, NULL '')"
        )

        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, buf)
        conn.commit()
        return len(df)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
```

- [ ] **Step 4: Перевірити синтаксис**

```bash
python -c "from olap_tool.sinks import PostgreSQLSink; print('OK')"
```

Очікуваний вивід: `OK`

- [ ] **Step 5: Commit**

```bash
git add olap_tool/sinks.py
git commit -m "feat: PostgreSQLSink з COPY FROM STDIN"
```

---

## Chunk 3: Інтеграція

### Task 4: `runner.py` — підключення sink

**Files:**
- Modify: `olap_tool/runner.py`

- [ ] **Step 1: Додати імпорт `PostgreSQLSink` (рядок 27 — поряд з іншими sinks)**

```python
from .sinks import ClickHouseSink, DuckDBSink, PostgreSQLSink
```

- [ ] **Step 2: Додати info-блок для PostgreSQL (після DuckDB info-блоку, ~рядок 253)**

```python
# PostgreSQL налаштування
if config.postgresql.enabled or export_format in ("PG", "POSTGRESQL"):
    print(
        f"   {Fore.CYAN}PostgreSQL:      {Fore.WHITE}{config.postgresql.host}:{config.postgresql.port}"
    )
    print(
        f"   {Fore.CYAN}PG Table:        {Fore.WHITE}{config.postgresql.schema}.{config.postgresql.table}"
    )
```

- [ ] **Step 3: Додати побудову `PostgreSQLSink` (після DuckDB sink, ~рядок 260)**

```python
if config.postgresql.enabled or export_format in ("PG", "POSTGRESQL"):
    sinks.append(PostgreSQLSink(config.postgresql))
```

- [ ] **Step 4: Commit**

```bash
git add olap_tool/runner.py
git commit -m "feat: PostgreSQLSink інтеграція у runner.py"
```

---

### Task 5: `cli.py` — нові значення `--format`

**Files:**
- Modify: `olap_tool/cli.py`

- [ ] **Step 1: Оновити `choices` у `--format` (~рядок 113)**

Знайти:
```python
choices=['xlsx', 'csv', 'both'],
help='Формат експорту: xlsx, csv або both (за замовчуванням з config.yaml)'
```
Замінити на:
```python
choices=['xlsx', 'csv', 'both', 'ch', 'clickhouse', 'duck', 'duckdb', 'pg', 'postgresql'],
help='Формат експорту: xlsx, csv, both або аналітичний sink: ch/clickhouse, duck/duckdb, pg/postgresql'
```

- [ ] **Step 2: Перевірити `--help`**

```bash
python olap.py --help
```

Очікуваний вивід: у секції `--format` повинні бути `pg`, `postgresql`.

- [ ] **Step 3: Перевірити синтаксис конфігу**

```bash
python -c "from olap_tool.config import AppConfig, PostgreSQLConfig; c = AppConfig(); print(c.postgresql)"
```

Очікуваний вивід: `PostgreSQLConfig(enabled=False, host='localhost', ...)`

- [ ] **Step 4: Commit**

```bash
git add olap_tool/cli.py
git commit -m "feat: додано pg/postgresql до --format CLI"
```

---

## Chunk 4: Фінальна перевірка

### Task 6: Smoke test та підсумковий commit

- [ ] **Step 1: Перевірити імпорти всіх нових компонентів**

```bash
python -c "
from olap_tool.config import PostgreSQLConfig, AppConfig, load_postgres_from_env, build_config
from olap_tool.sinks import PostgreSQLSink, _pandas_dtype_to_pg
import pandas as pd
cfg = PostgreSQLConfig(host='db.lwhs.xyz', port=54321, enabled=True)
print('Config OK:', cfg.host, cfg.port)
df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
sink = PostgreSQLSink(cfg)
print('Sink created OK')
print('dtype int →', _pandas_dtype_to_pg(df['a'].dtype))
print('dtype obj →', _pandas_dtype_to_pg(df['b'].dtype))
"
```

Очікуваний вивід:
```
Config OK: db.lwhs.xyz 54321
Sink created OK
dtype int → BIGINT
dtype obj → TEXT
```

- [ ] **Step 2: Перевірити build_config з PG env змінними**

```bash
python -c "
import os
os.environ['PG_ENABLED'] = 'true'
os.environ['PG_HOST'] = 'db.lwhs.xyz'
os.environ['PG_PORT'] = '54321'
os.environ['PG_PASSWORD'] = 'test'
from olap_tool.config import build_config
import argparse
args = argparse.Namespace(format=None, filter=None, timeout=None, compress=None, debug=False)
cfg = build_config(args)
print('PG enabled:', cfg.postgresql.enabled)
print('PG host:', cfg.postgresql.host)
print('PG port:', cfg.postgresql.port)
"
```

Очікуваний вивід:
```
PG enabled: True
PG host: db.lwhs.xyz
PG port: 54321
```

- [ ] **Step 3: Перевірити що `sanitize_df` коректно обробляє NaN перед COPY**

```bash
python -c "
import pandas as pd
import numpy as np
from olap_tool.sinks import sanitize_df
df = pd.DataFrame({'a': [1.0, np.inf, -np.inf, np.nan], 'b__test': ['x', None, 'y', 'z']})
result = sanitize_df(df)
print(result)
print('columns:', list(result.columns))
"
```

Очікуваний вивід: колонка `b__test` перейменована у `b__test` (без змін — вже безпечна), `inf` → `NaN`.

- [ ] **Step 4: Перевірити `--format pg` у CLI**

```bash
python olap.py --help | grep -A2 "format"
```

Очікуваний вивід: у рядку choices є `pg` та `postgresql`.

- [ ] **Step 5: Підсумковий commit (якщо є незакомічені зміни)**

```bash
git status
git add -A
git commit -m "feat: PostgreSQL sink повна інтеграція (psycopg2 COPY FROM STDIN)"
```

---

## Використання

Після реалізації:

```bash
# .env — додати:
PG_ENABLED=true
PG_HOST=db.lwhs.xyz
PG_PORT=54321
PG_DATABASE=analytics
PG_USER=analytics
PG_PASSWORD=1c37552e...
PG_SSL_MODE=require

# Запуск
python olap.py --last-weeks 4

# Або sink-only режим
python olap.py --last-weeks 4 --format pg

# З дому (LAN)
PG_HOST=192.168.1.111 PG_PORT=5432 python olap.py --last-weeks 4
```

**Профіль** `profiles/pg_export.yaml`:
```yaml
name: pg_export
description: "Щотижневий експорт у PostgreSQL"
postgresql:
  enabled: true
  table: weekly_sales
period:
  type: auto
  auto_type: last-weeks
  auto_value: 4
```
