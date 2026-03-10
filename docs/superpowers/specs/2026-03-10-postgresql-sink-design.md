# PostgreSQL Sink — Design Document

**Date:** 2026-03-10
**Status:** Approved

## Overview

Додати `PostgreSQLSink` як третій аналітичний sink поряд із `ClickHouseSink` і `DuckDBSink`. Дані завантажуються через `COPY FROM STDIN` (psycopg2) — найшвидший метод bulk-завантаження у PostgreSQL.

## Architecture

### New: `PostgreSQLConfig` dataclass (`config.py`)

```python
@dataclass
class PostgreSQLConfig:
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

### `.env` variables

```
PG_ENABLED=true
PG_HOST=db.lwhs.xyz
PG_PORT=54321
PG_DATABASE=analytics
PG_USER=analytics
PG_PASSWORD=...
PG_SCHEMA=public
PG_TABLE=sales
PG_SSL_MODE=require
```

`sslmode=require` — шифрування без перевірки self-signed сертифікату.

### `PostgreSQLSink` (`sinks.py`)

Реалізує `AnalyticsSink`:

- **`setup(df)`** — `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN IF NOT EXISTS` за схемою DataFrame. Кешує схему через `information_schema.columns`.
- **`delete_period(year, week)`** — `DELETE WHERE year_num=X AND week_num=Y` (ідемпотентність).
- **`insert(df, year, week)`** — `sanitize_df` → DataFrame → `io.StringIO` CSV → `cursor.copy_expert("COPY ... FROM STDIN WITH (FORMAT CSV, NULL '')")`.
- **`close()`** — `conn.close()` у try/except.

#### pandas dtype → PostgreSQL type mapping

| pandas dtype | PostgreSQL type   |
|--------------|-------------------|
| int/uint     | BIGINT            |
| float        | DOUBLE PRECISION  |
| bool         | BOOLEAN           |
| datetime     | TIMESTAMP         |
| date         | DATE              |
| інші         | TEXT              |

### Integration (`runner.py`)

За патерном ClickHouse/DuckDB:
- Виводить info-рядки про підключення якщо `config.postgresql.enabled` або `--format pg/postgresql`
- Додає `PostgreSQLSink(config.postgresql)` до списку `sinks`

### CLI (`cli.py`)

`--format` приймає нові значення: `pg`, `postgresql`

### Profile support

```yaml
postgresql:
  enabled: true
  table: weekly_sales
```

`apply_profile()` обробляє секцію `postgresql:`.

## Files Changed

| File | Change |
|------|--------|
| `olap_tool/config.py` | `PostgreSQLConfig`, `load_postgres_from_env()`, оновлення `AppConfig`, `apply_profile()`, `build_config()` |
| `olap_tool/sinks.py` | `PostgreSQLSink` клас + `_pandas_dtype_to_pg()` helper |
| `olap_tool/runner.py` | Info display + sink instantiation |
| `olap_tool/cli.py` | Нові значення для `--format` |
| `requirements.txt` | `psycopg2-binary` |
| `.env.example` | `PG_*` змінні |

## Decisions

- **Прямий psycopg2** (без SQLAlchemy) — менше залежностей, максимальна швидкість
- **COPY FROM STDIN** — найшвидший метод bulk-завантаження (50-100k рядків/сек)
- **Один хост у `.env`** — користувач перемикає `PG_HOST` вручну між роботою і домом
- **`sslmode=require`** — SSL обов'язковий, self-signed cert не перевіряється
- **Sink у `sinks.py`** — не окремий файл, як DuckDB
