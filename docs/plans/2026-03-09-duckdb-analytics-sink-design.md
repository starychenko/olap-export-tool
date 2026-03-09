# DuckDB Analytics Sink — Design Document

**Дата:** 2026-03-09
**Статус:** Затверджено

## Мета

Додати підтримку DuckDB як сховища поряд із ClickHouse. DuckDB доступний через REST API (FastAPI + uvicorn на `https://analytics.lwhs.xyz`). Інтеграція реалізується через абстракцію `AnalyticsSink` — рефакторинг існуючого ClickHouse-коду і паралельне додавання DuckDB.

## Архітектура

### Абстракція `AnalyticsSink` (`olap_tool/sinks.py`)

```python
class AnalyticsSink(ABC):
    def setup(self, df: pd.DataFrame) -> None: ...      # CREATE TABLE IF NOT EXISTS
    def delete_period(self, year: int, week: int) -> None: ...  # ідемпотентний DELETE
    def insert(self, df: pd.DataFrame, year: int, week: int) -> int: ...  # повертає row_count
    def close(self) -> None: ...
```

Реалізації:
- `ClickHouseSink` — адаптер навколо існуючого `clickhouse_export.py`
- `DuckDBSink` — HTTP через `requests.Session` до REST API

Спільна утиліта `sanitize_df()` виноситься з `clickhouse_export.py` до `sinks.py`.

### Зміни в `queries.py`

`run_dax_query()` замість `ch_config` отримує `sinks: list[AnalyticsSink]`. Після формування DataFrame ітерує по активних sinks:

```python
for sink in sinks:
    sink.insert(df, year=year_num, week=week_num)
```

### Зміни в `runner.py`

Будує список активних sinks із конфігу:
```python
sinks = []
if config.clickhouse.enabled or ch_only:
    sinks.append(ClickHouseSink(config.clickhouse))
if config.duckdb.enabled or duck_only:
    sinks.append(DuckDBSink(config.duckdb))
```

## Конфігурація

### `DuckDBConfig` dataclass (`config.py`)

```python
@dataclass
class DuckDBConfig:
    enabled: bool = False
    url: str = "https://analytics.lwhs.xyz"
    api_key: str = ""
    table: str = "sales"
    batch_size: int = 1000
```

### Env-змінні (`.env`)

```
DUCK_ENABLED=true
DUCK_URL=https://analytics.lwhs.xyz
DUCK_API_KEY=<key>
DUCK_TABLE=sales
DUCK_BATCH_SIZE=1000
```

### `AppConfig` розширення

```python
duckdb: DuckDBConfig = field(default_factory=DuckDBConfig)
```

`apply_profile()` підтримує секцію `duckdb` у YAML-профілях.

### Формат експорту

- `--format duck` / `--format duckdb` — тільки DuckDB (аналог `--format ch`)
- `enabled: true` у `clickhouse` і/або `duckdb` — обидва активні паралельно

## `DuckDBSink` — деталі реалізації

### Type mapping pandas → DuckDB

| pandas dtype | DuckDB тип |
|---|---|
| int*/uint* | BIGINT |
| float* | DOUBLE |
| bool | BOOLEAN |
| datetime* | TIMESTAMP |
| date | DATE |
| object/string/category | VARCHAR |

### Вставка даних

Батч VALUES через POST `/execute`:
```sql
INSERT INTO sales (col1, col2) VALUES (v1, v2), (v1, v2), ...
```
DataFrame розбивається на чанки `batch_size` рядків, кожен чанк = один SQL statement.

### Ідемпотентність

```sql
DELETE FROM sales WHERE year_num = 2025 AND week_num = 10;
-- потім batch INSERT
```

### Setup (schema)

```sql
CREATE TABLE IF NOT EXISTS sales (col1 BIGINT, col2 VARCHAR, ...);
-- schema evolution:
ALTER TABLE sales ADD COLUMN IF NOT EXISTS new_col VARCHAR;
```

### HTTP клієнт

`requests.Session` з `X-API-Key` header, `Content-Type: application/json`, timeout 600s.

Ендпоінти:
- `POST /execute` — DDL/DML (`{"statements": [...]}`)
- `POST /query` — читання схеми (`{"sql": "DESCRIBE sales"}`)

## Batch-скрипт `import_xlsx_to_duckdb.py`

Аналог `import_xlsx_to_clickhouse.py`:

```bash
python import_xlsx_to_duckdb.py
python import_xlsx_to_duckdb.py --year 2025 --week 10
python import_xlsx_to_duckdb.py --workers 4
python import_xlsx_to_duckdb.py --dry-run
```

**Відмінності від ClickHouse-варіанту:**
- `requests.Session` thread-safe → один спільний session на всі воркери (не thread-local)
- Схема кешується один раз через `DESCRIBE TABLE` перед паралельним завантаженням
- UI: rich progress bar (аналогічний)

## Нові файли

| Файл | Опис |
|---|---|
| `olap_tool/sinks.py` | `AnalyticsSink` ABC + `ClickHouseSink` + `DuckDBSink` |
| `import_xlsx_to_duckdb.py` | Batch-скрипт для завантаження XLSX |

## Змінені файли

| Файл | Зміни |
|---|---|
| `olap_tool/config.py` | `DuckDBConfig`, `AppConfig.duckdb`, `load_duckdb_from_env()`, `apply_profile()` |
| `olap_tool/queries.py` | `sinks: list[AnalyticsSink]` замість `ch_config` |
| `olap_tool/runner.py` | Побудова списку sinks, `duck_only` формат |
| `olap_tool/clickhouse_export.py` | `sanitize_df` переноситься до `sinks.py` (або реекспортується) |
