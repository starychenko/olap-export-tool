# OLAP Export Tool — TUI + Реструктуризація

**Дата:** 2026-03-10
**Статус:** Затверджено

## Мета

Реорганізувати кодову базу по підпакетах, об'єднати batch-скрипти імпорту в один, додати підтримку PostgreSQL в скрипт імпорту, та реалізувати повноцінний Textual TUI як основний інтерфейс.

---

## 1. Нова структура файлів

```
olap-export-tool/
├── olap.py                        # точка входу: TUI якщо без аргументів, CLI якщо з аргументами
│
├── olap_tool/
│   ├── __init__.py
│   │
│   ├── core/                      # базова інфраструктура
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── cli.py
│   │   ├── runner.py
│   │   ├── periods.py
│   │   ├── profiles.py
│   │   ├── scheduler.py
│   │   ├── compression.py
│   │   ├── progress.py
│   │   └── utils.py
│   │
│   ├── connection/                # OLAP підключення та автентифікація
│   │   ├── __init__.py
│   │   ├── connection.py
│   │   ├── auth.py
│   │   ├── security.py
│   │   └── prompt.py
│   │
│   ├── data/                      # DAX запити та файловий експорт
│   │   ├── __init__.py
│   │   ├── queries.py
│   │   └── exporter.py
│   │
│   ├── sinks/                     # аналітичні сховища
│   │   ├── __init__.py
│   │   ├── base.py                # AnalyticsSink ABC + sanitize_df()
│   │   ├── clickhouse.py          # ClickHouseSink (поглинає clickhouse_export.py)
│   │   ├── duckdb.py              # DuckDBSink
│   │   └── postgresql.py          # PostgreSQLSink
│   │
│   └── tui/                       # Textual TUI
│       ├── __init__.py
│       ├── app.py                 # головний OlapApp : App
│       ├── screens/
│       │   ├── __init__.py
│       │   ├── main_menu.py       # головне меню
│       │   ├── olap_export.py     # екран експорту з OLAP
│       │   └── xlsx_import.py     # екран імпорту XLSX
│       └── widgets/
│           ├── __init__.py
│           ├── log_panel.py       # RichLog з перехопленням print_*
│           └── progress_bar.py    # обгортка для прогрес-бару
│
└── scripts/                       # batch-утиліти
    └── import_xlsx.py             # об'єднаний імпорт XLSX → CH / DuckDB / PG
```

**Видаляються:**
- `import_xlsx_to_clickhouse.py` (корінь)
- `import_xlsx_to_duckdb.py` (корінь)
- `olap_tool/clickhouse_export.py` (поглинається `sinks/clickhouse.py`)
- `olap_tool/sinks.py` (розбивається на `sinks/base.py`, `sinks/clickhouse.py`, `sinks/duckdb.py`, `sinks/postgresql.py`)

---

## 2. TUI екрани та навігація

### Головне меню
```
┌─────────────────────────────────────────────┐
│          OLAP Export Tool  v2.0             │
├─────────────────────────────────────────────┤
│                                             │
│   > Експорт з OLAP куба                     │
│     Імпорт XLSX в аналітику                 │
│     Налаштування                            │
│     Вийти                                   │
│                                             │
└─────────────────────────────────────────────┘
```

### Екран "Експорт з OLAP"
Форма з полями:
- Профіль (Select зі списку `profiles/*.yaml`)
- Формат: xlsx / csv / both / ch / duck / pg
- Період: last-weeks N / current-month / manual range
- Стиснення: none / zip
- Кнопка "Запустити" → праворуч `RichLog` з живим виводом операції
- Кнопка "Скасувати" (з'являється під час виконання)

### Екран "Імпорт XLSX"
Форма з полями:
- Ціль: ClickHouse / DuckDB / PostgreSQL (RadioSet)
- Директорія з файлами (Input + Browse)
- Рік, тиждень (опційні фільтри)
- Workers: 1–16
- Dry-run: Checkbox
- Кнопка "Запустити" → `RichLog` з прогресом
- Кнопка "Скасувати"

### Навігація
- `q` / `Escape` → назад / вихід
- `Tab` / `Enter` → між елементами форми
- Під час виконання операції — форма блокується, активна кнопка "Скасувати"

---

## 3. Технічні рішення

### Детектування режиму в `olap.py`
```python
if len(sys.argv) == 1:
    from olap_tool.tui.app import OlapApp
    OlapApp().run()
else:
    from olap_tool.core.runner import main
    sys.exit(main())
```

### Перехоплення `print_*` для TUI
`utils.py` отримує `set_log_handler(fn: Callable[[str], None] | None)`. Коли TUI активний, всі виклики `print_error/warning/success/progress` пишуть у `RichLog`-віджет через handler. CLI-режим залишається без змін (handler = None → stdout).

### Textual Workers
Операції (експорт, імпорт) виконуються в `Worker` Textual:
```python
self.run_worker(self.run_export(), exclusive=True)
```
Cancellation через `worker.cancel()` по кнопці "Скасувати".

### `scripts/import_xlsx.py` — об'єднаний скрипт
```bash
python scripts/import_xlsx.py --target ch   --dir result/ --workers 4
python scripts/import_xlsx.py --target duck --year 2025 --week 10
python scripts/import_xlsx.py --target pg   --dry-run
```
- Спільна логіка: file discovery, Excel reading (calamine + openpyxl fallback), Rich progress
- Thread-local client для ClickHouse; shared Session для DuckDB та PostgreSQL

### Реімпорти після реструктуризації
- Всі відносні імпорти оновлюються до нових шляхів
- `olap_tool/__init__.py` реекспортує публічні символи для зворотної сумісності зовнішнього коду
- `sinks/__init__.py` реекспортує: `AnalyticsSink`, `sanitize_df`, `ClickHouseSink`, `DuckDBSink`, `PostgreSQLSink`

---

## 4. Залежності

Нові пакети для додавання в `requirements.txt`:
- `textual>=0.70.0` — TUI фреймворк

---

## 5. Порядок реалізації

1. Реструктуризація `sinks/` (base.py, clickhouse.py, duckdb.py, postgresql.py)
2. Реструктуризація `core/`, `connection/`, `data/`
3. Оновлення всіх імпортів
4. `scripts/import_xlsx.py`
5. TUI: `tui/app.py` + `screens/main_menu.py`
6. TUI: `screens/olap_export.py` + log handler в `utils.py`
7. TUI: `screens/xlsx_import.py`
8. Оновлення `olap.py` — детектування режиму
9. Видалення старих файлів
