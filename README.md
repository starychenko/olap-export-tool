# OLAP Export Tool

Інструмент для автоматизованого експорту даних з OLAP кубів (Microsoft Analysis Services) у файли Excel та CSV з підтримкою CLI, інтерактивного консольного UI, YAML-конфігурації, профілів, планувальника, автоматичних періодів та завантаження у ClickHouse, DuckDB та PostgreSQL.

## Основні можливості

- **Інтерактивне консольне меню** — wizard-режим без аргументів (InquirerPy + rich)
- **Єдина YAML-конфігурація** — всі налаштування в `config.yaml`, секрети окремо в `.env`
- **CLI аргументи** — гнучке управління через командний рядок
- **Автоматичні періоди** — 7 варіантів розумних періодів (останні N тижнів, поточний місяць, квартал і т.д.)
- **Профілі** — збереження наборів налаштувань для різних сценаріїв (перевизначають будь-яку секцію конфігу)
- **Планувальник** — автоматичне виконання експортів за розкладом
- **Стиснення** — ZIP архівація результатів
- **Два методи аутентифікації** — Windows (SSPI) та Логін/Пароль
- **Шифрування облікових даних** — безпечне зберігання паролів з прив'язкою до машини
- **Потоковий експорт** — ефективна робота з великими наборами даних
- **3 аналітичних sink'и** — ClickHouse, DuckDB (REST API), PostgreSQL (COPY FROM STDIN)
- **Паралельний імпорт XLSX** — єдиний скрипт для всіх sink'ів з thread pool

## Вимоги

### Python
- **Рекомендовано:** Python 3.13
- **Підтримується:** Python 3.8 — 3.13
- **Не підтримується:** Python 3.14+ (несумісність pythonnet)

> **Важливо:** Python 3.14 не підтримує pythonnet, тому OLAP-підключення працює лише на Python 3.8–3.13.

### Залежності
```bash
pip install -r requirements.txt
```

Основні бібліотеки:
- `pythonnet` — .NET інтероп для ADOMD.NET (Python 3.8–3.13)
- `pandas`, `xlsxwriter` — обробка та експорт даних
- `PyYAML` — конфігурація та профілі
- `schedule` — планувальник задач
- `cryptography` — шифрування облікових даних
- `python-dotenv`, `colorama` — завантаження .env та кольоровий вивід
- `rich`, `InquirerPy` — інтерактивний консольний UI (панелі, таблиці, fuzzy select)
- `clickhouse-connect` — завантаження даних у ClickHouse
- `requests` — DuckDB REST API
- `psycopg2-binary` — завантаження даних у PostgreSQL (COPY FROM STDIN)
- `python-calamine` — швидке читання Excel (Rust, 3-10x швидше за openpyxl)

## Швидкий старт

### 1. Налаштування секретів

Скопіюйте `.env.example` в `.env` та вкажіть параметри підключення:

```bash
cp .env.example .env
```

`.env` містить **тільки секрети** (сервер, БД, автентифікація):
```bash
OLAP_SERVER=10.40.0.48
OLAP_DATABASE=Sells
OLAP_AUTH_METHOD=LOGIN       # SSPI або LOGIN
OLAP_DOMAIN=EPICENTRK
OLAP_CREDENTIALS_ENCRYPTED=true
OLAP_CREDENTIALS_FILE=.credentials
```

### 2. Налаштування конфігурації (опційно)

Всі не-секретні налаштування знаходяться у `config.yaml`. За замовчуванням він уже містить розумні значення. За потреби скопіюйте з прикладу та відредагуйте:

```bash
cp config.yaml.example config.yaml
```

Див. [config.yaml.example](config.yaml.example) для повного переліку налаштувань.

### 3. Перший запуск

```bash
# Без аргументів — інтерактивне меню
python olap.py

# З аргументами — CLI режим
python olap.py --last-weeks 4 --format xlsx

# При першому запуску з LOGIN методом введіть облікові дані —
# вони будуть зашифровані та збережені автоматично
```

## Інтерактивне меню

При запуску без аргументів (`python olap.py`) відкривається консольне меню зі стрілковою навігацією:

```
┌──────────────────────────────────────┐
│ OLAP Export Tool                     │
│ Сервер: 10.40.0.48  ·  Auth: LOGIN  │
└──────────────────────────────────────┘
? Оберіть дію:
❯ Експорт з OLAP куба
  Імпорт XLSX в аналітику
  ──────────────
  Вийти
```

### Експорт з OLAP куба (wizard)

Покроковий wizard:
1. Вибір профілю (fuzzy search або пропустити)
2. Формат (XLSX, CSV, XLSX+CSV, ClickHouse, DuckDB, PostgreSQL)
3. Тип періоду (останні тижні, поточний місяць, ручний і т.д.)
4. Значення періоду (кількість тижнів або діапазон YYYY-WW:YYYY-WW)
5. Стиснення (ZIP або без)
6. Підтвердження та запуск

### Імпорт XLSX в аналітику (wizard)

Покроковий wizard:
1. Цільовий sink (ClickHouse, DuckDB, PostgreSQL)
2. Директорія з XLSX-файлами
3. Фільтр по року (опційно)
4. Фільтр по тижню (опційно)
5. Кількість паралельних воркерів (1–32)
6. Dry-run режим (опційно)
7. Підтвердження та запуск

## Використання (CLI)

### Базові команди

```bash
# Допомога
python olap.py --help

# Інтерактивне меню
python olap.py

# Очистити збережені облікові дані
python olap.py clear_credentials
```

### CLI Аргументи

#### Автоматичні періоди

```bash
# Останні 4 тижні
python olap.py --last-weeks 4 --format xlsx --compress zip

# Поточний місяць
python olap.py --current-month --format csv

# Попередній місяць
python olap.py --last-month --format both

# Поточний квартал (Q1-Q4)
python olap.py --current-quarter --compress zip

# Попередній квартал
python olap.py --last-quarter --format xlsx

# З початку року до сьогодні
python olap.py --year-to-date --compress zip

# Ковзаюче вікно 12 тижнів
python olap.py --rolling-weeks 12 --format xlsx
```

#### Ручні періоди

```bash
# Діапазон періодів
python olap.py --period 2025-01:2025-52 --format xlsx

# Або окремо початок та кінець
python olap.py --start 2025-01 --end 2025-12
```

#### Параметри експорту

```bash
# Формат: xlsx, csv, both, ch, duck, pg
python olap.py --last-weeks 4 --format both

# Кастомний фільтр
python olap.py --current-month --filter "Побутова техніка"

# Таймаут між запитами
python olap.py --last-quarter --timeout 60

# ZIP стиснення (оригінали зберігаються)
python olap.py --year-to-date --compress zip
```

### Профілі

Профілі дозволяють зберігати набори налаштувань для різних сценаріїв. Профіль може перевизначити **будь-яку** секцію з `config.yaml`.

```bash
# Список доступних профілів
python olap.py --list-profiles

# Використання профілю
python olap.py --profile weekly_sales

# Перевизначення параметрів профілю через CLI
python olap.py --profile weekly_sales --format csv --compress zip
```

#### Створення профілю

Створіть YAML файл у директорії `profiles/`:

```yaml
# profiles/my_report.yaml
name: my_report
description: Мій власний звіт

# Період (секція тільки для профілю)
period:
  type: auto
  auto_type: last-weeks
  auto_value: 4

# Перевизначення секцій з config.yaml
query:
  filter_fg1_name: Споживча електроніка
  timeout: 30

export:
  format: xlsx
  compress: zip

xlsx:
  streaming: true

# Планувальник (секція тільки для профілю)
schedule:
  enabled: true
  simple: "every monday at 09:00"
```

### Планувальник

Автоматичне виконання експортів за розкладом.

```bash
# Запуск за розкладом (одноразове налаштування)
python olap.py --profile weekly_sales --schedule "every monday at 09:00"

# Daemon режим (постійна робота в фоні)
python olap.py --profile weekly_sales --daemon
```

#### Формати розкладу

```
"every monday at 09:00"
"every day at 18:00"
"every 1 week"
"every 3 days at 14:30"
```

## Аналітичні sink'и

Інструмент підтримує завантаження даних у три аналітичних бекенди. Всі реалізують спільний інтерфейс `AnalyticsSink` (ABC) з ідемпотентною вставкою (DELETE + INSERT за `year_num + week_num`).

### ClickHouse

**Метод:** clickhouse-connect з LZ4 стисненням

**Особливості:**
- Автоматичне створення БД та таблиці зі схемою з DataFrame
- Schema evolution — автоматично додає нові колонки через `ALTER TABLE`
- Thread-local клієнти для паралельного імпорту

**Налаштування (.env):**
```bash
CH_ENABLED=true
CH_HOST=your-clickhouse-host
CH_PORT=8443
CH_USERNAME=default
CH_PASSWORD=your_password
CH_SECURE=true
CH_DATABASE=olap_export
CH_TABLE=sales
```

**Використання:**
```bash
# Пряме завантаження під час експорту
python olap.py --last-weeks 4 --format ch
```

### DuckDB (REST API)

**Метод:** HTTP REST API до зовнішнього DuckDB-сервісу

**Особливості:**
- Відправка даних через REST API (без локального DuckDB клієнта)
- Автоматичне визначення типів колонок з pandas DataFrame
- Thread-safe (одна shared сесія для всіх воркерів)
- Batch INSERT з налаштовуваним batch_size

**Налаштування (.env):**
```bash
DUCK_ENABLED=true
DUCK_URL=https://analytics.lwhs.xyz
DUCK_API_KEY=<your-key>
DUCK_TABLE=sales
DUCK_BATCH_SIZE=1000
```

**Використання:**
```bash
python olap.py --last-weeks 4 --format duck
```

### PostgreSQL

**Метод:** psycopg2 з `COPY FROM STDIN WITH (FORMAT CSV)`

**Особливості:**
- Bulk load через COPY (найшвидший метод для PostgreSQL)
- `\N` як NULL sentinel
- Schema evolution — автоматично додає нові колонки
- SSL підключення (за замовчуванням `sslmode=require`)
- **Не thread-safe** — кожен воркер створює власне з'єднання

**Налаштування (.env):**
```bash
PG_ENABLED=true
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=analytics
PG_USER=analytics
PG_PASSWORD=your_password
PG_SCHEMA=public
PG_TABLE=sales
PG_SSL_MODE=require
```

**Використання:**
```bash
python olap.py --last-weeks 4 --format pg
```

### Пакетний імпорт XLSX

Єдиний скрипт `scripts/import_xlsx.py` імпортує існуючі XLSX-файли у будь-який sink:

```bash
# ClickHouse (8 паралельних воркерів)
python scripts/import_xlsx.py --target ch --dir result/ --workers 8

# DuckDB (тільки 2025 рік)
python scripts/import_xlsx.py --target duck --dir result/ --year 2025

# PostgreSQL (конкретний тиждень)
python scripts/import_xlsx.py --target pg --dir result/ --year 2025 --week 10

# Показати файли без завантаження (dry run)
python scripts/import_xlsx.py --target ch --dir result/ --dry-run
```

Або через інтерактивне меню: `python olap.py` → "Імпорт XLSX в аналітику".

### Підключення Excel до ClickHouse

Для аналізу даних у Excel підключіться через ODBC:

1. Встановіть [ClickHouse ODBC Driver](https://github.com/ClickHouse/clickhouse-odbc)
2. Налаштуйте DSN у Windows ODBC Data Sources (`SSLMode=require`, не `strict`)
3. В Excel: **Дані → Отримати дані → З інших джерел → З ODBC**
4. Виберіть DSN та використовуйте Power Query для агрегації даних

## Конфігурація

### Пріоритет налаштувань

Система конфігурації дотримується строгого пріоритету (від найвищого до найнижчого):

1. **CLI аргументи** (`--format`, `--timeout`, тощо)
2. **Профіль** (якщо вказано `--profile`)
3. **Legacy .env** (не-секретні ключі з `.env` — з попередженнями про міграцію)
4. **config.yaml** (основна конфігурація)
5. **Вбудовані значення** (defaults у dataclass)

### Файли конфігурації

| Файл | Призначення |
|---|---|
| `.env` | Секрети: OLAP сервер/БД, автентифікація, ClickHouse/DuckDB/PostgreSQL підключення |
| `config.yaml` | Все інше: запити, експорт, форматування, шляхи, відображення |
| `profiles/*.yaml` | Перевизначення будь-якої секції config.yaml для конкретного сценарію |

### config.yaml — основні секції

```yaml
query:
  filter_fg1_name: Споживча електроніка  # Фільтр категорії
  timeout: 30                            # Таймаут між запитами (сек)

export:
  format: xlsx          # xlsx, csv, both, ch, duck, pg
  compress: none        # zip або none
  force_csv_only: false # Ігнорувати xlsx навіть якщо вказано

xlsx:
  streaming: false      # Потоковий запис (менший пік пам'яті)
  min_format: false     # Без автоширини та freeze panes

csv:
  delimiter: ";"
  encoding: "utf-8-sig"
  quoting: "minimal"    # minimal, all, nonnumeric

excel_header:
  color: "00365E"       # Колір фону заголовків (HEX)
  font_color: "FFFFFF"  # Колір шрифту заголовків (HEX)
  font_size: 11

paths:
  adomd_dll: "./lib"    # Шлях до бібліотеки ADOMD.NET
  result_dir: "result"  # Директорія для результатів

display:
  ascii_logs: false     # ASCII-режим (без emoji)
  debug: false
  progress_update_interval_ms: 100
```

Повний приклад з описом: [config.yaml.example](config.yaml.example)

### .env — тільки секрети

```bash
# OLAP підключення
OLAP_SERVER=10.40.0.48
OLAP_DATABASE=Sells
OLAP_AUTH_METHOD=LOGIN
OLAP_DOMAIN=EPICENTRK
OLAP_CREDENTIALS_ENCRYPTED=true
OLAP_CREDENTIALS_FILE=.credentials
OLAP_USE_MASTER_PASSWORD=false

# ClickHouse
CH_ENABLED=true
CH_HOST=your-clickhouse-host
CH_PORT=8443
CH_USERNAME=default
CH_PASSWORD=your_password
CH_SECURE=true
CH_DATABASE=olap_export
CH_TABLE=sales

# DuckDB REST API
DUCK_ENABLED=true
DUCK_URL=https://analytics.lwhs.xyz
DUCK_API_KEY=your_key
DUCK_TABLE=sales

# PostgreSQL
PG_ENABLED=true
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=analytics
PG_USER=analytics
PG_PASSWORD=your_password
PG_SCHEMA=public
PG_TABLE=sales
PG_SSL_MODE=require
```

Повний приклад: [.env.example](.env.example)

### Зворотна сумісність

Якщо `config.yaml` відсутній — інструмент працює з вбудованими значеннями. Якщо `.env` містить старі ключі (наприклад `FILTER_FG1_NAME`, `EXPORT_FORMAT`), вони все одно застосовуються з попередженням про перенесення у `config.yaml`.

## Формати експорту

### XLSX (Excel)
- Форматовані заголовки (колір, шрифт — налаштовується в `config.yaml`)
- Автоматична ширина колонок
- Freeze panes
- Потоковий експорт для великих даних (`xlsx.streaming: true`)

### CSV
- Налаштовуваний роздільник (`;`, `,`, tab)
- Кодування (utf-8-sig, utf-8)
- Режими цитування (minimal, all, nonnumeric)

### ZIP
- Автоматичне стиснення після експорту
- Збереження оригінальних файлів
- Статистика коефіцієнту стиснення

## Безпека

### Аутентифікація

**Windows (SSPI):**
```bash
OLAP_AUTH_METHOD=SSPI
```
Використовує Windows автентифікацію поточного користувача.

**Логін/Пароль:**
```bash
OLAP_AUTH_METHOD=LOGIN
OLAP_DOMAIN=EPICENTRK
OLAP_CREDENTIALS_ENCRYPTED=true
```

При першому запуску введіть облікові дані. Вони будуть зашифровані та збережені у файл `.credentials`. Спецсимволи у паролях (`;`, `=`, `{`, `}`) коректно обробляються.

### Шифрування

- Fernet шифрування (cryptography)
- Прив'язка до машини (комп'ютер, користувач, диск)
- Опційний майстер-пароль для додаткового захисту

```bash
OLAP_USE_MASTER_PASSWORD=true
OLAP_MASTER_PASSWORD=your_master_password
```

### Очищення облікових даних

```bash
python olap.py clear_credentials
```

## Структура результатів

```
result/
├── 2025/
│   ├── 2025-01.xlsx
│   ├── 2025-02.xlsx
│   ├── 2025-01_to_2025-12_export_20251224_100530.zip
│   └── ...
└── 2024/
    └── ...
```

Формат назв файлів:
- Один тиждень: `YYYY-WW.xlsx`
- Кілька тижнів: `YYYY-WW_to_YYYY-WW_export_TIMESTAMP.zip`

## Архітектура

```
olap-export-tool/
├── olap.py                        # Точка входу (без аргументів → меню, з аргументами → CLI)
├── .env                           # Секрети (не в git)
├── .env.example                   # Приклад секретів
├── config.yaml                    # Основна конфігурація
├── config.yaml.example            # Приклад конфігурації з описами
├── requirements.txt               # Python залежності
│
├── olap_tool/                     # Основний пакет
│   ├── core/                      # Ядро
│   │   ├── cli.py                 # Парсинг CLI аргументів
│   │   ├── config.py              # Єдина точка конфігурації (AppConfig + build_config)
│   │   ├── runner.py              # Основна логіка оркестрації
│   │   ├── periods.py             # Автоматичні періоди (7 типів)
│   │   ├── profiles.py            # Завантаження YAML профілів
│   │   ├── scheduler.py           # Планувальник задач
│   │   ├── compression.py         # ZIP стиснення
│   │   ├── progress.py            # Прогрес, таймери, анімації
│   │   └── utils.py               # Утиліти виводу та форматування
│   │
│   ├── connection/                # OLAP підключення
│   │   ├── connection.py          # ADOMD.NET / OleDb через pythonnet
│   │   ├── auth.py                # Управління обліковими даними
│   │   ├── security.py            # Шифрування (Fernet)
│   │   └── prompt.py              # Інтерактивний ввід
│   │
│   ├── data/                      # Обробка даних
│   │   ├── queries.py             # DAX запити та виконання + dispatch у sink'и
│   │   └── exporter.py            # Експорт у XLSX/CSV
│   │
│   ├── sinks/                     # Аналітичні бекенди
│   │   ├── base.py                # AnalyticsSink ABC + sanitize_df()
│   │   ├── clickhouse.py          # ClickHouseSink (clickhouse-connect)
│   │   ├── duckdb.py              # DuckDBSink (REST API)
│   │   └── postgresql.py          # PostgreSQLSink (psycopg2 COPY)
│   │
│   └── ui/                        # Консольний інтерфейс (InquirerPy + rich)
│       ├── menu.py                # Головне меню (Експорт / Імпорт / Вийти)
│       ├── olap_export.py         # Wizard експорту
│       └── xlsx_import.py         # Wizard імпорту XLSX
│
├── scripts/
│   └── import_xlsx.py             # Паралельний імпорт XLSX → будь-який sink
│
├── profiles/                      # Профілі (YAML)
│   └── weekly_sales.yaml          # Приклад: щотижневий звіт
│
├── result/                        # Результати експорту
│   └── YYYY/
│       ├── YYYY-WW.xlsx
│       └── *.zip
│
├── logs/                          # Логи планувальника
│   └── scheduler_YYYY-MM-DD.log
│
└── lib/                           # ADOMD.NET бібліотеки
    └── Microsoft.AnalysisServices.AdomdClient.dll
```

## Виправлення проблем

### Помилки підключення

**"Не вдалося підключитися до OLAP"**
- Перевірте `OLAP_SERVER` та `OLAP_DATABASE` в `.env`
- Перевірте доступність сервера (`ping 10.40.0.48`)
- Перевірте файрвол та мережеві налаштування

**"Помилка автентифікації"**
- Очистіть збережені дані: `python olap.py clear_credentials`
- Перевірте `OLAP_DOMAIN` та облікові дані
- Для SSPI перевірте права Windows користувача

### Помилки модулів

**"ModuleNotFoundError: No module named 'pythonnet'"**
```bash
pip install pythonnet>=3.0.0
```
Потрібен Python 3.8–3.13.

**"ModuleNotFoundError: No module named 'yaml'"**
```bash
pip install PyYAML>=6.0.0
```

**"ModuleNotFoundError: No module named 'clickhouse_connect'"**
```bash
pip install clickhouse-connect>=0.7.0
```

**"ModuleNotFoundError: No module named 'psycopg2'"**
```bash
pip install psycopg2-binary>=2.9.0
```

### ODBC підключення до ClickHouse

**"bad value 'strict' for attribute 'SSLMode'"**
- Відкрийте ODBC Data Sources → знайдіть DSN → змініть `SSLMode=strict` на `SSLMode=require`

### Режим налагодження

```bash
python olap.py --debug --last-weeks 1
```

Або в `config.yaml`:
```yaml
display:
  debug: true
```

## Приклади сценаріїв

### Щотижневий автоматичний звіт

1. Створіть профіль `profiles/weekly_auto.yaml`:
```yaml
name: weekly_auto
description: Автоматичний щотижневий звіт

period:
  type: auto
  auto_type: last-weeks
  auto_value: 1

export:
  format: xlsx
  compress: zip

schedule:
  enabled: true
  simple: "every monday at 06:00"
```

2. Запустіть daemon:
```bash
python olap.py --profile weekly_auto --daemon
```

### Місячний звіт

```bash
# Вручну
python olap.py --current-month --format both --compress zip

# Або через профіль
python olap.py --profile monthly_report
```

### Аналіз за рік

```bash
python olap.py --year-to-date --format xlsx --compress zip
```

### Квартальний звіт з кастомними параметрами

```bash
python olap.py --last-quarter \
  --filter "Побутова техніка" \
  --format xlsx \
  --timeout 60 \
  --compress zip
```

### Імпорт архіву XLSX у sink'и

```bash
# ClickHouse (8 воркерів)
python scripts/import_xlsx.py --target ch --dir result/ --workers 8

# PostgreSQL (тільки 2025 рік)
python scripts/import_xlsx.py --target pg --dir result/ --year 2025

# DuckDB (dry run)
python scripts/import_xlsx.py --target duck --dir result/ --dry-run
```

## Додаткова документація

- **[config.yaml.example](config.yaml.example)** — повний приклад конфігурації з описом всіх параметрів
- **[.env.example](.env.example)** — приклад секретів підключення
- **[docs/UPGRADE_GUIDE.md](docs/UPGRADE_GUIDE.md)** — детальний посібник з міграції

## Версія

**v4.0** — Консольне інтерактивне меню (InquirerPy + rich), PostgreSQL sink (psycopg2 COPY), реструктуризація пакету (`core/`, `connection/`, `data/`, `sinks/`, `ui/`), єдиний скрипт імпорту `scripts/import_xlsx.py`.

**v3.2** — DuckDB інтеграція: `DuckDBSink` (REST API), `DuckDBConfig`, паралельний імпортер.

**v3.1** — ClickHouse інтеграція: `ClickHouseSink`, паралельний імпортер з rich UI, ідемпотентна вставка, python-calamine.

**v3.0** — Єдина YAML-конфігурація, AppConfig dataclass, виправлення багів, видалення dead code.

**v2.0** — CLI, автоматичні періоди, профілі, планувальник та стиснення файлів.

---

**Дата оновлення:** 16 березня 2026
