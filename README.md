# OLAP Export Tool

Інструмент для автоматизованого експорту даних з OLAP кубів (Microsoft Analysis Services) у файли Excel та CSV з підтримкою CLI, YAML-конфігурації, профілів, планувальника, автоматичних періодів та завантаження у ClickHouse.

## Основні можливості

- **Єдина YAML-конфігурація** — всі налаштування в `config.yaml`, секрети окремо в `.env`
- **CLI аргументи** — гнучке управління через командний рядок
- **Автоматичні періоди** — 7 варіантів розумних періодів (останні N тижнів, поточний місяць, квартал і т.д.)
- **Профілі** — збереження наборів налаштувань для різних сценаріїв (перевизначають будь-яку секцію конфігу)
- **Планувальник** — автоматичне виконання експортів за розкладом
- **Стиснення** — ZIP архівація результатів
- **Два методи аутентифікації** — Windows (SSPI) та Логін/Пароль
- **Шифрування облікових даних** — безпечне зберігання паролів з прив'язкою до машини
- **Потоковий експорт** — ефективна робота з великими наборами даних
- **ClickHouse інтеграція** — завантаження експортованих даних у ClickHouse з паралельним імпортом

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
- `clickhouse-connect` — завантаження даних у ClickHouse
- `python-calamine` — швидке читання Excel (Rust, 3-10x швидше за openpyxl)
- `rich` — інтерактивний термінальний UI (progress bar, панелі, таблиці)

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
# Без аргументів — використовує config.yaml + .env
python olap.py

# При першому запуску з LOGIN методом введіть облікові дані —
# вони будуть зашифровані та збережені автоматично
```

## Використання

### Базові команди

```bash
# Допомога
python olap.py --help

# Експорт з config.yaml налаштуваннями
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
# Формат: xlsx, csv або both
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

## ClickHouse інтеграція

### Опис

Інструмент підтримує завантаження експортованих XLSX-файлів у ClickHouse. Модуль `olap_tool/clickhouse_export.py` надає повний набір функцій для роботи з ClickHouse, а скрипт `import_xlsx_to_clickhouse.py` забезпечує паралельний пакетний імпорт.

**Ключові особливості:**
- Ідемпотентна вставка — перед кожним INSERT видаляє дані за `year_num + week_num`
- Автоматичне створення БД та таблиці зі схемою з DataFrame
- Schema evolution — автоматично додає нові колонки через `ALTER TABLE`
- Thread-local клієнти — одне з'єднання на потік без перевідкриття
- Кешована схема таблиці — один запит `system.columns` на весь batch
- Читання Excel через python-calamine (Rust, 3-10x швидше за openpyxl)

### Налаштування

Додайте в `.env` параметри ClickHouse:

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

### Пакетний імпорт XLSX → ClickHouse

```bash
# Імпорт всіх файлів з директорії result/ (4 паралельних воркери)
python import_xlsx_to_clickhouse.py

# 8 паралельних воркерів
python import_xlsx_to_clickhouse.py --workers 8

# Тільки 2025 рік
python import_xlsx_to_clickhouse.py --year 2025

# Конкретний тиждень
python import_xlsx_to_clickhouse.py --year 2025 --week 10

# Показати файли без завантаження (dry run)
python import_xlsx_to_clickhouse.py --dry-run
```

Скрипт відображає прогрес у реальному часі:
```
✅ 2025-44    18,486 рядків  0.8с
✅ 2025-43   143,920 рядків  1.2с
✅ 2025-42   139,810 рядків  1.1с
```

### Пряме завантаження під час експорту

Для завантаження у ClickHouse разом з XLSX-експортом налаштуйте в `config.yaml`:

```yaml
clickhouse:
  enabled: true
```

Або через CLI:
```bash
python olap.py --last-weeks 4 --format ch
```

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
| `.env` | Секрети: OLAP сервер/БД, автентифікація, ClickHouse підключення |
| `config.yaml` | Все інше: запити, експорт, форматування, шляхи, відображення |
| `profiles/*.yaml` | Перевизначення будь-якої секції config.yaml для конкретного сценарію |

### config.yaml — основні секції

```yaml
query:
  filter_fg1_name: Споживча електроніка  # Фільтр категорії
  timeout: 30                            # Таймаут між запитами (сек)

export:
  format: xlsx          # xlsx, csv, both або ch (ClickHouse)
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

# ClickHouse підключення
CH_ENABLED=true
CH_HOST=your-clickhouse-host
CH_PORT=8443
CH_USERNAME=default
CH_PASSWORD=your_password
CH_SECURE=true
CH_DATABASE=olap_export
CH_TABLE=sales
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

### ClickHouse
- Прямий INSERT через clickhouse-connect з LZ4 стисненням
- Ідемпотентна вставка (DELETE + INSERT за тижнем)
- Автоматичне створення схеми та schema evolution

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
├── olap.py                        # Точка входу (OLAP експорт)
├── import_xlsx_to_clickhouse.py   # Паралельний імпорт XLSX → ClickHouse
├── .env                           # Секрети (не в git)
├── .env.example                   # Приклад секретів
├── config.yaml                    # Основна конфігурація
├── config.yaml.example            # Приклад конфігурації з описами
├── requirements.txt               # Python залежності
│
├── olap_tool/                     # Основний пакет
│   ├── config.py                  # Єдина точка конфігурації (AppConfig + build_config)
│   ├── cli.py                     # Парсинг CLI аргументів
│   ├── runner.py                  # Основна логіка оркестрації
│   ├── queries.py                 # DAX запити та виконання
│   ├── exporter.py                # Експорт у XLSX/CSV
│   ├── clickhouse_export.py       # Завантаження DataFrame у ClickHouse
│   ├── connection.py              # OLAP підключення (ADOMD.NET / OleDb)
│   ├── auth.py                    # Управління обліковими даними
│   ├── security.py                # Шифрування (Fernet)
│   ├── prompt.py                  # Інтерактивний ввід
│   ├── periods.py                 # Автоматичні періоди (7 типів)
│   ├── profiles.py                # Завантаження YAML профілів
│   ├── scheduler.py               # Планувальник задач
│   ├── compression.py             # ZIP стиснення
│   ├── progress.py                # Прогрес, таймери, анімації
│   └── utils.py                   # Утиліти виводу та форматування
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

### Імпорт архіву Excel у ClickHouse

```bash
# Повний архів (8 воркерів)
python import_xlsx_to_clickhouse.py --workers 8

# Тільки поточний рік
python import_xlsx_to_clickhouse.py --year 2025 --workers 4
```

## Додаткова документація

- **[config.yaml.example](config.yaml.example)** — повний приклад конфігурації з описом всіх параметрів
- **[.env.example](.env.example)** — приклад секретів підключення
- **[docs/UPGRADE_GUIDE.md](docs/UPGRADE_GUIDE.md)** — детальний посібник з міграції

## Версія

**v3.1** — ClickHouse інтеграція: новий модуль `clickhouse_export.py`, паралельний імпортер `import_xlsx_to_clickhouse.py` з rich UI, ідемпотентна вставка (year_num/week_num), python-calamine.

**v3.0** — Єдина YAML-конфігурація, AppConfig dataclass, виправлення багів, видалення dead code.

**v2.0** — CLI, автоматичні періоди, профілі, планувальник та стиснення файлів.

---

**Дата оновлення:** 6 березня 2026
