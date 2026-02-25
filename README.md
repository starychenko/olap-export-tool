# OLAP Export Tool

Інструмент для автоматизованого експорту даних з OLAP кубів (Microsoft Analysis Services) у файли Excel та CSV з підтримкою CLI, YAML-конфігурації, профілів, планувальника та автоматичних періодів.

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
| `.env` | Секрети: сервер, БД, метод автентифікації, облікові дані |
| `config.yaml` | Все інше: запити, експорт, форматування, шляхи, відображення |
| `profiles/*.yaml` | Перевизначення будь-якої секції config.yaml для конкретного сценарію |

### config.yaml — основні секції

```yaml
query:
  filter_fg1_name: Споживча електроніка  # Фільтр категорії
  timeout: 30                            # Таймаут між запитами (сек)

export:
  format: xlsx          # xlsx, csv або both
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
OLAP_SERVER=10.40.0.48
OLAP_DATABASE=Sells
OLAP_AUTH_METHOD=LOGIN
OLAP_DOMAIN=EPICENTRK
OLAP_CREDENTIALS_ENCRYPTED=true
OLAP_CREDENTIALS_FILE=.credentials
OLAP_USE_MASTER_PASSWORD=false
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
├── olap.py                    # Точка входу
├── .env                       # Секрети (не в git)
├── .env.example               # Приклад секретів
├── config.yaml                # Основна конфігурація
├── config.yaml.example        # Приклад конфігурації з описами
├── requirements.txt           # Python залежності
│
├── olap_tool/                 # Основний пакет
│   ├── config.py              # Єдина точка конфігурації (AppConfig + build_config)
│   ├── cli.py                 # Парсинг CLI аргументів
│   ├── runner.py              # Основна логіка оркестрації
│   ├── queries.py             # DAX запити та виконання
│   ├── exporter.py            # Експорт у XLSX/CSV
│   ├── connection.py          # OLAP підключення (ADOMD.NET / OleDb)
│   ├── auth.py                # Управління обліковими даними
│   ├── security.py            # Шифрування (Fernet)
│   ├── prompt.py              # Інтерактивний ввід
│   ├── periods.py             # Автоматичні періоди (7 типів)
│   ├── profiles.py            # Завантаження YAML профілів
│   ├── scheduler.py           # Планувальник задач
│   ├── compression.py         # ZIP стиснення
│   ├── progress.py            # Прогрес, таймери, анімації
│   └── utils.py               # Утиліти виводу та форматування
│
├── profiles/                  # Профілі (YAML)
│   └── weekly_sales.yaml      # Приклад: щотижневий звіт
│
├── result/                    # Результати експорту
│   └── YYYY/
│       ├── YYYY-WW.xlsx
│       └── *.zip
│
├── logs/                      # Логи планувальника
│   └── scheduler_YYYY-MM-DD.log
│
└── lib/                       # ADOMD.NET бібліотеки
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

## Додаткова документація

- **[config.yaml.example](config.yaml.example)** — повний приклад конфігурації з описом всіх параметрів
- **[.env.example](.env.example)** — приклад секретів підключення
- **[docs/UPGRADE_GUIDE.md](docs/UPGRADE_GUIDE.md)** — детальний посібник з міграції

## Версія

**v3.0** — Єдина YAML-конфігурація, AppConfig dataclass, виправлення багів, видалення dead code.

**v2.0** — CLI, автоматичні періоди, профілі, планувальник та стиснення файлів.

---

**Дата оновлення:** 25 лютого 2026
