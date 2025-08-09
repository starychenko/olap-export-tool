## OLAP Export Tool

Інструмент для підключення до SSAS/XMLA (OLAP), виконання DAX‑запитів і експорту результатів у XLSX/CSV. Працює на Windows з інтеграцією .NET (ADOMD.NET для SSPI і OLE DB/MSOLAP для LOGIN).

---

## Огляд
- Два способи автентифікації:
  - SSPI (Windows): через ADOMD.NET (`pyadomd`), без введення пароля.
  - LOGIN (логін/пароль): через OLE DB провайдер MSOLAP (потрібна інсталяція MSOLAP x64).
- Експорт у XLSX/CSV, пакетна обробка тижнів, кольорові логи, оцінка часу.
- Безпечне зберігання облікових даних (шифрування + посилені права доступу на Windows).

---

## Вимоги
- Windows, Python 3.8+.
- Бібліотеки Python з `requirements.txt`.
- Для SSPI: доступний ADOMD.NET (можна локально з папки `./lib`).
- Для LOGIN: встановлений MSOLAP x64 (OLE DB Provider for Analysis Services).

---

## Швидкий старт
```powershell
python -m venv venv
./venv/Scripts/Activate.ps1
python -m pip install -r requirements.txt
copy .env.example .env
python olap.py
```
Альтернатива (скрипт підготовки):
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
./setup.ps1
python olap.py
```

---

## Встановлення і запуск
- SSPI (Windows-автентифікація)
  - В `.env`: `OLAP_AUTH_METHOD=SSPI`, `ADOMD_DLL_PATH=./lib`.
  - Запуск: `python olap.py`.
- LOGIN (логін/пароль через MSOLAP)
  - В `.env`: `OLAP_AUTH_METHOD=LOGIN`.
  - Потрібен встановлений MSOLAP x64.
  - Запуск: `python olap.py`.
- Видалити збережені облікові дані: `python olap.py clear_credentials`.

Порада (з дому під доменним користувачем у режимі SSPI):
```powershell
runas /netonly /user:DOMAIN\user "C:\Path\to\venv\Scripts\python.exe C:\GIT\olap-export-tool\olap.py"
```

---

## Архітектура
- `olap.py` — вхідна точка (завантажує `.env`, викликає раннер).
- `olap_tool/` пакет:
  - `runner.py` — головний сценарій (параметри, цикл періодів, підсумок).
  - `connection.py` — ініціалізація .NET; конекти: SSPI → ADOMD.NET, LOGIN → OleDb (MSOLAP).
  - `queries.py` — DAX, перетворення .NET → Python, виклики експорту.
  - `exporter.py` — експорт CSV/XLSX (потоковий/через DataFrame).
  - `progress.py` — спінер, TimeTracker, зворотний відлік.
  - `auth.py` — збереження/завантаження/видалення облікових даних.
  - `security.py` — machine‑id, ключі, шифрування, ACL/права файлів.
  - `prompt.py` — інтерактивний ввід логіну/пароля.
  - `utils.py` — логи, утиліти, форматування часу.

---

## Конфігурація (.env)
Найчастіші параметри (мінімально достатній набір):
```ini
# Сервер і база
OLAP_SERVER=10.40.0.48
OLAP_DATABASE=Sells

# Метод автентифікації: SSPI (Windows) або LOGIN (логін/пароль)
OLAP_AUTH_METHOD=SSPI

# ADOMD.NET (для SSPI). Використайте локальну DLL з папки lib
ADOMD_DLL_PATH=./lib

# Запити/періоди
FILTER_FG1_NAME=Споживча електроніка
YEAR_WEEK_START=2025-27
YEAR_WEEK_END=2025-32
QUERY_TIMEOUT=5
DEBUG=false

# Експорт
EXPORT_FORMAT=XLSX      # XLSX | CSV | BOTH
FORCE_CSV_ONLY=false    # true → лише CSV
XLSX_STREAMING=false    # true → швидший XLSX без важкого форматування

# CSV
CSV_DELIMITER=;
CSV_ENCODING=utf-8-sig
CSV_QUOTING=minimal     # all | minimal | nonnumeric

# Збереження облікових даних (для LOGIN)
OLAP_CREDENTIALS_ENCRYPTED=true
OLAP_CREDENTIALS_FILE=.credentials
OLAP_USE_MASTER_PASSWORD=false
# OLAP_MASTER_PASSWORD=your_master_password

# Домен для зручного формування повного логіну у промпті (LOGIN)
OLAP_DOMAIN=EPICENTRK
```

Повний перелік із поясненнями:

### Параметри підключення
- `OLAP_SERVER` (host/IP): адреса SSAS/XMLA.
- `OLAP_DATABASE` (string): назва бази (Initial Catalog).

### Автентифікація
- `OLAP_AUTH_METHOD` (`SSPI` | `LOGIN`): режим автентифікації.
- `ADOMD_DLL_PATH` (path): шлях до `Microsoft.AnalysisServices.AdomdClient.dll` (обов’язково для SSPI).
- `OLAP_DOMAIN` (string): домен для формування `DOMAIN\user` у промпті (для LOGIN).

Примітка щодо нерелевантних для поточної реалізації змінних із прикладу:
- `OLAP_PORT`, `OLAP_HTTP_URL`, `OLAP_TIMEOUT` — задокументовані в `.env.example` для майбутніх сценаріїв, але зараз не використовуються кодом.

### Експорт і продуктивність
- `EXPORT_FORMAT` (`XLSX` | `CSV` | `BOTH`): обраний формат експорту.
- `FORCE_CSV_ONLY` (bool): форсувати тільки CSV (ігнорувати XLSX навіть у BOTH).
- `XLSX_STREAMING` (bool): пришвидшений запис XLSX рядками (мінімум форматування).
- `CSV_DELIMITER` (символ): роздільник CSV (`,`, `;`, `\t`).
- `CSV_ENCODING` (string): кодування CSV (рекомендовано `utf-8-sig`).
- `CSV_QUOTING` (`all` | `minimal` | `nonnumeric`): політика лапок у CSV.
- `EXCEL_HEADER_COLOR` (hex без `#`): колір фону заголовків у XLSX.
- `EXCEL_HEADER_FONT_COLOR` (hex без `#`): колір шрифту заголовків у XLSX.
- `EXCEL_HEADER_FONT_SIZE` (int): розмір шрифту заголовків у XLSX.

### Параметри запитів
- `FILTER_FG1_NAME` (string): фільтр категорії для DAX.
- `YEAR_WEEK_START` / `YEAR_WEEK_END` (`YYYY-WW`): діапазон тижнів.
- `QUERY_TIMEOUT` (int, сек): пауза між запитами, щоб зменшити навантаження на сервер.
- `DEBUG` (bool): розширений діагностичний вивід під час розрахунку часу/прогресу.

### Безпека і зберігання облікових даних (LOGIN)
- `OLAP_CREDENTIALS_ENCRYPTED` (bool): шифрувати збережені креденшіали (Fernet).
- `OLAP_CREDENTIALS_FILE` (path): файл для зберігання облікових даних.
- `OLAP_USE_MASTER_PASSWORD` (bool): підсилити ключ шифрування майстер‑паролем.
- `OLAP_MASTER_PASSWORD` (string): майстер‑пароль (якщо не вказаний — може бути запитаний інтерактивно у TTY).

Механіка:
- Креденшіали зберігаються лише після успішного логіну (LOGIN).
- Ключ шифрування формується з machine‑id (+ майстер‑пароль, якщо увімкнено).
- Windows: права на файл обмежуються через `icacls`; інші ОС: `chmod 600`.
- Видалення збережених облікових даних: `python olap.py clear_credentials`.

---

## Робота з інструментом
- Запуск із параметрами з `.env`: `python olap.py`.
- Підсумок: файли у `result/YYYY/YYYY-WW.xlsx|csv`.
- Очистити облікові дані: `python olap.py clear_credentials`.

---

## Типові помилки та діагностика
- «The 'MSOLAP' provider is not registered on the local machine» (LOGIN):
  - Встановіть MSOLAP x64 (OLE DB Provider for Analysis Services).
- «Pyadomd/ADOMD.NET недоступні» (SSPI):
  - Перевірте `ADOMD_DLL_PATH` і наявність `Microsoft.AnalysisServices.AdomdClient.dll` у вказаній директорії.
- Помилки мережі (ADOMD):
  - Сервер/порт недоступні (VPN/фаєрвол/ACL). Перевірте `OLAP_SERVER` і доступність з вашої мережі.
- Помилки логіну (LOGIN):
  - Перевірте форму `DOMAIN\user`. У промпті домен підставляється з `OLAP_DOMAIN` (якщо вказано).

---

## Структура проєкту
```
olap-export-tool/
├─ olap.py                      # Вхідна точка
├─ setup.ps1                    # Автоматичне налаштування
├─ requirements.txt             # Залежності Python
├─ .env / .env.example          # Конфігурація
├─ lib/
│  └─ Microsoft.AnalysisServices.AdomdClient.dll
├─ olap_tool/
│  ├─ __init__.py
│  ├─ runner.py
│  ├─ connection.py
│  ├─ queries.py
│  ├─ exporter.py
│  ├─ progress.py
│  ├─ auth.py
│  ├─ security.py
│  ├─ prompt.py
│  └─ utils.py
└─ result/
   └─ YYYY/
      ├─ YYYY-WW.xlsx
      └─ YYYY-WW.csv
```

---

## Ліцензія
MIT. Див. `LICENSE`.


