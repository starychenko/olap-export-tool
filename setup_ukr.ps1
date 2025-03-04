# setup_ukr_final.ps1
# Скрипт для автоматизації створення віртуального середовища Python та встановлення залежностей
# Автор: Claude AI
# Дата: 2023

# Функція для виведення кольорового тексту
function Write-ColorOutput {
    param (
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

# Налаштування строгої обробки помилок
$ErrorActionPreference = "Stop"

# Заголовок
Write-ColorOutput "===== Налаштування віртуального середовища для OLAP Export Tool =====" "Cyan"
Write-ColorOutput "Цей скрипт створить віртуальне середовище Python та встановить усі необхідні залежності." "Yellow"

# Перевірка наявності Python з покращеною обробкою помилок
try {
    $pythonVersion = python --version
    Write-ColorOutput "Знайдено $pythonVersion" "Green"
}
catch {
    Write-ColorOutput "Python не знайдено або неправильно налаштовано. Будь ласка, встановіть Python 3.8 або новіше." "Red"
    Write-ColorOutput "Деталі помилки: $_" "Red"
    exit 1
}

# Перевірка наявності файлу requirements.txt
if (-not (Test-Path -Path ".\requirements.txt")) {
    Write-ColorOutput "Помилка: файл requirements.txt не знайдено в поточній директорії." "Red"
    Write-ColorOutput "Будь ласка, переконайтеся, що ви знаходитесь у правильній директорії проекту або створіть файл requirements.txt." "Red"
    exit 1
}

# Перевірка існування віртуального середовища
if (Test-Path -Path ".\venv") {
    Write-ColorOutput "Виявлено існуюче віртуальне середовище." "Yellow"
    $confirmation = Read-Host "Бажаєте використати існуюче середовище (Y), видалити та створити нове (R), або скасувати операцію (N)? [Y/R/N]"
    
    if ($confirmation -eq "N") {
        Write-ColorOutput "Операцію скасовано." "Red"
        exit 0
    }
    elseif ($confirmation -eq "R") {
        Write-ColorOutput "Видалення існуючого віртуального середовища..." "Yellow"
        try {
            # Переконуємося, що жоден процес не використовує директорію
            if (Get-Process | Where-Object { $_.Path -like "*\venv\*" }) {
                Write-ColorOutput "Увага: Деякі процеси використовують файли у віртуальному середовищі." "Red"
                Write-ColorOutput "Будь ласка, закрийте всі програми, які використовують ці файли, і спробуйте знову." "Red"
                exit 1
            }
            Remove-Item -Recurse -Force ".\venv"
            Write-ColorOutput "Старе віртуальне середовище видалено." "Green"
        }
        catch {
            Write-ColorOutput "Помилка видалення віртуального середовища: $_" "Red"
            Write-ColorOutput "Будь ласка, закрийте всі програми, які використовують ці файли, і спробуйте знову." "Red"
            exit 1
        }
    }
    else {
        Write-ColorOutput "Використання існуючого віртуального середовища." "Green"
        
        # Перевірка, чи встановлені усі залежності
        $checkDeps = Read-Host "Бажаєте перевстановити залежності у існуючому середовищі? [Y/N]"
        if ($checkDeps -eq "Y") {
            Write-ColorOutput "Активація віртуального середовища..." "Blue"
            try {
                # Активуємо віртуальне середовище
                & .\venv\Scripts\Activate.ps1
                
                # Перевіряємо, чи активація пройшла успішно
                if (-not $env:VIRTUAL_ENV) {
                    throw "Не вдалося активувати віртуальне середовище"
                }
                
                Write-ColorOutput "Встановлення залежностей..." "Blue"
                # Встановлюємо залежності з обробкою помилок
                $pipResult = pip install -r requirements.txt
                if ($LASTEXITCODE -ne 0) {
                    throw "Не вдалося встановити залежності"
                }
                
                Write-ColorOutput "Залежності успішно встановлено!" "Green"
                
                # Деактивуємо середовище, якщо функція існує
                if (Get-Command deactivate -ErrorAction SilentlyContinue) {
                    deactivate
                } else {
                    # Альтернативний спосіб деактивації, якщо функція недоступна
                    Write-ColorOutput "Примітка: Стандартна функція deactivate не знайдена, очищуємо змінні середовища..." "Yellow"
                    $env:VIRTUAL_ENV = $null
                    # Видаляємо директорію Scripts віртуального середовища з PATH
                    $env:PATH = ($env:PATH -split ';' | Where-Object { $_ -notlike "*\venv\Scripts*" }) -join ';'
                }
                
                exit 0
            }
            catch {
                Write-ColorOutput "Помилка: $_" "Red"
                exit 1
            }
        }
        else {
            Write-ColorOutput "Операцію завершено. Для активації середовища використайте: .\venv\Scripts\Activate.ps1" "Cyan"
            exit 0
        }
    }
}

# Створення віртуального середовища
Write-ColorOutput "Створення віртуального середовища Python..." "Blue"
try {
    python -m venv venv

    if (-not (Test-Path -Path ".\venv")) {
        throw "Директорія віртуального середовища не створена"
    }
}
catch {
    Write-ColorOutput "Не вдалося створити віртуальне середовище." "Red"
    Write-ColorOutput "Деталі помилки: $_" "Red"
    Write-ColorOutput "Перевірте, чи встановлено пакет 'venv'. Спробуйте: python -m pip install virtualenv" "Yellow"
    exit 1
}

Write-ColorOutput "Віртуальне середовище успішно створено!" "Green"

# Активація віртуального середовища та встановлення залежностей
Write-ColorOutput "Активація віртуального середовища..." "Blue"
try {
    # Активуємо віртуальне середовище
    & .\venv\Scripts\Activate.ps1
    
    # Перевіряємо, чи активація пройшла успішно
    if (-not $env:VIRTUAL_ENV) {
        throw "Не вдалося активувати віртуальне середовище"
    }
    
    # Встановлюємо залежності
    Write-ColorOutput "Встановлення залежностей з requirements.txt..." "Blue"
    $pipResult = pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) {
        throw "Не вдалося встановити залежності"
    }
}
catch {
    Write-ColorOutput "Помилка: $_" "Red"
    exit 1
}

# Перевірка наявності файлу .env
if (-not (Test-Path -Path ".\.env")) {
    if (Test-Path -Path ".\env.example") {
        Write-ColorOutput "Файл .env не знайдено. Створюю його з env.example..." "Yellow"
        try {
            Copy-Item -Path ".\env.example" -Destination ".\.env"
            Write-ColorOutput "Файл .env створено. Не забудьте налаштувати його відповідно до вашого середовища!" "Yellow"
        }
        catch {
            Write-ColorOutput "Помилка створення файлу .env: $_" "Red"
        }
    }
    else {
        Write-ColorOutput "Увага: Файли .env та env.example не знайдено. Вам потрібно створити файл .env вручну." "Red"
    }
}

# Завершення
Write-ColorOutput "===== Налаштування завершено! =====" "Cyan"
Write-ColorOutput "Віртуальне середовище створено та активовано." "Green"
Write-ColorOutput "Усі залежності встановлено." "Green"
Write-ColorOutput "" "White"
Write-ColorOutput "Для активації середовища використовуйте: .\venv\Scripts\Activate.ps1" "Yellow"
Write-ColorOutput "Для деактивації середовища використовуйте команду: deactivate" "Yellow"

# Питаємо, чи користувач хоче залишити середовище активованим
$keepActive = Read-Host "Бажаєте залишити віртуальне середовище активованим? [Y/N]"
if ($keepActive -ne "Y") {
    Write-ColorOutput "Деактивація віртуального середовища..." "Blue"
    # Деактивуємо середовище, якщо функція існує
    if (Get-Command deactivate -ErrorAction SilentlyContinue) {
        deactivate
    } else {
        # Альтернативний спосіб деактивації, якщо функція недоступна
        Write-ColorOutput "Примітка: Стандартна функція deactivate не знайдена, очищуємо змінні середовища..." "Yellow"
        $env:VIRTUAL_ENV = $null
        # Видаляємо директорію Scripts віртуального середовища з PATH
        $env:PATH = ($env:PATH -split ';' | Where-Object { $_ -notlike "*\venv\Scripts*" }) -join ';'
    }
    Write-ColorOutput "Віртуальне середовище деактивовано." "Green"
}
else {
    Write-ColorOutput "Віртуальне середовище залишається активованим." "Green"
} 