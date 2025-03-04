# setup_ukr.ps1
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

# Заголовок
Write-ColorOutput "===== Налаштування віртуального середовища для OLAP Export Tool =====" "Cyan"
Write-ColorOutput "Цей скрипт створить віртуальне середовище Python та встановить усі необхідні залежності." "Yellow"

# Перевірка наявності Python
try {
    $pythonVersion = python --version
    Write-ColorOutput "Знайдено $pythonVersion" "Green"
}
catch {
    Write-ColorOutput "Python не знайдено. Будь ласка, встановіть Python 3.8 або новіше." "Red"
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
        Remove-Item -Recurse -Force ".\venv"
        Write-ColorOutput "Старе віртуальне середовище видалено." "Green"
    }
    else {
        Write-ColorOutput "Використання існуючого віртуального середовища." "Green"
        
        # Перевірка, чи встановлені усі залежності
        $checkDeps = Read-Host "Бажаєте перевстановити залежності у існуючому середовищі? [Y/N]"
        if ($checkDeps -eq "Y") {
            Write-ColorOutput "Активація віртуального середовища..." "Blue"
            & .\venv\Scripts\Activate.ps1
            
            Write-ColorOutput "Встановлення залежностей..." "Blue"
            pip install -r requirements.txt
            
            Write-ColorOutput "Залежності успішно встановлено!" "Green"
            deactivate
            exit 0
        }
        else {
            Write-ColorOutput "Операцію завершено. Для активації середовища використайте: .\venv\Scripts\Activate.ps1" "Cyan"
            exit 0
        }
    }
}

# Створення віртуального середовища
Write-ColorOutput "Створення віртуального середовища Python..." "Blue"
python -m venv venv

if (-not (Test-Path -Path ".\venv")) {
    Write-ColorOutput "Не вдалося створити віртуальне середовище. Перевірте, чи встановлено пакет 'venv'." "Red"
    exit 1
}

Write-ColorOutput "Віртуальне середовище успішно створено!" "Green"

# Активація віртуального середовища та встановлення залежностей
Write-ColorOutput "Активація віртуального середовища..." "Blue"
& .\venv\Scripts\Activate.ps1

# Встановлення залежностей
Write-ColorOutput "Встановлення залежностей з requirements.txt..." "Blue"
pip install -r requirements.txt

# Перевірка наявності файлу .env
if (-not (Test-Path -Path ".\.env")) {
    if (Test-Path -Path ".\env.example") {
        Write-ColorOutput "Файл .env не знайдено. Створюю його з env.example..." "Yellow"
        Copy-Item -Path ".\env.example" -Destination ".\.env"
        Write-ColorOutput "Файл .env створено. Не забудьте налаштувати його відповідно до вашого середовища!" "Yellow"
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
    deactivate
    Write-ColorOutput "Віртуальне середовище деактивовано." "Green"
}
else {
    Write-ColorOutput "Віртуальне середовище залишається активованим." "Green"
}