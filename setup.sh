#!/bin/bash
# setup.sh
# Скрипт для автоматизації створення віртуального середовища Python та встановлення залежностей
# для Linux/MacOS систем
# Автор: Claude AI
# Дата: 2023

# Кольори для виведення
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Функція для виведення повідомлень
print_message() {
    local message="$1"
    local color="$2"
    echo -e "${color}${message}${NC}"
}

# Заголовок
print_message "===== Налаштування віртуального середовища для OLAP Export Tool =====" "$CYAN"
print_message "Цей скрипт створить віртуальне середовище Python та встановить усі необхідні залежності." "$YELLOW"

# Перевірка наявності Python
if command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    print_message "Python не знайдено. Будь ласка, встановіть Python 3.8 або новіше." "$RED"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD --version)
print_message "Знайдено $PYTHON_VERSION" "$GREEN"

# Перевірка існування віртуального середовища
if [ -d "venv" ]; then
    print_message "Виявлено існуюче віртуальне середовище." "$YELLOW"
    read -p "Бажаєте використати існуюче середовище (Y), видалити та створити нове (R), або скасувати операцію (N)? [Y/R/N] " confirmation
    
    if [ "$confirmation" = "N" ] || [ "$confirmation" = "n" ]; then
        print_message "Операцію скасовано." "$RED"
        exit 0
    elif [ "$confirmation" = "R" ] || [ "$confirmation" = "r" ]; then
        print_message "Видалення існуючого віртуального середовища..." "$YELLOW"
        rm -rf venv
        print_message "Старе віртуальне середовище видалено." "$GREEN"
    else
        print_message "Використання існуючого віртуального середовища." "$GREEN"
        
        # Перевірка, чи встановлені усі залежності
        read -p "Бажаєте перевстановити залежності у існуючому середовищі? [Y/N] " checkDeps
        if [ "$checkDeps" = "Y" ] || [ "$checkDeps" = "y" ]; then
            print_message "Активація віртуального середовища..." "$BLUE"
            source venv/bin/activate
            
            print_message "Встановлення залежностей..." "$BLUE"
            pip install -r requirements.txt
            
            print_message "Залежності успішно встановлено!" "$GREEN"
            deactivate
            exit 0
        else
            print_message "Операцію завершено. Для активації середовища використайте: source venv/bin/activate" "$CYAN"
            exit 0
        fi
    fi
fi

# Створення віртуального середовища
print_message "Створення віртуального середовища Python..." "$BLUE"
$PYTHON_CMD -m venv venv

if [ ! -d "venv" ]; then
    print_message "Не вдалося створити віртуальне середовище. Перевірте, чи встановлено пакет 'venv'." "$RED"
    print_message "Спробуйте: $PYTHON_CMD -m pip install virtualenv" "$YELLOW"
    exit 1
fi

print_message "Віртуальне середовище успішно створено!" "$GREEN"

# Активація віртуального середовища та встановлення залежностей
print_message "Активація віртуального середовища..." "$BLUE"
source venv/bin/activate

# Встановлення залежностей
print_message "Встановлення залежностей з requirements.txt..." "$BLUE"
pip install -r requirements.txt

# Перевірка наявності файлу .env
if [ ! -f ".env" ]; then
    if [ -f "env.example" ]; then
        print_message "Файл .env не знайдено. Створюю його з env.example..." "$YELLOW"
        cp env.example .env
        print_message "Файл .env створено. Не забудьте налаштувати його відповідно до вашого середовища!" "$YELLOW"
    else
        print_message "Увага: Файли .env та env.example не знайдено. Вам потрібно створити файл .env вручну." "$RED"
    fi
fi

# Завершення
print_message "===== Налаштування завершено! =====" "$CYAN"
print_message "Віртуальне середовище створено та активовано." "$GREEN"
print_message "Усі залежності встановлено." "$GREEN"
print_message "" "$NC"
print_message "Для активації середовища використовуйте: source venv/bin/activate" "$YELLOW"
print_message "Для деактивації середовища використовуйте команду: deactivate" "$YELLOW"

# Питаємо, чи користувач хоче залишити середовище активованим
read -p "Бажаєте залишити віртуальне середовище активованим? [Y/N] " keepActive
if [ "$keepActive" != "Y" ] && [ "$keepActive" != "y" ]; then
    print_message "Деактивація віртуального середовища..." "$BLUE"
    deactivate
    print_message "Віртуальне середовище деактивовано." "$GREEN"
else
    print_message "Віртуальне середовище залишається активованим." "$GREEN"
fi 