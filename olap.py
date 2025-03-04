import os
import sys
import clr
import re
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv
import time
import datetime
from colorama import init, Fore, Back, Style
import threading
import itertools

# Ініціалізуємо colorama для кольорового виводу в консоль
init(autoreset=True)

# Завантажуємо змінні середовища з .env файлу
load_dotenv()

# Глобальні змінні для керування анімацією
animation_running = False
avg_query_time = None  # Середній час виконання запиту (ініціалізується при першому вимірі)

# Додаємо шлях до Microsoft.AnalysisServices.AdomdClient.dll з .env
adomd_dll_path = os.getenv('ADOMD_DLL_PATH')
sys.path.append(adomd_dll_path)
clr.AddReference('Microsoft.AnalysisServices.AdomdClient')

from pyadomd import Pyadomd
import pandas as pd

# Визначаємо поточний рік та тиждень для значень за замовчуванням
CURRENT_YEAR = datetime.datetime.now().year
CURRENT_WEEK = datetime.datetime.now().isocalendar()[1]  # Поточний номер тижня

# Функція для виводу часу
def get_current_time():
    return datetime.datetime.now().strftime('%H:%M:%S')

# Функція для виводу заголовків
def print_header(text):
    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 80}")
    print(f"{Fore.CYAN}{Style.BRIGHT}== {text}")
    print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 80}")
    print() # Додаємо порожній рядок для кращої читабельності

# Функція для виводу інформаційних повідомлень
def print_info(text):
    print(f"{Fore.GREEN}[{get_current_time()}] ℹ️ {text}")

# Функція для виводу попереджень
def print_warning(text):
    print(f"{Fore.YELLOW}[{get_current_time()}] ⚠️ {text}")

# Функція для виводу помилок
def print_error(text):
    print(f"{Fore.RED}[{get_current_time()}] ❌ {text}")

# Функція для виводу успішних операцій
def print_success(text):
    print(f"{Fore.GREEN}[{get_current_time()}] ✅ {text}")

# Функція для виводу прогресу
def print_progress(text):
    print(f"{Fore.BLUE}[{get_current_time()}] 🔄 {text}")

# Функція для форматування часу у вигляді години:хвилини:секунди
def format_time(seconds):
    """Форматує час у секундах до читабельного формату (години, хвилини, секунди)"""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        return f"{int(hours)} год {int(minutes)} хв {seconds:.2f} сек"
    elif minutes > 0:
        return f"{int(minutes)} хв {seconds:.2f} сек"
    else:
        return f"{seconds:.2f} сек"

# Клас для відстеження прогресу та часу виконання завдання
class TimeTracker:
    """Клас для відстеження часу виконання та прогнозування завершення"""
    def __init__(self, total_items):
        self.total_items = total_items
        self.start_time = time.time()
        self.processed_items = 0
        self.elapsed_times = []  # Зберігаємо час обробки кожного елемента
    
    def update(self, items_processed=1):
        """Оновлює статус обробки після завершення елемента"""
        current_time = time.time()
        # Якщо це не перший елемент (для першого не можемо розрахувати час обробки)
        if self.processed_items > 0:  
            time_for_last_item = current_time - (self.start_time + sum(self.elapsed_times))
            self.elapsed_times.append(time_for_last_item)
        else:
            # Для першого елемента просто зберігаємо час від початку
            time_for_last_item = current_time - self.start_time
            self.elapsed_times.append(time_for_last_item)
        
        self.processed_items += items_processed
    
    def get_elapsed_time(self):
        """Повертає час, що минув з початку обробки"""
        return time.time() - self.start_time
    
    def get_remaining_time(self):
        """Прогнозує час, що залишився до завершення"""
        if not self.elapsed_times or self.processed_items == 0:
            return None  # Не можемо спрогнозувати без даних
        
        # Середній час на обробку одного елемента, виключаючи аномалії
        avg_time_per_item = sum(self.elapsed_times) / len(self.elapsed_times)
        
        # Кількість елементів, що залишилося обробити
        remaining_items = self.total_items - self.processed_items
        
        # Прогноз часу, що залишився
        return avg_time_per_item * remaining_items
    
    def get_total_time(self):
        """Прогнозує загальний час на виконання"""
        remaining = self.get_remaining_time()
        if remaining is None:
            return self.get_elapsed_time()  # Повертаємо лише час, що пройшов
        return self.get_elapsed_time() + remaining
    
    def get_percentage_complete(self):
        """Повертає відсоток виконання завдання"""
        return (self.processed_items / self.total_items) * 100 if self.total_items > 0 else 0
    
    def get_progress_info(self):
        """Повертає інформацію про прогрес у зручному форматі"""
        elapsed = self.get_elapsed_time()
        remaining = self.get_remaining_time()
        total = self.get_total_time()
        percentage = self.get_percentage_complete()
        
        info = f"Прогрес: {percentage:.1f}% ({self.processed_items}/{self.total_items})\n"
        info += f"Минуло: {format_time(elapsed)}"
        
        if remaining is not None:
            info += f" | Залишилось: {format_time(remaining)}"
            info += f" | Всього: {format_time(total)}"
        
        return info

# Функція для анімованого індикатора завантаження
def loading_spinner(description, estimated_time=None):
    """Функція для відображення анімованого індикатора завантаження"""
    global animation_running
    animation_running = True
    
    # Символи для анімації
    spinner = itertools.cycle(['⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷'])
    
    # Початковий час для відображення тривалості
    start_time = time.time()
    
    # Відображаємо анімацію поки вона активна
    while animation_running:
        elapsed = time.time() - start_time
        # Використовуємо нашу функцію format_time для форматування часу
        elapsed_str = format_time(elapsed)
        
        # Базовий рядок з інформацією
        message = f"{Fore.BLUE}[{get_current_time()}] {next(spinner)} {description}"
        
        # Додаємо інформацію про час
        message += f" | Минуло: {elapsed_str}"
        
        # Якщо є оцінка часу, додаємо її
        if estimated_time is not None:
            # Розраховуємо, скільки часу залишилось (з обмеженням знизу на 0)
            remaining = max(0, estimated_time - elapsed)
            # Додаємо інформацію про залишковий та загальний час
            message += f" | Залишилось: {format_time(remaining)}"
            message += f" | Всього: {format_time(estimated_time)}"
        
        sys.stdout.write(f"\r{message}")
        sys.stdout.flush()
        time.sleep(0.1)
    
    # Очищаємо останній рядок анімації (використовуємо довжину останнього повідомлення)
    sys.stdout.write("\r" + " " * len(message) + "\r")
    sys.stdout.flush()
    # Додаємо новий рядок для відокремлення від наступного повідомлення
    print()

# Функція для генерації переліку тижнів за періодом
def generate_year_week_pairs(start_period, end_period, available_weeks):
    """Генерує список пар (рік, тиждень) в заданому діапазоні, враховуючи доступні тижні у кубі"""
    # Парсимо початковий і кінцевий періоди (формат РРРР-ТТ)
    try:
        start_year, start_week = map(int, start_period.split('-'))
        end_year, end_week = map(int, end_period.split('-'))
    except (ValueError, AttributeError):
        print_error(f"Невірний формат періодів. Використовуйте формат РРРР-ТТ")
        return []
    
    # Перевіряємо коректність введених даних
    current_year = datetime.datetime.now().year
    min_year = current_year - 3
    max_year = current_year

    if start_year < min_year or end_year > max_year:
        print_error(f"Невірні значення року (має бути між {min_year} та {max_year})")
        return []
    
    if start_year > end_year or (start_year == end_year and start_week > end_week):
        print_error(f"Початковий період має бути раніше за кінцевий")
        return []
    
    # Створюємо словник доступних тижнів для швидкого пошуку
    available_dict = {(year, week): True for year, week in available_weeks}
    
    # Фільтруємо за доступними тижнями
    filtered_pairs = []
    
    # Генеруємо всі потенційні пари
    all_pairs = []
    current_year = start_year
    current_week = start_week
    
    while current_year < end_year or (current_year == end_year and current_week <= end_week):
        all_pairs.append((current_year, current_week))
        current_week += 1
        # Якщо перейшли до наступного року
        if current_week > 53:  # Використовуємо 53 як максимальне значення тижня
            current_week = 0   # Починаємо з тижня 0, якщо він існує
            current_year += 1
    
    # Фільтруємо пари за наявністю в кубі
    for year, week in all_pairs:
        if (year, week) in available_dict:
            filtered_pairs.append((year, week))
    
    if len(filtered_pairs) == 0:
        print_warning(f"Не знайдено доступних тижнів у вказаному діапазоні")
    else:
        print_info(f"Знайдено {len(filtered_pairs)} тижнів у вказаному діапазоні")
    
    return filtered_pairs

# Функція для отримання рядка підключення до OLAP
def get_connection_string():
    """Повертає рядок підключення до OLAP сервера на основі налаштувань з .env"""
    return (
        "Provider=MSOLAP;"
        f"Data Source={os.getenv('OLAP_SERVER')};" 
        f"Initial Catalog={os.getenv('OLAP_DATABASE')};" 
        "Integrated Security=SSPI;"
    )

# Функція для підключення до OLAP сервера
def connect_to_olap(connection_string=None):
    """Підключається до OLAP сервера і повертає з'єднання"""
    if connection_string is None:
        connection_string = get_connection_string()
    
    try:
        print_info(f"Підключення до OLAP сервера {os.getenv('OLAP_SERVER')}...")
        connection = Pyadomd(connection_string)
        connection.open()
        print_success(f"Підключення до OLAP сервера встановлено")
        return connection
    except Exception as e:
        print_error(f"Помилка підключення до OLAP сервера: {e}")
        return None

# Функція для виконання MDX-запиту і отримання результатів
def run_mdx_query(connection, reporting_period):
    """Виконує MDX-запит для заданого періоду і повертає результати"""
    # Парсимо період (формат РРРР-ТТ)
    try:
        year_num, week_num = map(int, reporting_period.split('-'))
    except (ValueError, AttributeError):
        print_error(f"Невірний формат періоду: {reporting_period}. Використовуйте формат РРРР-ТТ")
        return []
    
    # Отримуємо фільтр для запиту
    filter_fg1_name = os.getenv('FILTER_FG1_NAME')
    
    # Формуємо шлях для збереження результатів
    result_dir = "result"
    year_dir = os.path.join(result_dir, str(year_num))
    
    # Перевіряємо і створюємо папку для року, якщо вона не існує
    if not os.path.exists(year_dir):
        os.makedirs(year_dir)
        print_info(f"Створено директорію '{year_dir}'")
    
    # Формуємо назву файлу з ведучим нулем для тижня
    filename = f"{year_num}-{week_num:02d}.xlsx"
    # Повний шлях до файлу
    filepath = os.path.join(year_dir, filename)
    
    # Виводимо інформацію про запит
    print_info(f"Формування MDX запиту з параметрами:")
    print(f"   {Fore.CYAN}Рік:      {Fore.WHITE}{year_num}")
    print(f"   {Fore.CYAN}Тиждень:  {Fore.WHITE}{week_num}")
    print(f"   {Fore.CYAN}Фільтр:   {Fore.WHITE}{filter_fg1_name}")
    
    # Формуємо запит із використанням змінних для року та тижня
    query = f"""
    /* START QUERY BUILDER */
    EVALUATE
    SUMMARIZECOLUMNS(
        'Calendar'[calendar_date],
        Goods[fg1_name],
        Goods[fg2_name],
        Goods[fg3_name],
        Goods[fg4_name],
        Goods[articul],
        Goods[articul_name],
        Goods[producer_name],
        Agents_hybrid[name],
        Markets[doc_prefix_original],
        Channel_type[sell_channel_type_name],
        Price_types[name],
        Price_types[is_tender],
        Doc_types[name],
        Credit_products[payment_code],
        Credit_products[payment_typ],
        Credit_products[product_types],
        Credit_products[bank_name],
        Credit_products[bank_credit_product_code],
        Credit_products[product_name],
        Credit_products[payment_count],
        Promo[promo_type_name],
        Promo[basis],
        KEEPFILTERS( TREATAS( {{{year_num}}}, 'Calendar'[year_num] )),
        KEEPFILTERS( TREATAS( {{{week_num}}}, 'Calendar'[week_num] )),
        KEEPFILTERS( TREATAS( {{"{filter_fg1_name}"}}, Goods[fg1_name] )),
        "Реалізація, к-сть", [sell_qty],
        "Реалізація, грн.", [sell_amount_nds],
        "Реалізація ЦЗ, грн.", [buy_amount_nds],
        "Дохід, грн.", [profit_amount_nds],
        "Отримані бонуси", [bonus_obtained_amount],
        "Використані бонуси", [bonus_used_amount],
        "Комісія по кредитам", [credit_commission_amount]
    )
    ORDER BY 
        'Calendar'[calendar_date] ASC,
        Goods[fg1_name] ASC,
        Goods[fg2_name] ASC,
        Goods[fg3_name] ASC,
        Goods[fg4_name] ASC,
        Goods[articul] ASC,
        Goods[articul_name] ASC,
        Goods[producer_name] ASC,
        Agents_hybrid[name] ASC,
        Markets[doc_prefix_original] ASC,
        Channel_type[sell_channel_type_name] ASC,
        Price_types[name] ASC,
        Price_types[is_tender] ASC,
        Doc_types[name] ASC,
        Credit_products[payment_code] ASC,
        Credit_products[payment_typ] ASC,
        Credit_products[product_types] ASC,
        Credit_products[bank_name] ASC,
        Credit_products[bank_credit_product_code] ASC,
        Credit_products[product_name] ASC,
        Credit_products[payment_count] ASC,
        Promo[promo_type_name] ASC,
        Promo[basis] ASC
    /* END QUERY BUILDER */
    """
    
    print_progress(f"Виконання запиту до OLAP-кубу...")
    query_start_time = time.time()
    global animation_running
    
    try:
        cursor = connection.cursor()
        
        # Виконуємо запит
        cursor.execute(query)
        
        # Запускаємо індикатор завантаження в окремому потоці
        print_progress(f"Отримання даних з OLAP кубу...")
        
        # Оцінка часу виконання запиту, використовуючи усереднене значення у 5 хвилин
        # Ви можете налаштувати це значення на основі ваших спостережень
        estimated_query_time = 120  # 5 хвилин у секундах
        
        # Якщо є глобальна змінна з інформацією про середній час запитів, використовуємо її
        global avg_query_time
        if 'avg_query_time' in globals() and avg_query_time is not None:
            estimated_query_time = avg_query_time
        
        spinner_thread = threading.Thread(
            target=loading_spinner, 
            args=("Отримання даних з OLAP кубу", estimated_query_time)
        )
        spinner_thread.daemon = True
        spinner_thread.start()
        
        try:
            # Отримуємо всі рядки відразу
            rows = cursor.fetchall()
            # Зупиняємо анімацію
            animation_running = False
            spinner_thread.join(timeout=1.0)
            
            # Отримуємо імена колонок
            columns = [desc[0] for desc in cursor.description]
            
            query_end_time = time.time()
            query_duration = query_end_time - query_start_time
            
            # Оновлюємо середній час виконання запиту
            if 'avg_query_time' not in globals() or avg_query_time is None:
                avg_query_time = query_duration
            else:
                # Плавне оновлення середнього часу (алгоритм експоненційного згладжування)
                # Alpha - коефіцієнт згладжування (0.3 означає, що новий вимір має вагу 30%)
                alpha = 0.3
                avg_query_time = (1 - alpha) * avg_query_time + alpha * query_duration
            
            print_success(f"Запит виконано за {format_time(query_duration)}. Отримано {len(rows)} рядків даних.")
            
            cursor.close()
            
            # Створюємо DataFrame з отриманих даних
            df = pd.DataFrame(rows, columns=columns)
            
            # Якщо немає даних, повертаємо порожній список
            if len(df) == 0:
                print_warning(f"Запит не повернув даних для періоду {reporting_period}")
                return []
            
            print_progress(f"Обробка результатів запиту...")
            # Перейменовуємо стовпці для відповідності формату DAX Studio
            renamed_columns = {}
            potential_names = {}
            
            # Перший прохід: збираємо потенційні імена і перевіряємо дублікати
            for col in df.columns:
                # Шаблон для розпізнавання стовпців у форматі "TableName[ColumnName]"
                match = re.match(r'(\w+)\[([^\]]+)\]', col)
                if match:
                    # Витягуємо тільки назву стовпця без таблиці та дужок
                    column_name = match.group(2)
                    if column_name in potential_names:
                        # Дублювання виявлено, позначаємо обидва стовпці для збереження оригінальних назв
                        potential_names[column_name] = False
                    else:
                        # Поки що унікальне ім'я, помічаємо як потенційно перейменоване
                        potential_names[column_name] = True
                else:
                    # Для обчислюваних стовпців просто видаляємо квадратні дужки
                    column_name = col.strip('[]')
                    # Їхні імена зазвичай унікальні, але все одно перевіряємо
                    if column_name in potential_names:
                        potential_names[column_name] = False
                    else:
                        potential_names[column_name] = True
            
            # Другий прохід: застосовуємо перейменування, уникаючи дублікатів
            for col in df.columns:
                match = re.match(r'(\w+)\[([^\]]+)\]', col)
                if match:
                    column_name = match.group(2)
                    # Перейменовуємо тільки якщо немає конфлікту імен
                    if potential_names[column_name]:
                        renamed_columns[col] = column_name
                    # Інакше залишаємо оригінальну назву
                else:
                    # Для обчислюваних стовпців завжди видаляємо квадратні дужки
                    renamed_columns[col] = col.strip('[]')
            
            # Виводимо інформацію про стовпці, які не були перейменовані через дублювання
            duplicate_columns = [col for col in df.columns if re.match(r'(\w+)\[([^\]]+)\]', col) and 
                            re.match(r'(\w+)\[([^\]]+)\]', col).group(2) in potential_names and 
                            not potential_names[re.match(r'(\w+)\[([^\]]+)\]', col).group(2)]]
            
            if duplicate_columns:
                print_warning(f"Деякі стовпці не були перейменовані через потенційне дублювання:")
                for col in duplicate_columns:
                    match = re.match(r'(\w+)\[([^\]]+)\]', col)
                    if match:
                        print(f"   {Fore.YELLOW}• {Fore.WHITE}{col} {Fore.YELLOW}(конфлікт імені: {Fore.WHITE}{match.group(2)}{Fore.YELLOW})")
            else:
                print_info("Усі стовпці успішно перейменовано")
            
            # Застосовуємо нові назви стовпців
            df.rename(columns=renamed_columns, inplace=True)
            
            # Експортуємо дані у Excel-файл з форматуванням
            print_progress(f"Експорт даних у Excel-файл {filepath}...")
            
            # Спочатку створюємо Excel-файл з даними
            df.to_excel(filepath, index=False)
            
            # Тепер відкриваємо його за допомогою openpyxl для форматування
            from openpyxl import load_workbook
            
            wb = load_workbook(filepath)
            ws = wb.active
            
            # Налаштування стилів для заголовка з .env
            header_font = Font(
                name='Arial', 
                size=int(os.getenv('EXCEL_HEADER_FONT_SIZE', 11)), 
                bold=True, 
                color=os.getenv('EXCEL_HEADER_FONT_COLOR', 'FFFFFF')
            )
            header_fill = PatternFill(
                start_color=os.getenv('EXCEL_HEADER_COLOR', '00365E'), 
                end_color=os.getenv('EXCEL_HEADER_COLOR', '00365E'), 
                fill_type='solid'
            )
            
            # Застосування стилів до заголовків
            for cell in ws[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            
            # Закріплення заголовка, щоб він завжди був видимий при прокрутці
            ws.freeze_panes = 'A2'  # Закріплюємо перший рядок
            
            # Автоматичне налаштування ширини стовпців
            # Перебираємо всі стовпці та знаходимо максимальну довжину значення
            for col in range(1, len(df.columns) + 1):
                column_width = max(
                    len(str(df.columns[col-1])),  # Довжина заголовка
                    df.iloc[:, col-1].astype(str).str.len().max()  # Максимальна довжина даних
                )
                # Обмежуємо максимальну ширину стовпця
                adjusted_width = min(column_width + 2, 50)  # +2 для відступів
                ws.column_dimensions[get_column_letter(col)].width = adjusted_width
            
            # Зберігаємо відформатований файл
            wb.save(filepath)
            
            # Отримуємо розмір файлу та форматуємо його для виведення
            file_size_bytes = os.path.getsize(filepath)
            if file_size_bytes < 1024 * 1024:  # Менше 1 МБ
                file_size = f"{file_size_bytes / 1024:.1f} КБ"
            else:  # Більше або рівно 1 МБ
                file_size = f"{file_size_bytes / (1024 * 1024):.2f} МБ"
            
            print_success(f"Дані експортовано у файл: {Fore.WHITE}{filepath} {Fore.YELLOW}({file_size}, {len(df)} рядків)")
            
            # Повертаємо шлях до файлу для підтвердження успішного створення
            return filepath
            
        except Exception as e:
            # Зупиняємо анімацію при помилці
            animation_running = False
            spinner_thread.join(timeout=1.0)
            raise e
        
    except Exception as e:
        print_error(f"Помилка при виконанні запиту: {e}")
        return None

# Функція для отримання доступних тижнів з куба OLAP
def get_available_weeks(connection):
    """Отримує список доступних тижнів з куба OLAP"""
    print_info("Отримання доступних тижнів з куба OLAP...")
    
    query = """
    /* START QUERY BUILDER */
    EVALUATE
    SUMMARIZECOLUMNS(
        'Calendar'[year_num],
        'Calendar'[week_num]
    )
    ORDER BY 
        'Calendar'[year_num] ASC,
        'Calendar'[week_num] ASC
    /* END QUERY BUILDER */
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        
        available_weeks = []
        for row in rows:
            year = int(row[0])  # year_num
            week = int(row[1])  # week_num
            available_weeks.append((year, week))
        
        print_info(f"Отримано {len(available_weeks)} доступних тижнів з куба")
        return available_weeks
    
    except Exception as e:
        print_error(f"Помилка при отриманні доступних тижнів: {e}")
        return []

# Функція для відображення зворотнього відліку
def countdown_timer(seconds):
    """Відображає зворотній відлік"""
    for remaining in range(seconds, 0, -1):
        # Форматуємо час, що залишився
        time_left = format_time(remaining)
        sys.stdout.write(f"\r{Fore.YELLOW}[{get_current_time()}] ⏱️ Очікування: залишилось {time_left}...")
        sys.stdout.flush()
        time.sleep(1)
    print()  # Переходимо на новий рядок після завершення

# Головний код
try:
    # Отримуємо параметри з .env файлу
    load_dotenv()
    
    print_header(f"OLAP ЕКСПОРТ ДАНИХ - НАЛАШТУВАННЯ")
    
    # Зчитуємо періоди з .env файлу
    start_period = os.getenv('YEAR_WEEK_START')
    end_period = os.getenv('YEAR_WEEK_END')
    
    # Ініціалізація підключення до OLAP
    connection_string = get_connection_string()
    connection = connect_to_olap(connection_string)
    if not connection:
        print_error("Не вдалося підключитися до OLAP. Програма завершує роботу.")
        sys.exit(1)

    # Отримуємо доступні тижні з куба
    available_weeks = get_available_weeks(connection)
    
    # Якщо періоди вказані, генеруємо список пар (рік, тиждень)
    if start_period and end_period:
        year_week_pairs = generate_year_week_pairs(start_period, end_period, available_weeks)
        if not year_week_pairs:
            print_error("Не вдалося згенерувати список періодів. Використовуються значення за замовчуванням.")
            year_num = CURRENT_YEAR
            week_nums = [CURRENT_WEEK]
            year_week_pairs = [(year_num, week) for week in week_nums]
    else:
        # Задаємо значення для року та списку тижнів за замовчуванням
        year_num = CURRENT_YEAR
        week_nums = [CURRENT_WEEK]  # Список тижнів для обробки
        year_week_pairs = [(year_num, week) for week in week_nums]
    
    filter_fg1_name = os.getenv('FILTER_FG1_NAME')

    # Створюємо структуру папок для збереження результатів
    result_dir = "result"

    # Перевіряємо і створюємо основну папку, якщо вона не існує
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
        print_info(f"Створено директорію '{result_dir}'")

    # Попередньо створюємо всі папки для років, які будуть використовуватись
    for year, _ in set((year, 0) for year, _ in year_week_pairs):
        year_dir = os.path.join(result_dir, str(year))
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
            print_info(f"Створено директорію '{year_dir}'")

    # Зчитуємо налаштування таймауту між запитами
    query_timeout = int(os.getenv('QUERY_TIMEOUT', 30))  # Значення за замовчуванням 30 секунд

    # Виводимо інформацію про параметри запуску
    print_header(f"OLAP ЕКСПОРТ ДАНИХ - ПОЧАТОК РОБОТИ")
    print_info(f"Налаштування:")
    print(f"   {Fore.CYAN}OLAP сервер:  {Fore.WHITE}{os.getenv('OLAP_SERVER')}")
    print(f"   {Fore.CYAN}База даних:   {Fore.WHITE}{os.getenv('OLAP_DATABASE')}")
    print(f"   {Fore.CYAN}Фільтр:       {Fore.WHITE}{filter_fg1_name}")
    
    # Виводимо інформацію про періоди
    if start_period and end_period:
        print(f"   {Fore.CYAN}Період:       {Fore.WHITE}з {start_period} по {end_period}")
        print(f"   {Fore.CYAN}Кількість періодів: {Fore.WHITE}{len(year_week_pairs)}")
    else:
        print(f"   {Fore.CYAN}Рік:          {Fore.WHITE}{year_num}")
        print(f"   {Fore.CYAN}Тижні:        {Fore.WHITE}{', '.join(map(str, week_nums))}")
    
    print(f"   {Fore.CYAN}Таймаут:      {Fore.WHITE}{query_timeout} секунд")
    
    # Початок відліку часу
    start_time = time.time()
    
    # Виконуємо запити для всіх тижнів
    files_created = []
    
    print_info(f"Запуск обробки для {len(year_week_pairs)} тижнів...")
    
    # Ініціалізуємо трекер часу
    time_tracker = TimeTracker(len(year_week_pairs))
    
    for i, (year, week) in enumerate(year_week_pairs):
        # Для першого тижня не робимо затримку
        if i > 0:
            print(f"\n{Fore.YELLOW}{'-' * 40}")
            print_info(f"Очікування {query_timeout} секунд перед наступним запитом...")
            countdown_timer(query_timeout)
        
        reporting_period = f"{year}-{week:02d}"  # Формат РРРР-ТТ
        print(f"\n{Fore.CYAN}{'-' * 40}")
        
        # Відображаємо інформацію про прогрес обробки
        if i > 0:  # Після обробки хоча б одного елемента можемо показувати прогноз
            progress_info = time_tracker.get_progress_info()
            print(f"{Fore.MAGENTA}{progress_info}")
        
        print_info(f"Обробка тижня: {reporting_period} ({i+1}/{len(year_week_pairs)})")
        
        # Виконуємо запит і отримуємо результати
        file_path = run_mdx_query(connection, reporting_period)
        
        # Додаємо шлях до файлу до списку створених файлів
        if file_path:
            files_created.append(file_path)
        
        # Оновлюємо трекер часу після обробки елемента
        time_tracker.update()
    
    # Завершення відліку часу
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Виводимо підсумок обробки
    print_header(f"ПІДСУМОК ОБРОБКИ")
    # Детальна інформація про час виконання
    if len(year_week_pairs) > 1:
        avg_time_per_week = processing_time / len(year_week_pairs)
        print_info(f"Деталі часу виконання:")
        print(f"   {Fore.CYAN}Загальний час:    {Fore.WHITE}{format_time(processing_time)}")
        print(f"   {Fore.CYAN}Середній час на 1 тиждень: {Fore.WHITE}{format_time(avg_time_per_week)}")
        if time_tracker.elapsed_times:
            min_time = min(time_tracker.elapsed_times)
            max_time = max(time_tracker.elapsed_times)
            print(f"   {Fore.CYAN}Мінімальний час: {Fore.WHITE}{format_time(min_time)}")
            print(f"   {Fore.CYAN}Максимальний час: {Fore.WHITE}{format_time(max_time)}")
    else:
        print_success(f"Обробку завершено за {format_time(processing_time)}")

    print_info(f"Створено файлів: {len(files_created)}")
    
    if files_created:
        for i, file_path in enumerate(files_created, 1):
            file_size_bytes = os.path.getsize(file_path)
            if file_size_bytes < 1024 * 1024:  # Менше 1 МБ
                file_size = f"{file_size_bytes / 1024:.1f} КБ"
            else:  # Більше або рівно 1 МБ
                file_size = f"{file_size_bytes / (1024 * 1024):.2f} МБ"
            print(f"   {Fore.CYAN}{i}. {Fore.WHITE}{file_path} {Fore.YELLOW}({file_size})")
    else:
        print_warning("Не було створено жодного файлу")
    
    # Закриваємо підключення до OLAP
    if connection:
        connection.close()
        print_info("Підключення до OLAP сервера закрито")

except Exception as e:
    print_error(f"Помилка при виконанні програми: {e}")
    sys.exit(1)

finally:
    # Переконуємось, що анімація зупинена
    animation_running = False