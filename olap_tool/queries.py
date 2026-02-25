import datetime
import re
import csv
import math
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .utils import (
    print_info,
    print_warning,
    print_progress,
    print_success,
    format_time,
    convert_dotnet_to_python,
    ensure_dir,
)
from .exporter import export_csv_stream, export_xlsx_dataframe, export_xlsx_stream
from . import progress

if TYPE_CHECKING:
    from .config import QueryConfig, ExportConfig, XlsxConfig, CsvConfig, ExcelHeaderConfig, PathsConfig


def generate_year_week_pairs(start_period, end_period, available_weeks):
    try:
        start_year, start_week = map(int, start_period.split("-"))
        end_year, end_week = map(int, end_period.split("-"))
    except (ValueError, AttributeError):
        print_warning("Невірний формат періодів. Використовуйте формат РРРР-ТТ")
        return []

    current_year = datetime.datetime.now().year
    min_year = current_year - 3
    max_year = current_year
    if start_year < min_year or end_year > max_year:
        print_warning(f"Невірні значення року (має бути між {min_year} та {max_year})")
        return []
    if start_year > end_year or (start_year == end_year and start_week > end_week):
        print_warning("Початковий період має бути раніше за кінцевий")
        return []

    available_dict = {(year, week): True for year, week in available_weeks}
    filtered_pairs = []
    all_pairs = []
    cy, cw = start_year, start_week
    while cy < end_year or (cy == end_year and cw <= end_week):
        all_pairs.append((cy, cw))
        cw += 1
        # Перевірка існування тижня через fromisocalendar
        try:
            datetime.date.fromisocalendar(cy, cw, 1)
        except ValueError:
            cw = 1
            cy += 1
    for year, week in all_pairs:
        if (year, week) in available_dict:
            filtered_pairs.append((year, week))
    if len(filtered_pairs) == 0:
        print_warning("Не знайдено доступних тижнів у вказаному діапазоні")
    else:
        print_info(f"Знайдено {len(filtered_pairs)} тижнів у вказаному діапазоні")
    return filtered_pairs


def run_dax_query(
    connection,
    reporting_period: str,
    query_config: "QueryConfig",
    export_config: "ExportConfig",
    xlsx_config: "XlsxConfig",
    csv_config: "CsvConfig",
    excel_header: "ExcelHeaderConfig",
    paths_config: "PathsConfig",
):
    try:
        year_num, week_num = map(int, reporting_period.split("-"))
    except (ValueError, AttributeError):
        print_warning(
            f"Невірний формат періоду: {reporting_period}. Використовуйте формат РРРР-ТТ"
        )
        return []

    filter_fg1_name = query_config.filter_fg1_name
    escaped_filter_fg1 = (filter_fg1_name or "").replace('"', '""')

    result_dir = Path(paths_config.result_dir)
    year_dir = result_dir / str(year_num)
    ensure_dir(year_dir)

    print_info(f"Формування DAX запиту з параметрами:")
    from colorama import Fore

    print(f"   {Fore.CYAN}Рік:      {Fore.WHITE}{year_num}")
    print(f"   {Fore.CYAN}Тиждень:  {Fore.WHITE}{week_num}")
    print(f"   {Fore.CYAN}Фільтр:   {Fore.WHITE}{filter_fg1_name}")

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
        KEEPFILTERS( TREATAS( {{"{escaped_filter_fg1}"}}, Goods[fg1_name] )),
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

    import threading
    import time as _time

    print_progress("Виконання запиту до OLAP-кубу...")
    query_start_time = _time.time()
    spinner_thread = None
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        estimated_query_time = 120
        spinner_thread = threading.Thread(
            target=progress.loading_spinner,
            args=("Отримання даних з OLAP кубу", estimated_query_time),
        )
        spinner_thread.start()

        export_format = export_config.format.upper()
        force_csv_only = export_config.force_csv_only
        streaming_xlsx = xlsx_config.streaming

        # Стрімінговий XLSX
        if export_format in ("XLSX", "BOTH") and not force_csv_only and streaming_xlsx:
            progress.animation_running = False
            spinner_thread.join(timeout=1.0)
            xlsx_path = year_dir / f"{year_num}-{week_num:02d}.xlsx"
            row_count, xlsx_size = export_xlsx_stream(
                cursor, xlsx_path, f"{year_num}-{week_num:02d}",
                excel_header, xlsx_config,
            )
            query_duration = _time.time() - query_start_time
            print_success(
                f"Запит виконано за {format_time(query_duration)}. Отримано {row_count} рядків даних."
            )
            print_success(
                f"Дані експортовано у файл: {Fore.WHITE}{str(xlsx_path)} {Fore.YELLOW}(рядків: {row_count})"
            )
            cursor.close()
            if export_format == "BOTH":
                csv_path = year_dir / f"{year_num}-{week_num:02d}.csv"
                cursor = connection.cursor()
                cursor.execute(query)
                export_csv_stream(
                    cursor, csv_path,
                    csv_config.delimiter, csv_config.encoding, csv_config.quoting,
                )
                cursor.close()
                print_success(
                    f"Дані додатково експортовано у файл: {Fore.WHITE}{str(csv_path)}"
                )
            return str(xlsx_path)

        if export_format == "CSV" or force_csv_only:
            csv_path = year_dir / f"{year_num}-{week_num:02d}.csv"
            row_count = export_csv_stream(
                cursor, csv_path,
                csv_config.delimiter, csv_config.encoding, csv_config.quoting,
            )
            progress.animation_running = False
            spinner_thread.join(timeout=1.0)
            query_duration = _time.time() - query_start_time
            print_success(
                f"Запит виконано за {format_time(query_duration)}. Отримано {row_count} рядків даних."
            )
            print_success(
                f"Дані експортовано у файл: {Fore.WHITE}{str(csv_path)} {Fore.YELLOW}(рядків: {row_count})"
            )
            cursor.close()
            return str(csv_path)

        # DataFrame-based export
        rows = cursor.fetchall()
        progress.animation_running = False
        spinner_thread.join(timeout=1.0)
        columns = [desc[0] for desc in cursor.description]
        query_duration = _time.time() - query_start_time
        print_success(
            f"Запит виконано за {format_time(query_duration)}. Отримано {len(rows)} рядків даних."
        )
        cursor.close()

        converted_rows = []
        for row in rows:
            converted_row = [convert_dotnet_to_python(value) for value in row]
            converted_rows.append(converted_row)

        df = pd.DataFrame(converted_rows, columns=columns)
        if len(df) == 0:
            print_warning(f"Запит не повернув даних для періоду {reporting_period}")
            return []

        print_progress("Обробка результатів запиту...")
        pattern = re.compile(r"(\w+)\[([^\]]+)\]")
        renamed_columns = {}
        potential_names = {}
        for col in df.columns:
            m = pattern.match(col)
            column_name = m.group(2) if m else col.strip("[]")
            potential_names[column_name] = (
                False if column_name in potential_names else True
            )
        for col in df.columns:
            m = pattern.match(col)
            if m:
                column_name = m.group(2)
                if potential_names[column_name]:
                    renamed_columns[col] = column_name
            else:
                renamed_columns[col] = col.strip("[]")

        duplicate_columns = []
        for col in df.columns:
            m = pattern.match(col)
            if not m:
                continue
            key = m.group(2)
            if key in potential_names and not potential_names[key]:
                duplicate_columns.append(col)
        if duplicate_columns:
            print_warning(
                "Деякі стовпці не були перейменовані через потенційне дублювання:"
            )
            for col in duplicate_columns:
                match = re.match(r"(\w+)\[([^\]]+)\]", col)
                if match:
                    print(
                        f"   {Fore.YELLOW}• {Fore.WHITE}{col} {Fore.YELLOW}(конфлікт імені: {Fore.WHITE}{match.group(2)}{Fore.YELLOW})"
                    )
        else:
            print_info("Усі стовпці успішно перейменовано")

        df.rename(columns=renamed_columns, inplace=True)

        if export_format not in ["XLSX", "CSV", "BOTH"]:
            print_warning(
                f"Невідомий формат експорту: {export_format}. Використовуємо XLSX."
            )
            export_format = "XLSX"
        export_xlsx_flag = export_format in ["XLSX", "BOTH"]
        export_csv_flag = export_format in ["CSV", "BOTH"]
        exported_files = []
        if export_xlsx_flag and not force_csv_only:
            xlsx_path = year_dir / f"{year_num}-{week_num:02d}.xlsx"
            xlsx_size = export_xlsx_dataframe(
                df, xlsx_path, f"{year_num}-{week_num:02d}",
                excel_header, xlsx_config,
            )
            exported_files.append((str(xlsx_path), xlsx_size))
        if export_csv_flag or force_csv_only:
            csv_path = year_dir / f"{year_num}-{week_num:02d}.csv"
            df_replaced = df.replace([math.inf, -math.inf], None)
            quoting_mode = csv_config.quoting.lower()
            df_replaced.to_csv(
                str(csv_path),
                sep=csv_config.delimiter,
                encoding=csv_config.encoding,
                index=False,
                quoting=(
                    csv.QUOTE_MINIMAL
                    if quoting_mode == "minimal"
                    else (
                        csv.QUOTE_ALL if quoting_mode == "all" else csv.QUOTE_NONNUMERIC
                    )
                ),
                na_rep="",
            )
            exported_files.append((str(csv_path), Path(csv_path).stat().st_size))

        for filepath, file_size_bytes in exported_files:
            if file_size_bytes < 1024 * 1024:
                file_size = f"{file_size_bytes / 1024:.1f} КБ"
            else:
                file_size = f"{file_size_bytes / (1024 * 1024):.2f} МБ"
            print_success(
                f"Дані експортовано у файл: {Fore.WHITE}{filepath} {Fore.YELLOW}({file_size}, {len(df)} рядків)"
            )
        return exported_files[0][0] if exported_files else None
    except Exception as e:
        from .utils import print_error

        print_error(f"Помилка при виконанні запиту: {e}")
        return None
    finally:
        if spinner_thread is not None:
            progress.animation_running = False
            try:
                spinner_thread.join(timeout=1.0)
            except Exception:
                pass


def get_available_weeks(connection):
    print_info("Отримання доступних тижнів з куба OLAP...")
    query = """
        /* START QUERY BUILDER */
        EVALUATE
        FILTER(
        SUMMARIZECOLUMNS(
            'Calendar'[year_num],
            'Calendar'[week_num],
            KEEPFILTERS( FILTER( ALL( 'Calendar'[year_num] ), NOT( ISBLANK( 'Calendar'[year_num] ))))
        )
        ,NOT( ISBLANK( [sell_qty] ))
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
            year_value = convert_dotnet_to_python(row[0])
            week_value = convert_dotnet_to_python(row[1])
            if year_value is not None and week_value is not None:
                try:
                    year = int(year_value)
                    week = int(week_value)
                    available_weeks.append((year, week))
                except (ValueError, TypeError):
                    continue
        print_info(f"Отримано {len(available_weeks)} доступних тижнів з куба")
        return available_weeks
    except Exception as e:
        from .utils import print_error

        print_error(f"Помилка при отриманні доступних тижнів: {e}")
        return []
