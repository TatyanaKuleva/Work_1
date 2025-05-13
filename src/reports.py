import json
import datetime
import pandas as pd
import logging
import calendar
from typing import Optional
import logging
import os

from src.func_get_data import read_excel_file

# os.makedirs(os.path.dirname(logs), exist_ok=True)

# directory_path = os.path.dirname(os.path.abspath(__file__))
# file_name = os.path.join(directory_path, "file_name.csv")
# relative_path = "../logs/reports.log"
# absolute_path = os.path.abspath(relative_path)

# basedir = os.path.dirname(l)
# dir_name_log = 'logs'
# file_log= os.path.join(basedir, dir_name_log, "reports.log")

report_logger = logging.getLogger("report")
report_logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('logs.reports.log', "w", encoding="UTF-8")
file_formatter = logging.Formatter("%(asctime)s -%(name)s - %(levelname)s:%(message)s")
file_handler.setFormatter(file_formatter)
report_logger.addHandler(file_handler)

def save_file(function):
    def inner(*args, **kwargs):
        result = function(*args, **kwargs)
        with open('result_report_function.json', 'w', encoding='utf-8') as file:
            json.dump(result, file, ensure_ascii=False, indent=4)
        return result
    return inner

def decorator_set_name_file_for_save(name_file):
    def save_file(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            with open(name_file, 'w', encoding='utf-8') as file:
                json.dump(result, file, ensure_ascii=False, indent=4)
            return result
        return wrapper
    return save_file


# file_read= os.path.join(basedir, "operations.xlsx")
# df = read_excel_file(file_read)
df = read_excel_file('data/operations.xlsx')
df_to_df = pd.DataFrame(df)


def spending_by_category(transactions: pd.DataFrame,
                         category: str,
                         date: Optional[str] = None) -> pd.DataFrame:
    """Функция возвращает траты по заданной категории за последние три месяца от переданной даты"""
    report_logger.info(f"определяет дату окончания периода")
    if date is not None:
        date_end = datetime.datetime.strptime(date, "%d.%m.%Y %H:%M:%S")
    else:
        date_end = datetime.datetime.now()
    report_logger.info(f"определяет дату начала периода")
    date_start = (date_end - datetime.timedelta(days=90)).replace(hour=00, minute=00, second=00)

    transactions_dict = transactions.to_dict('records')

    report_logger.info(f"фильтрует операции относящиеся к заданному периоду от {date_start} до {date_end}")
    filtr_list_transaction = []
    for item in transactions_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            filtr_list_transaction.append(item)

    report_logger.info(f"формирует датафрейм из списка транзакций")
    df_filtr_transaction_by_period = pd.DataFrame(filtr_list_transaction)

    report_logger.info(f"фильтрует операции по категории {category}")
    df_filtr_by_category = df_filtr_transaction_by_period.loc[df_filtr_transaction_by_period['Категория'] == category].copy()
    df_filtr_by_category['sum_trans'] = abs(df_filtr_by_category['Сумма операции'])
    result = df_filtr_by_category.agg({'sum_trans': 'sum'}).round(2).to_dict()

    report_logger.info(f"формирует Json ответ")
    json_result = json.dumps(result, indent=4, ensure_ascii=False)

    return json_result


def spending_by_weekday(transactions: pd.DataFrame,
                        date: Optional[str] = None) -> pd.DataFrame:
    """Функция возвращает средние траты в каждый из дней недели за последние три месяца (от переданной даты).."""

    if date is not None:
        report_logger.info(f"определяет дату окончания периода")
        date_end = datetime.datetime.strptime(date, "%d.%m.%Y %H:%M:%S")
    else:
        date_end = datetime.datetime.now()
    report_logger.info(f"определяет дату окончания периода")
    date_start = (date_end - datetime.timedelta(days=90)).replace(hour=00, minute=00, second=00)

    report_logger.info(f"формирует словарь списка транзакций из датафрейма")
    transactions_dict = transactions.to_dict('records')

    report_logger.info(f"фильтрует операции относящиеся к заданному периоду от {date_start} до {date_end}")
    filtr_list_transaction = []
    for item in transactions_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            item['format_date'] = format_date.isoweekday()
            filtr_list_transaction.append(item)

    report_logger.info(f"формирует датафрейм из списка транзакций")
    df_filtr_transaction_by_period = pd.DataFrame(filtr_list_transaction)

    df_filtr_transaction_by_period['sum_trans_for_round'] = abs(df_filtr_transaction_by_period['Сумма операции'])

    report_logger.info(f"группирует по дням недели и агрегирует по среднему значению")
    df_mean_by_weekdays = df_filtr_transaction_by_period.groupby('format_date').agg({'sum_trans_for_round': 'mean'}).round(2).reset_index()

    report_logger.info(f"формирует словарь с результатами")
    result_dict= dict()
    for index, row in df_mean_by_weekdays.iterrows():
        if row['format_date'] == 1:
            result_dict['Понедельник'] = float(row['sum_trans_for_round'])
        elif row['format_date'] == 2:
            result_dict['Вторник'] = float(row['sum_trans_for_round'])
        elif row['format_date'] == 3:
            result_dict['Среда'] = float(row['sum_trans_for_round'])
        elif row['format_date'] == 4:
            result_dict['Четверг'] = float(row['sum_trans_for_round'])
        elif row['format_date'] == 5:
            result_dict['Пятница'] = float(row['sum_trans_for_round'])
        elif row['format_date'] == 6:
            result_dict['Суббота'] = float(row['sum_trans_for_round'])
        elif row['format_date'] == 7:
            result_dict['Воскресенье'] = float(row['sum_trans_for_round'])

    report_logger.info(f"формирует Json ответ")
    json_result = json.dumps(result_dict, indent=4, ensure_ascii=False)

    return json_result


@decorator_set_name_file_for_save('set_file.json')
def spending_by_workday(transactions: pd.DataFrame,
                        date: Optional[str] = None) -> pd.DataFrame:
    """Функция выводит средние траты в рабочий и в выходной день за последние три месяца (от переданной даты)."""
    report_logger.info(f"определяет дату окончания периода")
    if date is not None:
        date_end = datetime.datetime.strptime(date, "%d.%m.%Y %H:%M:%S")
    else:
        date_end = datetime.datetime.now()
    report_logger.info(f"определяет дату начала периода")
    date_start = (date_end - datetime.timedelta(days=90)).replace(hour=00, minute=00, second=00)

    report_logger.info(f"формирует словарь списка транзакций из датафрейма")
    transactions_dict = transactions.to_dict('records')

    report_logger.info(f"фильтрует операции относящиеся к заданному периоду от {date_start} до {date_end}")
    filtr_list_transaction = []
    for item in transactions_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            item['format_date'] = format_date.isoweekday()
            filtr_list_transaction.append(item)

    report_logger.info(f"формирует датафрейм из списка транзакций")
    df_filtr_transaction_by_period = pd.DataFrame(filtr_list_transaction)

    df_filtr_transaction_by_period['sum_trans_for_round'] = abs(df_filtr_transaction_by_period['Сумма операции'])

    work_day = [1,2,3,4,5]
    day_off = [6,7]

    report_logger.info(f"фильтрует транзакции по типу дня недели")
    df_filtr_work_day = df_filtr_transaction_by_period[df_filtr_transaction_by_period['format_date'].isin(work_day)].agg({'sum_trans_for_round': 'mean'}).round(2).to_dict()
    df_filtr_day_off = df_filtr_transaction_by_period[df_filtr_transaction_by_period['format_date'].isin(day_off)].agg({'sum_trans_for_round': 'mean'}).round(2).to_dict()

    report_logger.info(f"формирует словарь с результатами")
    result_dict = dict()
    result_dict["средние траты в рабочий день"] = df_filtr_work_day['sum_trans_for_round']
    result_dict["средние траты в выходной день"] = df_filtr_day_off['sum_trans_for_round']

    report_logger.info(f"формирует Json ответ")
    json_result = json.dumps(result_dict, indent=4, ensure_ascii=False)

    return json_result






if __name__ == '__main__':
    print(spending_by_workday(pd.DataFrame({'Валюта операции': ['RUB', 'RUB', 'RUB', 'RUB', 'RUB', 'RUB'],
                         'Дата операции': ['30.12.2021 16:44:00', '21.12.2021 12:22:00',
                                           '24.12.2021 10:02:00', '22.12.2021 16:44:00',
                                           '25.12.2021 12:22:00', '28.12.2021 10:02:00'],
                          'Категория': ['Аптеки', 'Аптеки', 'Супермаркеты',
                                        'Супермаркеты', 'Пополнения', 'Дом и ремонт'],
                         'Сумма операции': [-3000.0, 200.0, -10000.0,
                                            -3000.0, 200.0, -10000.0]}), '31.12.2021 16:44:00'))
