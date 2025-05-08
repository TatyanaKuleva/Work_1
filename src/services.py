import json
import datetime
import pandas as pd
import logging
import calendar
import math
import re

from mypy.scope import nullcontext

from src.func_get_data import read_excel_file

df = read_excel_file('../data/operations.xlsx')

def favorable_categories_of_increased_cashback(data, year, month):
    """функция для анализа выгодности категорий повышенного кешбэка"""
    year_dt = datetime.datetime.strptime(year, "%Y")
    month_abb = ""
    if month == 'январь':
        month_abb = 'Jan'
    elif month == "февраль":
        month_abb = 'Feb'
    elif month == "март":
        month_abb = 'Mar'
    elif month == "апрель":
        month_abb = 'Apr'
    elif month == "май":
        month_abb = 'May'
    elif month == "июнь":
        month_abb = 'Jun'
    elif month == "июль":
        month_abb = 'Jul'
    elif month == "август":
        month_abb = 'Aug'
    elif month == "сентябрь":
        month_abb = 'Sep'
    elif month == "октябрь":
        month_abb = 'Oct'
    elif month == "ноябрь":
        month_abb = 'Nov'
    elif month == "декабрь":
        month_abb = 'Dec'
    month_dt = datetime.datetime.strptime(month_abb, "%b")
    date_start = datetime.datetime(year = year_dt.year, month=month_dt.month, day=1)
    date_end = datetime.datetime(year = year_dt.year, month=month_dt.month,
                                 day=calendar.monthrange(year_dt.year, month=month_dt.month)[1], hour=23, minute=59)

    filtr_list_transaction = []
    for item in data:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            filtr_list_transaction.append(item)

    df_filtr_list_transaction = pd.DataFrame(filtr_list_transaction)

    df_sum_by_cashback = df_filtr_list_transaction.groupby("Категория", as_index=False).agg({"Кэшбэк": 'sum'})
    sort_for_sum_cashback = df_sum_by_cashback.sort_values(by='Кэшбэк', ascending=False, ignore_index=True).head(3)
    res_dict = dict()
    for index, row in sort_for_sum_cashback.iterrows():
        res_dict[row["Категория"]] = row["Кэшбэк"]

    json_result = json.dumps(res_dict, indent=4,  ensure_ascii=False)

    return json_result

def investment_bank(month, transactions, limit):
    date_obj = datetime.datetime.strptime(month, "%Y-%m")
    date_start = datetime.datetime(year=date_obj.year, month=date_obj.month, day=1)
    date_end = datetime.datetime(year=date_start.year, month=date_obj.month,
                                 day=calendar.monthrange(date_start.year, month=date_obj.month)[1], hour=23, minute=59)

    filtr_list_transaction = []
    for item in transactions:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            filtr_list_transaction.append(item)

    df_filtr_list_transaction = pd.DataFrame(filtr_list_transaction)
    df_only_expenses = df_filtr_list_transaction.loc[df_filtr_list_transaction['Сумма операции'] < 0].copy()
    df_only_expenses['sum_trans_for_round'] = abs(df_only_expenses['Сумма операции'] / limit)
    df_only_expenses['sum_round'] = df_only_expenses.apply(lambda x: limit * (math.ceil(x['sum_trans_for_round'])), axis=1)
    df_only_expenses['sum_for_invest'] = df_only_expenses['sum_round'] - abs(df_only_expenses['Сумма операции'])

    result = df_only_expenses.agg({'sum_for_invest': 'sum'}).to_dict()

    result_dict = dict()
    #
    result_dict['Сумма накопления в результате округления'] = result['sum_for_invest']


    json_result = json.dumps(result_dict, indent=4, ensure_ascii=False)

    return json_result

def basic_search(data_dict:list[dict], str_for_search:str):
    """принимает строку для поиска, возвращает JSON-ответ со всеми транзакциями, содержащими
    запрос в описании или категории."""
    result_list = []
    pattern = re.compile(str_for_search, flags=re.IGNORECASE)
    for item in data_dict:
        if pattern.findall(item['Описание']):
            result_list.append(item)
        elif type(item['Категория']) != float :
            if pattern.findall(item['Категория']):
                result_list.append(item)


    json_result = json.dumps(result_list, indent=4, ensure_ascii=False)

    return json_result

def phone_search(data_dict:list[dict]):
    """Функция возвращает JSON со всеми транзакциями, содержащими в описании мобильные номера."""
    result_list = []
    pattern = re.compile(r'[+][7]\s\d+')
    for item in data_dict:
        if pattern.findall(item['Описание']):
            result_list.append(item)
    json_result = json.dumps(result_list, indent=4, ensure_ascii=False)

    return json_result

def search_transfer_for_individuals(data_dict:list[dict]):
    """Функция возвращает JSON со всеми транзакциями, которые относятся к переводам физлицам.
    Категория такой транзакции — переводы, а в описании есть имя и первая буква фамилии с точкой."""
    # df_only_transfer = df_filtr_list_transaction.loc[df_filtr_list_transaction['Сумма операции'] < 0].copy()
    result_list = []
    pattern_cat = re.compile(r'перев', flags=re.IGNORECASE)
    pattern_ind = re.compile(r'\w+\s\w[.]')

    for item in data_dict:
        if type(item['Категория']) != float:
            if pattern_cat.findall(item['Категория']):
                if pattern_ind.findall(item['Описание']):
                    result_list.append(item)
    json_result = json.dumps(result_list, indent=4, ensure_ascii=False)

    return json_result









if __name__ == '__main__':
    print(search_transfer_for_individuals([{'Категория': 'Переводы', 'Описание': 'На р/с ООО "ФОРТУНА"', 'Дата операции': '15.07.2019 20:00:31', 'Сумма операции': -50000.0, 'Кэшбэк': 500.0},
            {'Категория': 'nan', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 17:44:36', 'Сумма операции': -17000.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 18:14:20', 'Сумма операции': 17000.0, 'Кэшбэк': 170.0},
            {'Категория': 'Переводы', 'Описание': 'Марина К.', 'Дата операции': '15.07.2019 20:00:31',
             'Сумма операции': -50000.0, 'Кэшбэк': 500.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 18:14:20',
             'Сумма операции': 17000.0, 'Кэшбэк': 170.0},
            {'Категория': 'Переводы', 'Описание': 'Перевод с карты Василий М.', 'Дата операции': '06.07.2019 21:08:09',
             'Сумма операции': 13000.0},
            {'Категория': 'Наличные', 'Описание': 'Снятие в банкомате Альфа-Банк',
             'Дата операции': '06.07.2019 14:08:29',
             'Сумма операции': -3000.0, 'Кэшбэк': 30}
            ]))
