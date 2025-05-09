import json
import datetime
import pandas as pd
import logging
import calendar
from typing import Optional

from src.func_get_data import read_excel_file

df = read_excel_file('../data/operations.xlsx')
df_to_df = pd.DataFrame(df)


def spending_by_category(transactions: pd.DataFrame,
                         category: str,
                         date: Optional[str] = None) -> pd.DataFrame:
    """Функция возвращает траты по заданной категории за последние три месяца (от переданной даты"""
    if date is not None:
        date_end = datetime.datetime.strptime(date, "%d.%m.%Y %H:%M:%S")
    else:
        date_end = datetime.datetime.now()
    date_start = (date_end - datetime.timedelta(days=90)).replace(hour=00, minute=00, second=00)

    transactions_dict = transactions.to_dict('records')

    filtr_list_transaction = []
    for item in transactions_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            filtr_list_transaction.append(item)

    df_filtr_transaction_by_period = pd.DataFrame(filtr_list_transaction)

    df_filtr_by_category = df_filtr_transaction_by_period.loc[df_filtr_transaction_by_period['Категория'] == category].copy()
    df_filtr_by_category['sum_trans'] = abs(df_filtr_by_category['Сумма операции'])
    result = df_filtr_by_category.agg({'sum_trans': 'sum'}).to_dict()

    json_result = json.dumps(result, indent=4, ensure_ascii=False)

    return json_result


def spending_by_weekday(transactions: pd.DataFrame,
                        date: Optional[str] = None) -> pd.DataFrame:

    if date is not None:
        date_end = datetime.datetime.strptime(date, "%d.%m.%Y %H:%M:%S")
    else:
        date_end = datetime.datetime.now()
    date_start = (date_end - datetime.timedelta(days=90)).replace(hour=00, minute=00, second=00)

    transactions_dict = transactions.to_dict('records')

    filtr_list_transaction = []
    for item in transactions_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            item['format_date'] = format_date.isoweekday()
            filtr_list_transaction.append(item)

    df_filtr_transaction_by_period = pd.DataFrame(filtr_list_transaction)

    df_filtr_transaction_by_period['sum_trans_for_round'] = abs(df_filtr_transaction_by_period['Сумма операции'])

    df_mean_by_weekdays = df_filtr_transaction_by_period.groupby('format_date').agg({'sum_trans_for_round': 'mean'}).round(2).reset_index()

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

    json_result = json.dumps(result_dict, indent=4, ensure_ascii=False)

    return json_result

def spending_by_workday(transactions: pd.DataFrame,
                        date: Optional[str] = None) -> pd.DataFrame:
    if date is not None:
        date_end = datetime.datetime.strptime(date, "%d.%m.%Y %H:%M:%S")
    else:
        date_end = datetime.datetime.now()
    date_start = (date_end - datetime.timedelta(days=90)).replace(hour=00, minute=00, second=00)

    transactions_dict = transactions.to_dict('records')

    filtr_list_transaction = []
    for item in transactions_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            item['format_date'] = format_date.isoweekday()
            filtr_list_transaction.append(item)

    df_filtr_transaction_by_period = pd.DataFrame(filtr_list_transaction)

    df_filtr_transaction_by_period['sum_trans_for_round'] = abs(df_filtr_transaction_by_period['Сумма операции'])

    work_day = [1,2,3,4,5]
    day_off = [6,7]

    df_filtr_work_day = df_filtr_transaction_by_period[df_filtr_transaction_by_period['format_date'].isin(work_day)].agg({'sum_trans_for_round': 'mean'}).round(2).to_dict()
    df_filtr_day_off = df_filtr_transaction_by_period[df_filtr_transaction_by_period['format_date'].isin(day_off)].agg({'sum_trans_for_round': 'mean'}).round(2).to_dict()

    result_dict = dict()

    result_dict["средние траты в рабочий день"] = df_filtr_work_day['sum_trans_for_round']
    result_dict["средние траты в выходной день"] = df_filtr_day_off['sum_trans_for_round']

    json_result = json.dumps(result_dict, indent=4, ensure_ascii=False)

    return json_result






if __name__ == '__main__':
    print(spending_by_workday(df_to_df, '15.12.2019 16:44:00'))
