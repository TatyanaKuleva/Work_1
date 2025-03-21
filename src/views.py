import json
import requests
import datetime
import pandas as pd

from pandas.core.interchange.dataframe_protocol import DataFrame
from func_get_data import read_excel_file



transaction = read_excel_file('../data/operations.xlsx')

def get_start_of_period(date:str)->datetime:
    """определяет первое число месяца в котором находится заданная дата"""
    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    start_period = date_obj.replace(day=1, hour=0, minute=0, second=0)
    return start_period

def greeting_user(date:str)->str:
    """приветствует пользователя в соответствии со временем заданной даты"""
    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    greeting = ""
    if 5 <= date_obj.hour <= 12:
        greeting = "Доброе утро"
    elif 12 < date_obj.hour <= 18:
        greeting = "Добрый день"
    elif 18 < date_obj.hour <= 23:
        greeting = "Добрый вечер"
    else:
        greeting = "Доброй ночи"

    return greeting


def filtr_transction_by_date(data_dict: list[dict], date:str)->list[dict]:
    """фильтрует транзакции совершенные в период с начала месяца до заданной даты"""
    start_date = get_start_of_period(date)
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    filtr_list_transaction = []
    for item in data_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if start_date <= format_date <= end_date:
            filtr_list_transaction.append(item)
    return filtr_list_transaction

def filtr_operation_with_cashback(sum_operation:float)->float:
    """фильтрует расходы по карте"""
    if sum_operation < 0:
        spent = sum_operation
    else:
        spent = 0
    return spent


def agregate_transaction_card(data_dict: list[dict])->DataFrame:
    """агрегирует транзакции по карте, выводит сумм транзакций по каждой карте и размер кэшбека"""
    df = pd.DataFrame(data_dict)
    df['расходы по карте'] = df.apply(lambda x: filtr_operation_with_cashback(x['Сумма операции']), axis=1)
    df['cashback'] = abs(df['расходы по карте']/100*1)
    card_grouped = df.groupby('Номер карты')
    sum_by_card = card_grouped.agg({'расходы по карте': 'sum', 'cashback': 'sum', 'Кэшбэк': 'sum'})
    return sum_by_card

def get_top_transaction(data_dict: list[dict])->DataFrame:
    """выводит топ 5 транзакций по сумме"""
    df = pd.DataFrame(data_dict)
    df['транзакция'] = abs(df['Сумма операции'])
    df_sorted = df.sort_values('транзакция', ascending=False, ignore_index=True)
    top_five_trans = df_sorted.loc[[0,1,2,3,4]]
    return top_five_trans







if __name__ == '__main__':
    print(get_top_transaction(transaction))


