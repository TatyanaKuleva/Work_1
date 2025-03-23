import json
import requests
import os
from dotenv import load_dotenv
import datetime
import pandas as pd

from pandas.core.interchange.dataframe_protocol import DataFrame
from func_get_data import read_excel_file, get_users_settings



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

load_dotenv(".env")
API_KEY_currency = os.getenv("API_KEY_currency")
API_KEY_stock = os.getenv("API_KEY_stock")

def get_currency_stocks_rate()->dict:
    """функция запрашивает и возвращает актуальный курс валюты и акций устанволенных пользвателем"""
    data_settings = get_users_settings('../user_settings.json')
    currency_1 = data_settings['user_currencies'][0]
    currency_2 = data_settings['user_currencies'][1]
    stock_1 = data_settings['user_stocks'][0]
    stock_2 = data_settings['user_stocks'][1]
    stock_3 = data_settings['user_stocks'][2]
    stock_4 = data_settings['user_stocks'][3]
    stock_5 = data_settings['user_stocks'][4]



    url_currency_1 = f"https://api.apilayer.com/exchangerates_data/convert"
    payload = {
        "amount": "1",
        "from": currency_1,
        "to": "RUB"}
    headers = {"apikey": f"{API_KEY_currency}"}
    response_1 = requests.get(url_currency_1, headers=headers, params=payload)

    url_currency_2 = f"https://api.apilayer.com/exchangerates_data/convert"
    payload = {
        "amount": "1",
        "from": currency_2,
        "to": "RUB"}
    headers = {"apikey": f"{API_KEY_currency}"}
    response_2= requests.get(url_currency_2, headers=headers, params=payload)
    result_1 = response_1.json()
    rate_currency_1 = round(float(result_1["result"]),2)
    result_2 = response_2.json()
    rate_currency_2 = round(float(result_2["result"]),2)

    url_stock_1 = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock_1}&apikey={API_KEY_stock}"
    response_stock_1 = requests.get(url_stock_1)
    result_stock_1 = response_stock_1.json()
    rate_stock_1 = round(float(result_stock_1["Global Quote"]["05. price"]), 2)

    url_stock_2 = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock_2}&apikey={API_KEY_stock}"
    response_stock_2 = requests.get(url_stock_2)
    result_stock_2 = response_stock_2.json()
    rate_stock_2 = round(float(result_stock_2["Global Quote"]["05. price"]), 2)

    url_stock_3 = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock_3}&apikey={API_KEY_stock}"
    response_stock_3 = requests.get(url_stock_3)
    result_stock_3 = response_stock_3.json()
    rate_stock_3 = round(float(result_stock_3["Global Quote"]["05. price"]), 2)

    url_stock_4 = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock_4}&apikey={API_KEY_stock}"
    response_stock_4 = requests.get(url_stock_4)
    result_stock_4 = response_stock_4.json()
    rate_stock_4 = round(float(result_stock_4["Global Quote"]["05. price"]), 2)

    url_stock_5 = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock_5}&apikey={API_KEY_stock}"
    response_stock_5 = requests.get(url_stock_5)
    result_stock_5 = response_stock_5.json()
    rate_stock_5 = round(float(result_stock_5["Global Quote"]["05. price"]), 2)




    # url_stock_1 = f"https://www.alphavantage.co/query"
    # payload = {
    #     "function": "GLOBAL_QUOTE",
    #     "symbol": stock_1}
    # headers = {"apikey": f"{API_KEY_stock}"}
    # response_stock_1 = requests.get(url_stock_1, headers=headers, params=payload)
    # result_stock_1 = response_stock_1.json()
    # rate_stock_1 = round(float(result_stock_1["05. price"]), 2)

    # url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'

    # rub_amount = float(result["result"])

    return rate_currency_1, rate_currency_2, rate_stock_1, rate_stock_2, rate_stock_3, rate_stock_4, rate_stock_5







if __name__ == '__main__':
    print(get_currency_stocks_rate())


