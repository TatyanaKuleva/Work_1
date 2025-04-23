import json
import time
import requests
import os
from dotenv import load_dotenv
import datetime
import pandas as pd
from pandas.core.computation.common import result_type_many

from pandas.core.interchange.dataframe_protocol import DataFrame
from src.func_get_data import get_users_settings


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


def agregate_transaction_card(data_dict: list[dict])->list[dict]:
    """агрегирует транзакции по карте, выводит сумм транзакций по каждой карте и размер кэшбека"""
    df = pd.DataFrame(data_dict)
    df['расходы по карте'] = df.apply(lambda x: filtr_operation_with_cashback(x['Сумма операции']), axis=1)
    df['cashback'] = abs(df['расходы по карте']/100*1)
    card_grouped = df.groupby('Номер карты')
    sum_by_card = round(card_grouped.agg({'расходы по карте': 'sum', 'cashback': 'sum'}),2)
    result = sum_by_card.reset_index().to_dict('records')
    return result

def get_top_transaction(data_dict: list[dict])->DataFrame:
    """выводит топ 5 транзакций по сумме"""
    df = pd.DataFrame(data_dict)
    df['транзакция'] = abs(df['Сумма операции'])
    df_sorted = df.sort_values('транзакция', ascending=False, ignore_index=True)
    top_five_trans = df_sorted.loc[[0,1,2,3,4]]
    extract_data = top_five_trans[['Дата операции', 'Сумма операции', 'Категория', 'Описание']]
    result = extract_data.to_dict('records')
    return result


def get_currency_rate()->list:
    """функция запрашивает и возвращает актуальный курс валют установленных пользвателем"""
    load_dotenv(".env")
    API_KEY_currency = os.getenv("API_KEY_currency")

    data_settings = get_users_settings('../user_settings.json')
    currency_1 = data_settings['user_currencies'][0]
    currency_2 = data_settings['user_currencies'][1]

    rates_currency_list = []
    rates_currency_dict_1 = {}
    rates_currency_dict_2 = {}


    url_currency_1 = f"https://api.currencylayer.com/convert?access_key={API_KEY_currency}&from={currency_1}&to=RUB&amount=1"
    response_1= requests.get(url_currency_1)
    result_1 = response_1.json()
    rate_currency_1 = round(float(result_1["result"]),2)
    rates_currency_dict_1['currency'] = currency_1
    rates_currency_dict_1['rate'] =  rate_currency_1
    rates_currency_list.append(rates_currency_dict_1)


    time.sleep(15)

    url_currency_2 = f"https://api.currencylayer.com/convert?access_key={API_KEY_currency}&from={currency_2}&to=RUB&amount=1"
    response_2 = requests.get(url_currency_2)
    result_2 = response_2.json()
    rate_currency_2 = round(float(result_2["result"]), 2)
    rates_currency_dict_2['currency'] = currency_2
    rates_currency_dict_2['rate'] = rate_currency_2
    rates_currency_list.append(rates_currency_dict_2)


    return  rates_currency_list


def get_stocks_rate()->list:
    """функция запрашивает и возвращает актуальный курс  акций устанволенных пользвателем"""
    load_dotenv(".env")
    API_KEY_stock = os.getenv("API_KEY_stock")

    data_settings = get_users_settings('../user_settings.json')
    stock_1 = data_settings['user_stocks'][0]
    stock_2 = data_settings['user_stocks'][1]
    stock_3 = data_settings['user_stocks'][2]
    stock_4 = data_settings['user_stocks'][3]
    stock_5 = data_settings['user_stocks'][4]

    rates_stock_list = []
    rates_stock_dict_1 = {}
    rates_stock_dict_2 = {}
    rates_stock_dict_3 = {}
    rates_stock_dict_4 = {}
    rates_stock_dict_5 = {}


    url_stock_1 = f"https://api.marketstack.com/v1/eod?access_key={API_KEY_stock}"
    querystring = {"symbols": {stock_1}}
    response_stock_1 = requests.get(url_stock_1, params=querystring)
    result_stock_1 = response_stock_1.json()
    rate_stock_1 = round(float(result_stock_1["data"][0]["close"]), 2)
    rates_stock_dict_1['stock'] = stock_1
    rates_stock_dict_1['price'] = rate_stock_1
    rates_stock_list.append(rates_stock_dict_1)

    time.sleep(5)

    url_stock_2 = f"https://api.marketstack.com/v1/eod?access_key={API_KEY_stock}"
    querystring = {"symbols": {stock_2}}
    response_stock_2 = requests.get(url_stock_2, params=querystring)
    result_stock_2 = response_stock_2.json()
    rate_stock_2 = round(float(result_stock_2["data"][0]["close"]), 2)
    rates_stock_dict_2['stock'] = stock_2
    rates_stock_dict_2['price'] = rate_stock_2
    rates_stock_list.append(rates_stock_dict_2)

    time.sleep(5)

    url_stock_3 = f"https://api.marketstack.com/v1/eod?access_key={API_KEY_stock}"
    querystring = {"symbols": {stock_3}}
    response_stock_3 = requests.get(url_stock_3, params=querystring)
    result_stock_3 = response_stock_3.json()
    rate_stock_3 = round(float(result_stock_3["data"][0]["close"]), 2)
    rates_stock_dict_3['stock'] = stock_3
    rates_stock_dict_3['price'] = rate_stock_3
    rates_stock_list.append(rates_stock_dict_3)

    time.sleep(5)

    url_stock_4 = f"https://api.marketstack.com/v1/eod?access_key={API_KEY_stock}"
    querystring = {"symbols": {stock_4}}
    response_stock_4 = requests.get(url_stock_4, params=querystring)
    result_stock_4 = response_stock_4.json()
    rate_stock_4 = round(float(result_stock_4["data"][0]["close"]), 2)
    rates_stock_dict_4['stock'] = stock_4
    rates_stock_dict_4['price'] = rate_stock_4
    rates_stock_list.append(rates_stock_dict_4)

    time.sleep(5)

    url_stock_5 = f"https://api.marketstack.com/v1/eod?access_key={API_KEY_stock}"
    querystring = {"symbols": {stock_5}}
    response_stock_5 = requests.get(url_stock_5, params=querystring)
    result_stock_5 = response_stock_5.json()
    rate_stock_5 = round(float(result_stock_5["data"][0]["close"]), 2)
    rates_stock_dict_5['stock'] = stock_5
    rates_stock_dict_5['price'] = rate_stock_5
    rates_stock_list.append(rates_stock_dict_5)

    return rates_stock_list

if __name__ == '__main__':
    print(get_currency_rate())






