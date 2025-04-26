import json
import time
import requests
import os

from dateutil.rrule import weekday
from dotenv import load_dotenv
import datetime
import pandas as pd
from pandas.core.computation.common import result_type_many

from pandas.core.interchange.dataframe_protocol import DataFrame
from src.func_get_data import get_users_settings


def get_start_of_period(date:str, data_range='M')->datetime:
    """определяет первое число месяца в котором находится заданная дата"""
    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    if data_range == 'M':
        start_period = date_obj.replace(day=1, hour=0, minute=0, second=0)
    elif data_range == 'W':
        day_week = date_obj.isoweekday()
        week_period = 1 - day_week
        new_start_period = date_obj + datetime.timedelta(days=week_period)
        start_period = new_start_period.replace(hour=0, minute=0, second=0)
    elif data_range == 'Y':
        start_period = date_obj.replace(day=1, month=1, hour=0, minute=0, second=0)
    elif data_range == 'ALL':
        start_period = date_obj.replace(year=1, day=1, month=1, hour=0, minute=0, second=0)

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
    start_date = get_start_of_period()
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
    user_curencies = data_settings['user_currencies']

    rates_currency_list = []

    for currency in user_curencies:
        rates_currency_dict = {}


        url_currency = f"https://api.currencylayer.com/convert?access_key={API_KEY_currency}&from={currency}&to=RUB&amount=1"
        response= requests.get(url_currency)
        result = response.json()
        rate_currency = round(float(result["result"]),2)
        rates_currency_dict['currency'] = currency
        rates_currency_dict['rate'] =  rate_currency
        rates_currency_list.append(rates_currency_dict)

        time.sleep(15)

    return  rates_currency_list


def get_stocks_rate()->list:
    """функция запрашивает и возвращает актуальный курс  акций устанволенных пользвателем"""
    load_dotenv(".env")
    API_KEY_stock = os.getenv("API_KEY_stock")

    data_settings = get_users_settings('../user_settings.json')
    user_stock = data_settings['user_stocks']

    rates_stock_list = []

    for stock in user_stock:
        rates_stock_dict = {}

        url_stock = f"https://api.marketstack.com/v1/eod?access_key={API_KEY_stock}"
        querystring = {"symbols": {stock}}
        response_stock = requests.get(url_stock, params=querystring)
        result_stock = response_stock.json()
        rate_stock = round(float(result_stock["data"][0]["close"]), 2)
        rates_stock_dict['stock'] = stock
        rates_stock_dict['price'] = rate_stock
        rates_stock_list.append(rates_stock_dict)

        time.sleep(5)

    return rates_stock_list


def filtr_transction_by_period(data_dict: list[dict], date:str, data_range='M')->list[dict]:
    """фильтрует транзакции совершенные в период с начала месяца до заданной даты"""
    start_date = get_start_of_period(date)
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    filtr_list_transaction = []
    for item in data_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if start_date <= format_date <= end_date:
            filtr_list_transaction.append(item)
    return filtr_list_transaction

if __name__ == '__main__':
    print(get_start_of_period('2019-07-17 15:05:27', 'W'))






