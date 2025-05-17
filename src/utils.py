import json
import time
import requests
import os
import logging

from dateutil.rrule import weekday
from dotenv import load_dotenv
import datetime
import pandas as pd
from pandas.core.computation.common import result_type_many

from pandas.core.interchange.dataframe_protocol import DataFrame
from src.func_get_data import get_users_settings

utils_logger = logging.getLogger("utils")
utils_logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("logs/utils.log", "w", encoding="UTF-8")
file_formatter = logging.Formatter("%(asctime)s -%(name)s - %(levelname)s:%(message)s")
file_handler.setFormatter(file_formatter)
utils_logger.addHandler(file_handler)



def get_start_of_period(date:str, data_range='M')->datetime:
    """определяет первое число для формирования диапазона данных для заданной даты"""
    utils_logger.info(f"определяет даты начала периода для формирования диапазона данных")
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
    else:
        utils_logger.error(f"неверный ввод данных для определения периода отчета")
        raise ValueError (f"Вы ввели неверные данные. период можно задать выбрав буквы M, W, Y или вариант ALL")

    return start_period

def greeting_user(date:str)->str:
    """приветствует пользователя в соответствии со временем заданной даты"""
    try:
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        utils_logger.info(f"Приветствует пользователя с учетом времени суток")
        greeting = ""
        if 5 <= date_obj.hour <= 12:
            greeting = "Доброе утро"
        elif 12 < date_obj.hour <= 18:
            greeting = "Добрый день"
        elif 18 < date_obj.hour <= 23:
            greeting = "Добрый вечер"
        else:
            greeting = "Доброй ночи"
    except ValueError as ex:
        utils_logger.error(f"произошла ошибка ввода данных {ex}")

    return greeting


def filtr_transction_by_date(data_dict: list[dict], date:str)->list[dict]:
    """фильтрует транзакции совершенные в период с начала месяца до заданной даты"""
    utils_logger.info(f"Определяет дату старта периода для выбора данных")
    start_date = get_start_of_period(date, data_range='M')
    utils_logger.info(f"Определяет дату окончания периода для выбора данных")
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    utils_logger.info(f"выбирает транзакции проведенные в период от {start_date} до {end_date}")
    filtr_list_transaction = []
    for item in data_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if start_date <= format_date <= end_date:
            filtr_list_transaction.append(item)
    return filtr_list_transaction

def filtr_transction_by_period(data_dict: list[dict], date_start:datetime, date_end:str)->list[dict]:
    """фильтрует транзакции совершенные в  заданный период"""
    date_end = datetime.datetime.strptime(date_end, "%Y-%m-%d %H:%M:%S")
    filtr_list_transaction = []
    utils_logger.info(f"выбирает транзакции проведенные в период от {date_start} до {date_end}")
    for item in data_dict:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start<= format_date <= date_end:
            filtr_list_transaction.append(item)
    return filtr_list_transaction


def filtr_operation_with_cashback(sum_operation:float)->float:
    """фильтрует расходы по карте для расчета кэшбека"""
    if sum_operation < 0:
        spent = sum_operation
    else:
        spent = 0
    return spent


def agregate_transaction_card(data_dict: list[dict])->list[dict]:
    """агрегирует транзакции по карте, выводит сумм транзакций по каждой карте и размер кэшбека"""
    df = pd.DataFrame(data_dict)
    df['расходы по карте'] = df.apply(lambda x: filtr_operation_with_cashback(x['Сумма операции']), axis=1)
    utils_logger.info(f"рассчитывает кэшбек")
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
    utils_logger.info(f"определяет 5 транзакциий по сумме операций за период")
    top_five_trans = df_sorted.loc[[0,1,2,3,4]]
    extract_data = top_five_trans[['Дата операции', 'Сумма операции', 'Категория', 'Описание']]
    result = extract_data.to_dict('records')
    return result

def filtr_only_expenses(data_dict: list[dict])->list[dict]:
    """сортирует расходы по карте от поступлений"""
    df = pd.DataFrame(data_dict)
    utils_logger.info(f"фильтрует операции расхода")
    df_only_expenses = df[df['Сумма платежа'] < 0]
    return df_only_expenses

def filtr_only_income(data_dict):
    """выделяет поступления от расходов"""
    df = pd.DataFrame(data_dict)
    utils_logger.info(f"фильтрует операции поступлений")
    df_only_income = df[df['Сумма платежа'] > 0]
    return df_only_income.reset_index()

def get_sum_by_column(data_df, name_column='Сумма платежа'):
    """расситывает общую сумму по заданному столбцу"""
    data_df.rename(columns={name_column: "sum_by_column"}, inplace=True)
    return abs(round(data_df.sum_by_column.sum(),0))

def group_sort_by_category(data_df):
    """группирует расходы по катергориям и сортирует по сумме расходов"""
    data_df.rename(columns={'Категория': "category", 'sum_by_column': "amount" }, inplace=True)
    utils_logger.info(f"группирует расходы по категориям")
    df_sum_by_category = data_df.groupby("category", as_index=False).agg({"amount": 'sum'})
    utils_logger.info(f"сортирует сгруппированнные расхаоды по сумме")
    sort_for_main = df_sum_by_category.sort_values(by='amount', ascending= True, ignore_index=True)
    return sort_for_main

def group_sort_by_category_income(data_df):
    """группирует расходы по катергориям и сортирует по сумме расходов"""
    data_df.rename(columns={'Категория': "category", 'sum_by_column': "amount" }, inplace=True)
    utils_logger.info(f"группирует поступления по категориям")
    df_sum_by_category = data_df.groupby("category", as_index=False).agg({"amount": 'sum'})
    utils_logger.info(f"сортирует сгруппированнные поступления по сумме")
    sort_for_main = df_sum_by_category.sort_values(by='amount', ascending= False, ignore_index=True)
    return sort_for_main

def extract_main_category(data_df, name_category_1, name_category_2):
    """формирует список категорий относящихся к main,исключая категории заданные категории"""
    utils_logger.info(f"формирует список категорий относящихся к main")
    main_category = data_df.loc[~data_df.category.isin([name_category_1, name_category_2])]
    main_category = main_category.reset_index()
    return main_category


def top_seven_category_main(data_df):
    """выделяет 7 категорий по сумме платежа, формирует словарь с данными"""
    utils_logger.info(f"выделяет 7 категорий по сумме платежа")
    top_seven_category = data_df.head(7)
    other_category = data_df.loc[7:, ['category', 'amount']].agg({'amount': 'sum'})
    res_dict = []
    for index, row in top_seven_category.iterrows():
        categ_7 = dict()
        categ_7["category"] = row["category"]
        categ_7["amount"] = abs(round(row['amount'],0))
        res_dict.append(categ_7)

    other = dict()
    other["category"] = "Остальное"
    other["amount"] = abs(round(other_category['amount'], 0))
    res_dict.append(other)

    return res_dict

def category_main_income(data_df):
    """формирует словарь с данными о поступлениях"""
    utils_logger.info(f"формирует словарь с данными о поступлениях")
    res_dict = []
    for index, row in data_df.iterrows():
        income_main = dict()
        income_main["category"] = row["category"]
        income_main["amount"] = round(row['amount'],0)
        res_dict.append(income_main)
    return res_dict


def extract_transfer_and_cash_category(data_df, name_category_1, name_category_2):
    """фильтрует категории по заданному названию и формирует словарь с данными"""
    utils_logger.info(f"фильтрует категории по заданному названию и формирует словарь с данными")
    transfer_and_cash = data_df.loc[data_df.category.isin([name_category_1, name_category_2])]
    transfer_and_cash= transfer_and_cash.reset_index()
    res_dict = []
    for index, row in transfer_and_cash.iterrows():
        trans_cash = dict()
        trans_cash["category"] = row["category"]
        trans_cash["amount"] = abs(round(row['amount'], 0))
        res_dict.append(trans_cash)
    return res_dict


def get_currency_rate()->list:
    """функция запрашивает и возвращает актуальный курс валют установленных пользвателем"""
    load_dotenv(".env")
    API_KEY_currency = os.getenv("API_KEY_currency")

    data_settings = get_users_settings('../user_settings.json')
    user_curencies = data_settings['user_currencies']

    rates_currency_list = []

    for currency in user_curencies:
        rates_currency_dict = {}

        utils_logger.info(f"запрашивает курс валюты {currency}")
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

        utils_logger.info(f"запрашивает курс акции {stock}")
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









