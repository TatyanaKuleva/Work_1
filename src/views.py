import json
import requests
import datetime
from func_get_data import read_excel_file
import pandas as pd


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

def agregate_transaction_card(data_dict: list[dict])->list[dict]:
    """агрегирует транзакции по карте, выводит суммы и размер кэшбека по каждой карте"""
    df = pd.DataFrame(data_dict)

                      # columns=['Дата операции', 'Дата платежа', 'Номер карты', 'Статус',
                      #                                  'Сумма операции',  'Валюта операции', 'Сумма платежа',
                      #                                  'Валюта платеж', 'Кэшбэк', 'Категория', 'MCC', 'Описание',
                      #                                  'Бонусы (включая кэшбэк)', 'Округление на инвесткопилку',
                      #                                  'Сумма операции с округлением'])
    return df

    # df = pd.DataFrame(list(dict_data.items()), columns=['Date', 'Value'])
    # # Группировка данных по странам
    # country_grouped = df.groupby('country')
    #
    # # Вычисление средней цены для каждой страны
    # mean_price_by_country = country_grouped['price'].mean()
    # print(mean_price_by_country)




if __name__ == '__main__':
    print(agregate_transaction_card(transaction))


