import json
import requests
import datetime
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

def aregate_transsction_card(data_dict: list[dict])->list[dict]:
    """агрегирует транзакции по карте, выводит суммы и размер кэшбека по каждой карте"""





if __name__ == '__main__':
    print(filtr_transction_by_date(transaction,'2021-12-25 19:06:39'))


