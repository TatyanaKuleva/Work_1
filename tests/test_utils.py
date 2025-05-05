import pandas as pd
import pytest
from unittest.mock import patch, Mock
import requests
from src.utils import (get_start_of_period, greeting_user, filtr_transction_by_date, filtr_operation_with_cashback,
                       agregate_transaction_card, get_top_transaction, get_currency_rate, get_stocks_rate, filtr_only_expenses,
                       filtr_only_income, get_sum_by_column, group_sort_by_category, extract_main_category,
                       top_seven_category_main, category_main_income, extract_transfer_and_cash_category)
from src.func_get_data import get_users_settings

from dotenv import load_dotenv
import  os

from tests.conftest import get_list_dict_for_expensies, result_list_dict_only_expenses

load_dotenv(".env")
API_KEY = os.getenv("API_KEY")

def test_get_date_right_result(get_str_date, result_str_date):
    assert get_start_of_period(get_str_date) == result_str_date

def test_greeting_user(get_str_date):
    assert greeting_user(get_str_date) == 'Добрый день'

def test_filtr_transction_by_date(get_data_dict, get_str_date, result_data_dict):
    assert filtr_transction_by_date(get_data_dict, get_str_date) == result_data_dict

@pytest.mark.parametrize("get_sum_operation, result_sum_operation", [(12100.0, 0),(0.0, 0.0),(-12100.0, -12100.0)])
def test_filtr_operation_with_cashback(get_sum_operation, result_sum_operation):
    assert filtr_operation_with_cashback(get_sum_operation) == result_sum_operation

def test_agregate_transaction_card(get_list_dict_by_card_trans, result_list_dict_by_card_trans):
    assert agregate_transaction_card(get_list_dict_by_card_trans) == result_list_dict_by_card_trans

def test_get_top_transaction(get_list_dict_by_top_five_trans, result_list_dict_by_top_five_trans):
    assert get_top_transaction(get_list_dict_by_top_five_trans) == result_list_dict_by_top_five_trans



@pytest.mark.parametrize("data_dict, expected_df", [
    ( [{'Категория': 'Переводы', 'Описание': 'На р/с ООО "ФОРТУНА"', 'Дата операции': '15.07.2019 20:00:31', 'Сумма платежа': -50000.0},
            {'Категория': 'nan', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 17:44:36', 'Сумма платежа': -17000.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 18:14:20', 'Сумма платежа': 17000.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '06.07.2019 21:08:09', 'Сумма платежа': 13000.0},
            ],
      pd.DataFrame({'Категория': ['Переводы', 'nan'], 'Описание': ['На р/с ООО "ФОРТУНА"', 'Перевод с карты'],
                    'Дата операции': ['15.07.2019 20:00:31', '01.07.2019 17:44:36'],
                    'Сумма платежа': [-50000.0, -17000.0]})
      )])


def test_filtr_only_expenses(data_dict, expected_df):
    pd.testing.assert_frame_equal(filtr_only_expenses(data_dict), expected_df)


@pytest.mark.parametrize("data_dict, expected_df", [
    ( [{'Категория': 'Переводы', 'Описание': 'На р/с ООО "ФОРТУНА"', 'Дата операции': '15.07.2019 20:00:31', 'Сумма платежа': -50000.0},
            {'Категория': 'nan', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 17:44:36', 'Сумма платежа': -17000.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 18:14:20', 'Сумма платежа': 17000.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '06.07.2019 21:08:09', 'Сумма платежа': 13000.0},
            ],
      pd.DataFrame({'index': [2,3], 'Категория': ['Пополнения', 'Пополнения'], 'Описание': ['Перевод с карты', 'Перевод с карты'],
                    'Дата операции': ['01.07.2019 18:14:20', '06.07.2019 21:08:09'],
                    'Сумма платежа': [17000.0, 13000.0]})
      )])

def test_filtr_only_income(data_dict, expected_df):
    pd.testing.assert_frame_equal(filtr_only_income(data_dict), expected_df)

@pytest.mark.parametrize("data_dict, result", [
    (pd.DataFrame({'Категория':['Переводы', 'nan'], 'Описание': ['На р/с ООО "ФОРТУНА"', 'Перевод с карты'], 'Дата операции': ['15.07.2019 20:00:31','01.07.2019 17:44:36'],
                         'Сумма платежа': [-50000.0, -17000.0]}),
      67000),
(pd.DataFrame({'Категория':['Переводы', 'nan'], 'Описание': ['На р/с ООО "ФОРТУНА"', 'Перевод с карты'], 'Дата операции': ['15.07.2019 20:00:31','01.07.2019 17:44:36'],
                         'Сумма платежа': [17000.0, 13000.0]}),
      30000)
])
def test_get_sum_by_column(data_dict, result):
   assert get_sum_by_column(data_dict) == result

def test_group_sort_by_category(get_df_trans, result_df_sorted):
   pd.testing.assert_frame_equal(group_sort_by_category(get_df_trans), result_df_sorted)

def test_extract_main_category(get_df_trans_for_filtr, filtr_main_category):
   pd.testing.assert_frame_equal(extract_main_category(get_df_trans_for_filtr,
                                                       'Пополнения', 'Аптеки'),
                                 filtr_main_category)
def test_top_seven_category_main(get_top_seven_category_mai, result_top_seven_category_mai):
   assert top_seven_category_main(get_top_seven_category_mai) == result_top_seven_category_mai

def test_category_main_income(get_top_seven_category_mai, result_income_category_main):
   assert category_main_income(get_top_seven_category_mai) == result_income_category_main

def test_extract_transfer_and_cash_category(get_extract_transfer_cash, result_extract_transfer_cash):
   assert extract_transfer_and_cash_category(get_extract_transfer_cash, 'Аптеки', 'Пополнения') == result_extract_transfer_cash

@patch('src.utils.get_users_settings')
@patch('requests.get')
def test_get_currency_rate(mock_1, mock_2):
    mock_1.return_value.json.return_value = {"result": 80.00}
    mock_2.return_value = {"user_currencies": ["USD", "EUR"]}
    result = get_currency_rate()
    assert result == [{'currency': 'USD', 'rate': 80.00}, {'currency': 'EUR', 'rate': 80.00}]

@patch('src.utils.get_users_settings')
@patch('requests.get')
def test_get_stocks_rate(mock_1, mock_2):
    mock_1.return_value.json.return_value = {'data':[{'close':95}]}
    # "data"][0]["close"]
    mock_2.return_value = {"user_stocks": ["AAPL", "AMZN", "GOOGL"]}
    result = get_stocks_rate()
    assert result == [{'stock': 'AAPL', 'price': 95}, {'stock': 'AMZN', 'price': 95}, {'stock': 'GOOGL', 'price': 95}]





