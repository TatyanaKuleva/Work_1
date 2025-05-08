import pandas as pd
import pytest

from unittest.mock import patch, Mock
from src.services import (favorable_categories_of_increased_cashback, investment_bank, basic_search, phone_search,
                          search_transfer_for_individuals)
from tests.conftest import result_invest_first


def test_favorable_categories_of_increased_cashback(get_data_dict_with_cashback, result_data_dict_with_cashback ):
    assert (favorable_categories_of_increased_cashback(get_data_dict_with_cashback, '2019', 'июль')
            == result_data_dict_with_cashback)


@pytest.mark.parametrize("month, df, limit", [
    ('2019-07',[{'Категория': 'Переводы', 'Описание': 'На р/с ООО "ФОРТУНА"', 'Дата операции': '15.07.2019 20:00:31', 'Сумма операции': -50000.0, 'Кэшбэк': 500.0},
            {'Категория': 'nan', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 17:44:36', 'Сумма операции': -17000.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '01.07.2019 18:14:20', 'Сумма операции': 17000.0, 'Кэшбэк': 170.0},
            {'Категория': 'Пополнения', 'Описание': 'Перевод с карты', 'Дата операции': '06.07.2019 21:08:09', 'Сумма операции': 13000.0},
            {'Категория': 'Наличные', 'Описание': 'Снятие в банкомате Альфа-Банк', 'Дата операции': '06.07.2019 14:08:29',
             'Сумма операции': -3001.0, 'Кэшбэк': 30}], 100)])
def test_investment_bank(month, df, limit, result_invest_first):
   assert investment_bank(month, df, limit) == result_invest_first


def test_basic_search_first(get_data_dict_with_cashback, result_search_first):
    assert basic_search(get_data_dict_with_cashback, 'перевод') == result_search_first

def test_basic_search_second(get_data_dict_with_cashback, result_search_second):
    assert basic_search(get_data_dict_with_cashback, 'фортуна') == result_search_second

def test_phone_search(get_data_search_phone, result_search_phone):
    assert phone_search(get_data_search_phone) == result_search_phone

def test_search_transfer_for_individuals(get_data_for_search_transfers, result_search_for_transfers):
    assert search_transfer_for_individuals(get_data_for_search_transfers) == result_search_for_transfers