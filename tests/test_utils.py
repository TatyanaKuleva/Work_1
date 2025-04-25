import pytest
from unittest.mock import patch, Mock
import requests
from src.utils import (get_start_of_period, greeting_user, filtr_transction_by_date, filtr_operation_with_cashback,
                       agregate_transaction_card, get_top_transaction, get_currency_rate, get_stocks_rate)
from src.func_get_data import get_users_settings

from dotenv import load_dotenv
import  os

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

# @patch('src.func_get_data.get_users_settings')
# @patch('requests.get'

@patch('src.func_get_data.get_users_settings')
@patch('requests.get')
def test_get_currency_rate(mock_1, mock_2):
    # mock_response = Mock()
    mock_2.json.return_value = {"result": 80.00}
    mock_1.return_value = {"user_currencies": ["USD", "EUR"]}

    # with patch('requests.get', return_value=mock_response) as mock_get:

        # result = get_currency_rate()
        # assert result == [{'currency': 'USD', 'rate': 80.00}, {'currency': 'EUR', 'rate': 80.00}]
        # # mock_get.assert_called_once_with("1.0", "USD")

    # mocked_get_user_curr.return_value = {"user_currencies": ["USD", "EUR"]}
    # mock_get_1.return_value = {"result": '80.00'}
    # mock_get_2.return_value = {"result": '90.00'}
    #
    # load_dotenv(".env")
    # API_KEY_currency = os.getenv("API_KEY_currency")
    #
    # # with patch('requests.get', 'requests.get') as mock_get_1, mock_get_2:
    # #     mock_get_1.return_value.json.return_value = {"result": '80.00'}
    # #     mock_get_2.return_value.json.return_value = {"result": '90.00'}
    # #     assert get_currency_rate() == [{'currency': 'USD', 'rate': 80.00}, {'currency': 'EUR', 'rate': 90.00}]
    # # mock_user_settings = Mock()
    # # mock_user_settings = {"user_currencies": ["USD", "EUR"]}
    #
    result = get_currency_rate()
    assert result == [{'currency': 'USD', 'rate': 80.00}, {'currency': 'EUR', 'rate': 90.00}]
    # mock_user_settings.return_value = {"user_currencies": ["USD", "EUR"]}

    # with patch('requests.get', return_value=mock_response) as mock_get:
    #     result = get_currency_rate()
    #     assert result == [{'currency': 'USD', 'rate': 83.35}, {'currency': 'EUR', 'rate': 83.35}  ]
        # mock_get.assert_called_once_with()

# @patch('pandas.read_csv')
# def test_read_csv_valid_data(mock_read_csv):
#     mock_data = pd.DataFrame({'id': [1], 'amount': [100]})
#     mock_read_csv.return_value = mock_data
#     assert read_csv_file("test.csv") == [{'id': 1, 'amount': 100}]
