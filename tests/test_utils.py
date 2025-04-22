from unittest.mock import patch, Mock
import requests
from src.utils import get_start_of_period, greeting_user, filtr_transction_by_date
from dotenv import load_dotenv
import  os

def test_get_date_right_result(get_str_date, result_str_date):
    assert get_start_of_period(get_str_date) == result_str_date

def test_greeting_user(get_str_date):
    assert greeting_user(get_str_date) == 'Добрый день'

def test_filtr_transction_by_date(get_data_dict, get_str_date, result_data_dict):
    assert filtr_transction_by_date(get_data_dict, get_str_date) == result_data_dict