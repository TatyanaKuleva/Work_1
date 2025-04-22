import pytest
import datetime

@pytest.fixture
def get_str_date():
    return '2019-07-17 15:05:27'

@pytest.fixture
def result_str_date():
    return datetime.datetime(2019, 7, 1, 0, 0)

@pytest.fixture
def get_data_dict():
    return [{'Дата операции': '17.07.2019 15:04:27'},
            {'Дата операции': '17.07.2019 15:04:27'},
            {'Дата операции': '17.08.2019 15:04:27'},
            {'Дата операции': '17.08.2019 15:04:27'}]

@pytest.fixture
def result_data_dict():
    return [{'Дата операции': '17.07.2019 15:04:27'},
            {'Дата операции': '17.07.2019 15:04:27'}]



