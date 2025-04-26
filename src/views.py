import json

from src.func_get_data import read_excel_file, get_users_settings
from src.utils import (get_start_of_period, greeting_user, filtr_transction_by_date, filtr_operation_with_cashback,
                       agregate_transaction_card, get_top_transaction, get_currency_rate, get_stocks_rate)


def main(current_date: str):
    df = read_excel_file('../data/operations.xlsx')
    result = dict()

    result["greeting"] = greeting_user(current_date)

    start = get_start_of_period(current_date)

    filtr = filtr_transction_by_date(df, current_date)

    dict_agregate = agregate_transaction_card(filtr)
    for item in dict_agregate:
        replacements = {'Номер карты': 'last_digits', 'расходы по карте': 'total_spent', 'cashback': 'cashback'}
        for i in item:
            if i in replacements:
                item[replacements[i]] = item.pop(i)
    result["cards"] = dict_agregate

    top_transaction = get_top_transaction(filtr)
    for item in top_transaction:
        replacements = {'Категория': 'category',
                        'Описание': 'description',
                        'Дата операции': 'date',
                        'Сумма операции': 'amount',
                        }
        for i in item:
            if i in replacements:
                item[replacements[i]] = item.pop(i)
    result["top_transactions"] = top_transaction

    data_rate_by_users_curr = get_currency_rate()
    result["currency_rates"] = data_rate_by_users_curr

    data_rate_by_users_stock = get_stocks_rate()
    result["stock_prices"] = data_rate_by_users_stock


    json_result = json.dumps(result, indent=4, ensure_ascii=False)

    return  json_result

def event(date:str, data_range='M'):
    df = read_excel_file('../data/operations.xlsx')
    result = dict()

    start = get_start_of_period(date)

    filtr = filtr_transction_by_date(df, date)

    json_res = json.dumps(filtr, indent=4, ensure_ascii=False)

    return json_res

if __name__ == '__main__':
    print(event('2019-07-17 15:05:27', 'W'))
