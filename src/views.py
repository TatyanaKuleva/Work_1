import json

from src.func_get_data import read_excel_file, get_users_settings
from src.utils import (get_start_of_period, greeting_user, filtr_transction_by_date, filtr_operation_with_cashback,
                       agregate_transaction_card, get_top_transaction, get_currency_rate, get_stocks_rate,
                       filtr_transction_by_period, filtr_only_expenses, get_sum_by_column, group_sort_by_category,
                       extract_main_category, top_seven_category_main,extract_transfer_and_cash_category,
                       other_category_main_sum)


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

    start = get_start_of_period(date,data_range)

    filtr_transaction = filtr_transction_by_period(df, start, date)

    only_exp = filtr_only_expenses(filtr_transaction)

    # result = get_sum_by_column(only_exp)

    result_1 = group_sort_by_category(only_exp)

    result_2 = extract_main_category(result_1, 'Переводы', 'Наличные')

    result = top_seven_category_main(result_2)

    result_4 = other_category_main_sum(result_2)

    result_5 = get_sum_by_column(result_4, 'sum_pay')

    result_3= extract_transfer_and_cash_category(result_1, 'Переводы', 'Наличные')

    # result = result.to_dict('records')

    # json_res = json.dumps(result, indent=4, ensure_ascii=False)

    return result_5

if __name__ == '__main__':
    print(event('2019-07-17 15:05:27', 'Y'))
