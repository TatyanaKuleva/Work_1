import json
import datetime
import pandas as pd
import logging
import calendar
from src.func_get_data import read_excel_file

df = read_excel_file('../data/operations.xlsx')

def favorable_categories_of_increased_cashback(data, year, month):
    year_dt = datetime.datetime.strptime(year, "%Y")
    month_abb = ""
    if month == 'январь':
        month_abb = 'Jan'
    elif month == "февраль":
        month_abb = 'Feb'
    elif month == "март":
        month_abb = 'Mar'
    elif month == "апрель":
        month_abb = 'Apr'
    elif month == "май":
        month_abb = 'May'
    elif month == "июнь":
        month_abb = 'Jun'
    elif month == "июль":
        month_abb = 'Jul'
    elif month == "август":
        month_abb = 'Aug'
    elif month == "сентябрь":
        month_abb = 'Sep'
    elif month == "октябрь":
        month_abb = 'Oct'
    elif month == "ноябрь":
        month_abb = 'Nov'
    elif month == "декабрь":
        month_abb = 'Dec'
    month_dt = datetime.datetime.strptime(month_abb, "%b")
    date_start = datetime.datetime(year = year_dt.year, month=month_dt.month, day=1)
    date_end = datetime.datetime(year = year_dt.year, month=month_dt.month,
                                 day=calendar.monthrange(year_dt.year, month=month_dt.month)[1], hour=23, minute=59)

    filtr_list_transaction = []
    for item in data:
        format_date = datetime.datetime.strptime(item['Дата операции'], "%d.%m.%Y %H:%M:%S")
        if date_start <= format_date <= date_end:
            filtr_list_transaction.append(item)

    df_filtr_list_transaction = pd.DataFrame(filtr_list_transaction)

    df_sum_by_cashback = df_filtr_list_transaction.groupby("Категория", as_index=False).agg({"Кэшбэк": 'sum'})
    sort_for_sum_cashback = df_sum_by_cashback.sort_values(by='Кэшбэк', ascending=False, ignore_index=True).head(3)
    # result = sort_for_sum_cashback.to_dict('records')
    res_dict = []
    for index, row in sort_for_sum_cashback.iterrows():
        categ = dict()
        categ[row["Категория"]] = row["Кэшбэк"]
        res_dict.append(categ)

    json_result = json.dumps(res_dict, indent=4, ensure_ascii=False)

    return json_result


if __name__ == '__main__':
    print(favorable_categories_of_increased_cashback(df, '2021', 'декабрь'))
    # print(type(favorable_categories_of_increased_cashback(df, '2022', 'август')))