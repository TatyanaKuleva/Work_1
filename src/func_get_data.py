import pandas as pd
import json

def read_excel_file(path):
    """ считывание данных о транзакциях из EXCEL файла"""
    df =  pd.read_excel(path)
    result_dict = df.to_dict(orient='records')
    return result_dict


def get_users_settings(path: str) -> dict:
    """функция принимает путь до JSON-файла и возвращает список словарей с данными о настройках пользователя"""
    with open(path, "r", encoding="utf-8") as data_file:
        data_settings = json.load(data_file)
        return data_settings





if __name__ == "__main__":
    print(read_excel_file('../data/operations.xlsx'))



