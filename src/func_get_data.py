import pandas as pd
import json

def read_excel_file(path):
    """ считывание данных о транзакциях из EXCEL файла"""
    df =  pd.read_excel(path)
    result_dict = df.to_dict(orient='records')
    return result_dict



if __name__ == "__main__":
    print(read_excel_file('../data/operations.xlsx'))



