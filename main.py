
from src.func_get_data import read_excel_file


def main():
    df = read_excel_file('data/operations.xlsx')
    return df





if __name__ == '__main__':
    main()
