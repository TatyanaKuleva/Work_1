import pandas as pd
import pytest

from src.reports import spending_by_category, spending_by_weekday, spending_by_workday

def test_spending_by_category(get_df_trans_for_reports, result_reports_by_category):
    assert spending_by_category(get_df_trans_for_reports, 'Аптеки', '31.12.2021 16:44:00') == result_reports_by_category

def test_spending_by_weekday(get_df_trans_for_reports, result_reports_by_weekday):
    assert spending_by_weekday(get_df_trans_for_reports, '31.12.2021 16:44:00') == result_reports_by_weekday

def test_spending_by_workday(get_df_trans_for_reports, result_reports_by_workday):
    assert spending_by_workday(get_df_trans_for_reports, '31.12.2021 16:44:00') == result_reports_by_workday


