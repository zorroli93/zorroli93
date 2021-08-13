import sys
sys.path.append('..')

from client import interact_mysql_client as mc
import pandas as pd
import numpy as np
from tools import excel_format as ef
from tools import query_decorator as qd
from datetime import datetime
pd.set_option("display.max_columns", None)

sw_sector_path = r'/Users/zorro/FOF/sw_sector_index_files/'
zz_sector_path = r'/Users/zorro/FOF/zz_sector_pe_files/'
wy_hs_index_path = r'/Users/zorro/FOF/wy_hs_index_chg/'
sql_path = r'/Users/zorro/FOF/sql/'

def numeric_format(x):
    try:
        px = round(float(x), 3)
    except Exception as e:
        px = np.nan
    return px


def SW_DailyReport():
    sql = mc.get_sql_text(sql_path+r'sw_sector_index_chg/daily.sql')
    sw_sector_daily = mc.run(sql)
    sw_sector_daily.iloc[:, 3:] = sw_sector_daily.iloc[:, 3:].applymap(numeric_format)

    ef.format_excel_file(sw_sector_daily,
                        path=sw_sector_path+'sw_sector_daily.xlsx',
                        sheet_name='sector_daily')
    sw_sector_daily.head()

def SW_WeeklyReport():
    sql = mc.get_sql_text(sql_path+r'sw_sector_index_chg/weekly_page.sql')
    wk_report = mc.run(sql)

    # 将文本转化为数字，注意列的位置，日期回转换为空值
    # wk_report.iloc[:, 4:] = wk_report.iloc[:, 4:].applymap(numeric_format)

    ef.format_excel_file(wk_report,
                        path=sw_sector_path+'sw_sector_weekly.xlsx',
                        sheet_name='sector_weekly')
    print(wk_report.head())

def SW_MonthlyReport():
    sql = mc.get_sql_text(sql_path+r'sw_sector_index_chg/monthly.sql')
    sw_sector_monthly = mc.run(sql)
    sw_sector_monthly.iloc[:, 4:] = sw_sector_monthly.iloc[:, 4:].applymap(numeric_format)

    ef.format_excel_file(sw_sector_monthly,
                        path=sw_sector_path+'sw_sector_monthly.xlsx',
                        sheet_name='sector_monthly')
    sw_sector_monthly.head()

if __name__ == '__main__':
    SW_DailyReport()
    SW_WeeklyReport()
    SW_MonthlyReport()