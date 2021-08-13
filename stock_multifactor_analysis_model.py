import sys
sys.path.append('..')

from client import interact_mysql_client as mc
import pandas as pd
import numpy as np

from tools import excel_format as ef
from tools import query_decorator as qd

sql_path = r'/Users/zorro/STOCK/sql/'
stock_path = r'/Users/zorro/STOCK/stock_tableau_file/'

years = 3
indexname = '通信'

def UpdateStockList():
    drop_sql = 'drop table if exists stock.stock_multifactor_score_evaluation'
    mc.run_ddl(drop_sql)

    f_create_sql = mc.get_sql_text(sql_path + r'stock_multifactor_weight_evaluation.sql')
    create_sql = f_create_sql.format(years, indexname)
    print(create_sql)
    mc.run_ddl(create_sql)
    print('table stock_multifactor_score_evaluation created!')

def StockAnalysisModel():
    sql = '''
        select
            *
        from stock.stock_multifactor_score_evaluation as s
    '''

    stocklist = mc.run(sql)

    stocklist['income_cagr'] = stocklist['income_growth_rate'].apply(lambda x: np.real(pow(x, 1 / years)) - 1)
    stocklist['profit_cagr'] = stocklist['netprofit_growth_rate'].apply(lambda x: np.real(pow(x, 1 / years)) - 1)
    stocklist['eps_cagr'] = stocklist['eps_growth_rate'].apply(lambda x: np.real(pow(x, 1 / years)) - 1)

    cagr_pcent = 50
    # income_cagr_pcent = np.percentile(stocklist['income_cagr'], cagr_pcent)
    # profit_cagr_pcent = np.percentile(stocklist['profit_cagr'], cagr_pcent)
    eps_cagr_pcent = np.percentile(stocklist['eps_cagr'], cagr_pcent)

    # case1 = stocklist.income_cagr > income_cagr_pcent
    # case2 = stocklist.profit_cagr > profit_cagr_pcent
    case3 = stocklist.eps_cagr >= eps_cagr_pcent
    case4 = stocklist.eps_cagr > 0
    stocklist_filter = stocklist.loc[(case3 & case4)]

    print(stocklist_filter.iloc[-1, :])

    ef.format_excel_file(stocklist_filter,
                         path=stock_path+r'stock_multifactor_analysis_model.xlsx',
                         sheet_name='multifactor',
                         )
    return stocklist_filter

if __name__ == '__main__':
    # UpdateStockList()
    StockAnalysisModel()
