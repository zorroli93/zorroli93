import sys, os
base_dir = os.path.dirname(__file__)
base_dir = os.path.dirname(base_dir)
print(f'base dir: \n====================\n{base_dir}')
sys.path.append(base_dir)

from client import interact_mysql_client as mc
# from client import interact_mongo_client as mo
import numpy as np
import pandas as pd
from tools import excel_format as ef
from tools import query_decorator as qd
import time

from pprint import pprint
pd.set_option("display.max_columns", None)

sql_path = r'/Users/zorro/STOCK/sql/'
desk_path = r'/Users/zorro/tmpFiles/'
stock_path = r'/Users/zorro/STOCK/stock_tableau_file/'

class EvalStockFactorModel():
    def __init__(self):
        pass

    def RawStocklist(self, sw_indexname):
        # 查询申万一级行业股票池
        f_sql = mc.get_sql_text(sql_path+r'stock_basic_financial_report_research_weight_by_sw_indexname.sql')
        sql = f_sql.format(sw_indexname)

        stock_list = mc.run(sql)
        # stock_list.fillna(0, inplace=True)

        print()
        print(f'一共获得{len(stock_list)}只股票')

        # stock_list.dropna(how='any', inplace=True)
        ef.format_excel_file(stock_list, path=stock_path+r'stock_raw_list.xlsx', sheet_name='stock')
        pprint(stock_list.head(5))
        return stock_list

    def PercentileRank(self, stock_list):
        # 每股收益变异系数，作为筛选条件
        basic_eps_cv30 = round(np.percentile(stock_list['basic_eps_cv'], 30), 2)
        basic_eps_cv70 = round(np.percentile(stock_list['basic_eps_cv'], 70), 2)

        # 每股收益平均值，作为筛选条件
        basic_eps_avg50 = round(np.percentile(stock_list['basic_eps_avg'], 50), 2)

        print('三个重要参数：')
        print(basic_eps_cv30, basic_eps_cv70, basic_eps_avg50, '\n')
        case1 = stock_list.basic_eps_cv >= basic_eps_cv30
        case2 = stock_list.basic_eps_cv <= basic_eps_cv70
        case3 = stock_list.basic_eps_avg >= basic_eps_avg50

        # 变异系数不能太小，太小意味着没有增长
        # 变异系数不能太大，太大意味着增长不稳定
        stock_list_filter = stock_list.loc[case1 & case2 & case3]

        print(f'筛选获得{len(stock_list_filter)}只股票')
        ef.format_excel_file(stock_list_filter, path=stock_path+r'stock_percentile_list.xlsx', sheet_name='factor')
        stock_list_filter.head()
        return stock_list_filter

    def StockListOutput(self, data):
        # 剔除倒数两行的每股收益数据列
        stock_datas = data.iloc[:, :-2].copy()

        # 权重得分标准化
        stock_datas.revenue_factor  = round(stock_datas.revenue_factor.transform(lambda x: (x - x.mean()) / x.std()), 2)
        stock_datas.profit_factor   = round(stock_datas.profit_factor.transform(lambda x: (x - x.mean()) / x.std()), 2)
        stock_datas.GM_factor       = round(stock_datas.GM_factor.transform(lambda x: (x - x.mean()) / x.std()), 2)
        stock_datas.roe_factor      = round(stock_datas.roe_factor.transform(lambda x: (x - x.mean()) / x.std()), 2)
        stock_datas.eps_factor      = round(stock_datas.eps_factor.transform(lambda x: (x - x.mean()) / x.std()), 2)
        stock_datas.cashflow_factor = round(stock_datas.cashflow_factor.transform(lambda x: (x - x.mean()) / x.std()), 2)

        # 等权重求和评分
        stock_datas['total_score'] = stock_datas.iloc[:, 4:].sum(axis=1)

        # 计算各项因子得分的标准差，剔除波动高的股票
        stock_datas['factor_std'] = [round(stock_datas.iloc[i, 4:10].std(), 2) for i in range(len(stock_datas))]
        # 选择因子得分的标准差在80分位数以下的股票
        factor_std_bench = round(np.percentile(stock_datas['factor_std'], 100), 2)
        stock_datas['factor_std_bench'] = factor_std_bench

        # 确认因子分项得分的波动性筛选条件
        factor_filter_case = stock_datas['factor_std'] <= factor_std_bench
        stock_datas = stock_datas.loc[factor_filter_case]

        stock_datas = stock_datas.sort_values(by=['total_score'], ascending=0)
        stock_datas['score_rank'] = stock_datas.total_score.rank(ascending=0)
        stock_datas['etldate'] = time.strftime('%Y-%m-%d')
        # ef.format_excel_file(stock_datas, path=desk_path+'infrastructure_stock_eval_list.xlsx', sheet_name='stock')

        stock_datas.fillna('-', inplace=True)
        qd.load_into_mysql_server(stock_datas,
                        'stock',
                        'stock_financial_research_list',
                        server='local',
                        operation='replace')
        pprint(stock_datas.head(10))
        return stock_datas

if __name__ == '__main__':
    # 初始化
    smfmodel = EvalStockFactorModel()

    indexname = '食品饮料'
    stock_list = smfmodel.RawStocklist(indexname)
    filter_list = smfmodel.PercentileRank(stock_list)
    results = smfmodel.StockListOutput(filter_list)
