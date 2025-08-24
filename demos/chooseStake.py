import akshare as ak
import pandas as pd

print(dir(ak))

# 获取沪深A股上市公司利润表数据
#profit_data = ak.stock_financial_report_profit()
#profit_data = ak.stock_profit_forecast()
profit_data = ak.stock_profit_sheet_by_yearly_em(symbol="SH600519")
print(profit_data)
# 将数据转换为DataFrame格式
profit_df = pd.DataFrame(profit_data)

# 获取沪深A股上市公司资产负债表数据
balance_sheet_data = ak.stock_financial_report_balance()
balance_sheet_df = pd.DataFrame(balance_sheet_data)

# 获取沪深A股上市公司现金流量表数据
cash_flow_data = ak.stock_financial_report_cash_flow()
cash_flow_df = pd.DataFrame(cash_flow_data)

# 计算部分估值和盈利能力指标
# 假设股票代码为'code'，市值数据需要另外获取，这里为了简化示例，不真实计算市值
# 计算市盈率（PE），假设净利润为'net_profit'列，实际使用需调整
profit_df['pe'] = 100 / profit_df['net_profit']  
# 计算市净率（PB），假设净资产为'total_equities'列，实际使用需调整
balance_sheet_df['pb'] = 100 / balance_sheet_df['total_equities']  
# 计算净资产收益率（ROE），假设净利润为'net_profit'，平均净资产需更准确计算，这里简化处理
profit_df['roe'] = profit_df['net_profit'] / balance_sheet_df['total_equities'] * 100  

# 筛选优质股票示例：选择市盈率小于20，市净率小于2，净资产收益率大于15%的股票
selected_stocks = profit_df[(profit_df['pe'] < 20) & (balance_sheet_df['pb'] < 2) & (profit_df['roe'] > 15)]['code'].tolist()
print("筛选出的优质股票代码：", selected_stocks)
