import akshare as ak

#print(ak.__version__)
# 上海证劵交易所
# 获取上交所每日概况数据
# 目标：单次返回最近交易日的股票数据总貌(当前交易日的数据需要交易所收盘后统计)。
print("--------------获取上交所每日概况数据-------------------------")
sse_summary = ak.stock_sse_summary()
print(sse_summary)

# 深圳证劵交易所
# 
# 目标：单次返回指定 date 的市场总貌数据 - 证券类别统计 (当前交易日的数据需要交易所收盘后统计)
print("--------------证券类别统计-------------------------")
stock_szse_summary_df = ak.stock_szse_summary (date="20250815")
print (stock_szse_summary_df)
# 证券类别统计  
# 目标：单次返回指定 date 的市场总貌数据 - 证券类别统计 (当前交易日的数据需要交易所收盘后统计)
print("-------------------证券类别统计 --------------------")
stock_szse_sector_summary_df = ak.stock_szse_sector_summary (symbol="当年", date="202501")
print (stock_szse_sector_summary_df)

# 上海证券交易所 - 每日概况
# 目标：单次返回指定日期的每日概况数据，当前交易日数据需要在收盘后获取；注意仅支持获取在 20211227（包含）之后的数据。
print("-------------------上海证券交易所 - 每日概况 --------------------")
stock_sse_deal_daily_df = ak.stock_sse_deal_daily (date="20250325")
print (stock_sse_deal_daily_df)

print("-------------------获取股票日线数据 - 贵州茅台，代码600519 --------------------")
# 获取指定股票代码（如贵州茅台，代码600519）的日线数据
# 可以根据实际需求修改开始日期和结束日期
stock_code = "sh600519"
start_date = "20250201"
end_date = "20250831"
df = ak.stock_zh_a_daily(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
print(df.head())
