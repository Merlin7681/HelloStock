import akshare as ak

stock_info = ak.stock_individual_info_em(symbol="000061")
print(stock_info)

financial_indicator = ak.stock_financial_abstract(symbol="000061")
print(financial_indicator)

profit_ability = ak.stock_financial_analysis_indicator(symbol="000061")
print(profit_ability)

stock_zh_a_spot = ak.stock_zh_a_spot()
stock_data = stock_zh_a_spot[stock_zh_a_spot['代码'] == "000061"]
print(stock_data)

stock_financial = ak.stock_financial_analysis_indicator(symbol="000061")
print(stock_financial)
