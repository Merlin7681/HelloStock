import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import akshare as ak
import mplfinance as mpf  # 用于绘制K线图
import matplotlib.font_manager as fm

# 1. 获取股票数据（以贵州茅台为例，代码600519）
def get_stock_data(code, startDate, endDate):
    df = ak.stock_zh_a_daily(symbol="sh600000", adjust="qfq")
    
    # 使用AkShare获取A股日线数据
    stock_df = ak.stock_zh_a_daily(symbol=code, start_date=startDate, end_date=endDate, adjust="qfq")
    print(stock_df.head())
    
    # 重命名列以便后续处理
    stock_df = stock_df.rename(columns={
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume"
    })
    # 将日期转换为datetime格式并设置为索引
    stock_df['date'] = pd.to_datetime(stock_df['date'])
    stock_df = stock_df.set_index('date')
    return stock_df

# 获取2020-2023年数据
stock_code = "sh600519"
start_date = "20250201"
end_date = "20250831"
stock_data = get_stock_data(stock_code, start_date, end_date)
print("数据样例：")
print(stock_data.head())

# 2. 计算RSI指标（相对强弱指数）
def calculate_rsi(data, window=14):
    """计算RSI指标"""
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# 计算14日RSI
stock_data['rsi14'] = calculate_rsi(stock_data)

# 3. 定义交易信号：RSI<30为超卖（买入信号），RSI>70为超买（卖出信号）
stock_data['signal'] = 0  # 0表示无信号
stock_data.loc[stock_data['rsi14'] < 30, 'signal'] = 1  # 买入信号
stock_data.loc[stock_data['rsi14'] > 70, 'signal'] = -1  # 卖出信号

# 只保留信号变化的点（避免连续信号）
stock_data['position'] = stock_data['signal'].diff()

# 4. 回测策略
initial_capital = 1000000.0  # 初始资金100万，使用float类型
stock_data['cash'] = float(initial_capital)
stock_data['holdings'] = 0.0  # 持仓市值，使用float类型
stock_data['total_asset'] = float(initial_capital)  # 总资产，使用float类型

for i in range(1, len(stock_data)):
    prev = stock_data.iloc[i-1]
    current = stock_data.iloc[i]
    
    # 继承前一天的资产状态
    stock_data.iloc[i, stock_data.columns.get_loc('cash')] = prev['cash']
    stock_data.iloc[i, stock_data.columns.get_loc('holdings')] = prev['holdings']
    
    # 买入信号：RSI<30且未持仓
    if prev['position'] == 1 and prev['holdings'] == 0:
        shares = int(prev['cash'] / current['open'])  # 计算可买股数
        stock_data.iloc[i, stock_data.columns.get_loc('holdings')] = shares * current['open']
        stock_data.iloc[i, stock_data.columns.get_loc('cash')] = prev['cash'] - shares * current['open']
    
    # 卖出信号：RSI>70且有持仓
    elif prev['position'] == -1 and prev['holdings'] > 0:
        stock_data.iloc[i, stock_data.columns.get_loc('cash')] = prev['cash'] + prev['holdings']
        stock_data.iloc[i, stock_data.columns.get_loc('holdings')] = 0
    
    # 计算总资产
    stock_data.iloc[i, stock_data.columns.get_loc('total_asset')] = stock_data.iloc[i]['cash'] + stock_data.iloc[i]['holdings']

# 5. 可视化结果
plt.rcParams["font.family"] = ["Arial Unicode MS"]  # 使用支持中文的Arial Unicode MS字体
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题

# 绘制价格和RSI
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]})

# 价格曲线
ax1.plot(stock_data.index, stock_data['close'], label='收盘价', color='blue')
# 标记买卖点
ax1.scatter(stock_data[stock_data['position'] == 1].index, 
            stock_data[stock_data['position'] == 1]['close'], 
            marker='^', color='red', label='买入')
ax1.scatter(stock_data[stock_data['position'] == -1].index, 
            stock_data[stock_data['position'] == -1]['close'], 
            marker='v', color='green', label='卖出')
ax1.set_title('股价与交易信号')
ax1.legend()

# RSI曲线
ax2.plot(stock_data.index, stock_data['rsi14'], label='14日RSI', color='purple')
ax2.axhline(y=30, color='orange', linestyle='--', label='超卖线(30)')
ax2.axhline(y=70, color='red', linestyle='--', label='超买线(70)')
ax2.set_title('RSI指标')
ax2.legend()

plt.tight_layout()
plt.show()

# 绘制K线图（可选）
# 创建mplfinance的自定义样式，包含中文字体设置
mc = mpf.make_marketcolors(up='r', down='g', inherit=True)
s = mpf.make_mpf_style(marketcolors=mc, rc={'font.family': 'Arial Unicode MS'})

mpf.plot(stock_data[-100:], type='candle', mav=(5, 10), volume=True, 
         title='近期K线图', show_nontrading=False, style=s)

# 输出策略结果
final_asset = stock_data.iloc[-1]['total_asset']
print(f"初始资金：{initial_capital}元")
print(f"最终资产：{final_asset:.2f}元")
print(f"累计收益率：{(final_asset - initial_capital)/initial_capital*100:.2f}%")
    