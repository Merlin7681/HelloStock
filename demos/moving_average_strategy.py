import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tushare as ts

# 1. 设置Tushare token（需要先在tushare官网注册获取）
ts.set_token('你的token')  # 替换为你的实际token
pro = ts.pro_api()

# 2. 获取股票数据（以贵州茅台为例，代码600519.SH）
def get_stock_data(code, start_date, end_date):
    # 获取日线数据
    df = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
    # 转换日期格式并按时间排序
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)
    # 重命名列便于后续处理
    df = df.rename(columns={
        'trade_date': 'date',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'vol': 'volume'
    })
    return df

# 获取2020-2023年数据
stock_data = get_stock_data('600519.SH', '20200101', '20231231')
print("数据样例：")
print(stock_data[['date', 'open', 'close']].head())

# 3. 计算均线指标（5日均线和20日均线）
stock_data['ma5'] = stock_data['close'].rolling(window=5).mean()  # 5日均线
stock_data['ma20'] = stock_data['close'].rolling(window=20).mean()  # 20日均线

# 4. 定义交易信号：5日均线上穿20日均线为买入信号，下穿为卖出信号
stock_data['signal'] = 0  # 0表示无信号，1表示买入，-1表示卖出
# 金叉：ma5从下往上穿过ma20
stock_data.loc[stock_data['ma5'] > stock_data['ma20'], 'signal'] = 1
# 死叉：ma5从上往下穿过ma20
stock_data.loc[stock_data['ma5'] < stock_data['ma20'], 'signal'] = -1
# 只保留信号变化的点（避免连续重复信号）
stock_data['position'] = stock_data['signal'].diff()

# 5. 回测策略：计算累计收益
# 假设初始资金100万，每次全仓操作
initial_capital = 1000000
stock_data['cash'] = initial_capital
stock_data['holdings'] = 0  # 持仓市值
stock_data['total_asset'] = initial_capital  # 总资产（现金+持仓）

for i in range(1, len(stock_data)):
    # 前一天的数据
    prev = stock_data.iloc[i-1]
    current = stock_data.iloc[i]
    
    # 继承前一天的资产状态
    stock_data.at[i, 'cash'] = prev['cash']
    stock_data.at[i, 'holdings'] = prev['holdings']
    
    # 买入信号：position=1且前一天没有持仓
    if prev['position'] == 1 and prev['holdings'] == 0:
        # 计算可买股数（不考虑手续费）
        shares = int(prev['cash'] / current['open'])
        stock_data.at[i, 'holdings'] = shares * current['open']
        stock_data.at[i, 'cash'] = prev['cash'] - shares * current['open']
    
    # 卖出信号：position=-1且前一天有持仓
    elif prev['position'] == -1 and prev['holdings'] > 0:
        stock_data.at[i, 'cash'] = prev['cash'] + prev['holdings']
        stock_data.at[i, 'holdings'] = 0
    
    # 计算总资产
    stock_data.at[i, 'total_asset'] = stock_data.at[i, 'cash'] + stock_data.at[i, 'holdings']

# 6. 可视化结果
plt.figure(figsize=(14, 8))

# 绘制价格和均线
ax1 = plt.subplot(2, 1, 1)
ax1.plot(stock_data['date'], stock_data['close'], label='收盘价', color='blue')
ax1.plot(stock_data['date'], stock_data['ma5'], label='5日均线', color='orange')
ax1.plot(stock_data['date'], stock_data['ma20'], label='20日均线', color='green')
# 标记买入卖出点
ax1.scatter(stock_data[stock_data['position'] == 1]['date'], 
            stock_data[stock_data['position'] == 1]['close'], 
            marker='^', color='red', label='买入')
ax1.scatter(stock_data[stock_data['position'] == -1]['date'], 
            stock_data[stock_data['position'] == -1]['close'], 
            marker='v', color='black', label='卖出')
ax1.legend()
ax1.set_title('股价与均线策略信号')

# 绘制总资产曲线
ax2 = plt.subplot(2, 1, 2)
ax2.plot(stock_data['date'], stock_data['total_asset'], label='策略总资产', color='purple')
ax2.axhline(y=initial_capital, color='gray', linestyle='--', label='初始资金')
ax2.legend()
ax2.set_title('策略收益曲线')

plt.tight_layout()
plt.show()

# 输出最终收益
final_asset = stock_data.iloc[-1]['total_asset']
print(f"初始资金：{initial_capital}元")
print(f"最终资产：{final_asset:.2f}元")
print(f"累计收益：{(final_asset - initial_capital)/initial_capital*100:.2f}%")
