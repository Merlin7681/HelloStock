import matplotlib.pyplot as plt
import akshare as ak

import matplotlib.font_manager as fm

# 方法2：加载自定义字体文件（.ttf 或 .otf）
font_path = "../simhei.ttf"  # 字体文件路径
my_font = fm.FontProperties(fname=font_path)
plt.rcParams['font.sans-serif'] = ['SimHei']

# 使用自定义字体
#plt.title("标题", fontproperties=my_font)
#plt.xlabel("X轴", fontproperties=my_font)
#plt.show()

# 获取数据（注意调整日期参数）
szse_industry = ak.stock_szse_sector_summary(symbol="当年", date="202301")[1:] # 日期改为
    # 提取饼图数据
industry_names = szse_industry['项目名称']
trade_amount = szse_industry['成交金额-人民币元']

# 绘制饼图
plt.figure(figsize=(10, 8))
plt.pie(trade_amount,
        labels=industry_names,
        autopct='%1.1f%%',
        startangle=90)
plt.title('深交所行业成交金额占比（2023年）', fontproperties=my_font)
plt.axis('equal')  # 保持圆形
plt.legend(industry_names, loc='best', bbox_to_anchor=(1, 0.5))
plt.show()