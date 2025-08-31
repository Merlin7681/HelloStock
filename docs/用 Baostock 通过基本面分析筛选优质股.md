# 用 Baostock 通过基本面分析筛选优质股

通过 Baostock 数据源分析股票基本面并筛选优质股，核心是**利用其免费的财务数据、估值数据和交易数据，搭建多维度基本面评价体系**，结合量化逻辑排除风险股、聚焦高质地标的。以下是具体步骤、关键指标、实操代码及注意事项，适用于从数据获取到选股落地的全流程。

### 一、前期准备：明确 Baostock 的基本面数据范围

在分析前，需先了解 Baostock 能提供哪些核心基本面数据（免费且覆盖 A 股全市场），避免因数据缺失导致分析偏差。主要可用数据如下：



| 数据类别       | 核心指标示例                                     | 数据频率         | 用途             |
| ---------- | ------------------------------------------ | ------------ | -------------- |
| **财务数据**   | 净利润、ROE（净资产收益率）、ROA（资产回报率）、毛利率、资产负债率、经营现金流 | 年报 / 中报 / 季报 | 评估公司盈利、偿债、运营能力 |
| **估值数据**   | 市盈率（PE-TTM）、市净率（PB）、市销率（PS-TTM）            | 日度更新         | 判断股价是否高估 / 低估  |
| **公司基本信息** | 行业分类、上市时间、总股本、流通股本                         | 静态 / 季度更新    | 排除次新股、区分行业属性   |
| **分红数据**   | 分红总额、股息率                                   | 年度更新         | 筛选高分红、现金流稳定标的  |

### 二、核心逻辑：优质股的基本面筛选框架

优质股的核心特征是 “**盈利稳定增长、财务结构健康、估值合理、现金流充沛**”。基于 Baostock 数据，可搭建 “**排除风险→核心指标筛选→行业对比验证**” 的三层选股逻辑，具体指标如下：

#### 1. 第一层：风险排除（先 “排雷”，再选优）

先排除基本面存在明显风险的股票，避免踩雷（如退市风险、财务造假风险、高负债风险），核心排除指标：



*   **上市时间＜1 年**：次新股财务数据不完整，业绩稳定性差；

*   **净利润连续 2 年为负**：避免 ST 股或持续亏损股（退市风险高）；

*   **资产负债率＞80%**：高负债企业抗风险能力弱（金融、地产行业可适当放宽至 85%）；

*   **经营现金流净额连续 2 年为负**：盈利 “纸上谈兵”，现金流无法支撑业绩（需排除，除非是高研发投入的成长股，需单独验证）；

*   **PE-TTM＞100 或为负**：估值过高或业绩亏损，安全边际低。

#### 2. 第二层：核心指标筛选（优质股的 “硬标准”）

通过以下 5 类核心指标，从 “盈利、成长、偿债、运营、估值” 维度聚焦优质标的，指标需结合**近 2-3 年数据趋势**（避免单年偶然性）：



| 评价维度      | 核心指标          | 筛选标准（示例，可根据行业调整）                   | 逻辑说明                                |
| --------- | ------------- | ---------------------------------- | ----------------------------------- |
| **盈利能力**  | ROE（净资产收益率）   | 近 3 年 ROE 均＞15%（且逐年稳定，无大幅下滑）       | ROE 是巴菲特核心指标，连续高 ROE 说明公司赚钱能力强、护城河深 |
|           | 毛利率           | 近 3 年毛利率均＞30%（且波动幅度＜10%）           | 高毛利率代表产品 / 服务有定价权，波动小说明盈利稳定性强       |
| **成长能力**  | 净利润同比增长率      | 近 3 年净利润同比增速均＞10%（且无负增长）           | 排除 “一次性盈利”，聚焦持续增长的公司                |
|           | 营收同比增长率       | 近 3 年营收增速均＞8%（与净利润增速匹配，避免 “增收不增利”） | 营收增长是净利润增长的基础，两者匹配说明增长质量高           |
| **偿债能力**  | 流动比率          | 流动比率＞1.5（短期偿债能力充足，避免资金链风险）         | 流动比率 = 流动资产 / 流动负债，＞1.5 说明短期资产能覆盖负债 |
| **运营能力**  | 资产周转率         | 近 3 年资产周转率稳定（无持续下滑），且高于行业平均水平      | 资产周转率高说明公司资产运营效率高，能更快变现资产创造利润       |
| **估值合理性** | PE-TTM（动态市盈率） | PE-TTM＜50，且低于行业平均 PE 的 80%（避免高估）   | 估值合理是 “安全边际” 的核心，低于行业均值说明相对低估       |
|           | 股息率           | 近 3 年平均股息率＞2%（可选，适合价值型投资者）         | 高股息率说明公司现金流充沛，愿意回馈股东，业绩真实性高         |

#### 3. 第三层：行业对比验证（避免 “跨行业误判”）

不同行业的基本面指标差异极大（如科技股 ROE 通常低于消费股，周期股净利润波动大），需将筛选出的股票与**所属行业平均水平**对比，确保标的在行业内具备竞争力：



*   例如：消费行业（如白酒）的 ROE 普遍＞20%，若某消费股 ROE=15%，虽满足 “＞15%” 的通用标准，但低于行业均值，需排除；

*   操作方式：用 Baostock 的 “行业分类” 数据，按申万一级行业分组，计算每组的指标均值，再筛选出 “指标高于行业均值” 的个股。

### 三、实操步骤：用 Python+Baostock 实现选股

以下是完整的代码流程，包括 “数据获取→数据清洗→指标计算→筛选优质股→结果输出”，以 “筛选 2022-2024 年（近 3 年）符合条件的优质股” 为例（当前时间假设为 2025 年，2024 年年报已披露）。

#### 1. 第一步：导入工具库并登录 Baostock



```
import baostock as bs

import pandas as pd

import numpy as np

\# 1. 登录Baostock（免费，无需注册，直接登录）

lg = bs.login()

print('登录状态：', lg.error\_code, lg.error\_msg)  # 显示0表示登录成功
```

#### 2. 第二步：获取 A 股所有股票代码（排除港股、B 股）

先获取全市场股票列表，筛选出 A 股（代码以 60、00、30、688 开头，分别对应沪 A、深 A、创业板、科创板）：



```
\# 2. 获取指定日期的全市场股票列表（日期选年报披露后，如2025-04-30，确保2024年年报已出）

stock\_rs = bs.query\_all\_stock(day="2025-04-30")

stock\_df = stock\_rs.get\_data()

\# 筛选A股代码（排除B股、港股通标的）

a\_share\_codes = stock\_df\[

&#x20;   stock\_df\['code'].str.startswith(('60', '00', '30', '688'))

]\['code'].tolist()

\# 获取股票名称（后续匹配用）

stock\_name\_dict = dict(zip(stock\_df\['code'], stock\_df\['code\_name']))

print(f"A股总数量：{len(a\_share\_codes)}")  # 约5000+只（2025年数据）
```

#### 3. 第三步：批量获取个股近 3 年财务数据（核心步骤）

通过 Baostock 的财务接口，批量获取每只股票 2022-2024 年的年报数据（净利润、ROE、毛利率等），并整理成 DataFrame：



```
\# 3. 定义函数：获取单只股票的近3年财务数据

def get\_stock\_finance(code, years=\[2022, 2023, 2024]):

&#x20;   finance\_data = \[]

&#x20;   for year in years:

&#x20;       \# 获取利润表数据（净利润、毛利率）

&#x20;       profit\_rs = bs.query\_profit\_data(code=code, year=year, quarter=4)  # 4=年报

&#x20;       profit\_df = profit\_rs.get\_data()

&#x20;       if profit\_df.empty:

&#x20;           continue  # 跳过数据缺失的股票

&#x20;      &#x20;

&#x20;       \# 获取资产负债表数据（ROE、资产负债率、流动比率）

&#x20;       balance\_rs = bs.query\_balance\_sheet(code=code, year=year, quarter=4)

&#x20;       balance\_df = balance\_rs.get\_data()

&#x20;       if balance\_df.empty:

&#x20;           continue

&#x20;      &#x20;

&#x20;       \# 获取现金流量表数据（经营现金流净额）

&#x20;       cashflow\_rs = bs.query\_cash\_flow(code=code, year=year, quarter=4)

&#x20;       cashflow\_df = cashflow\_rs.get\_data()

&#x20;       if cashflow\_df.empty:

&#x20;           continue

&#x20;      &#x20;

&#x20;       \# 提取关键指标（注意：Baostock的字段名需精确匹配，可通过print(profit\_df.columns)查看）

&#x20;       row = {

&#x20;           '股票代码': code,

&#x20;           '股票名称': stock\_name\_dict.get(code, ''),

&#x20;           '年份': year,

&#x20;           '净利润(万元)': float(profit\_df\['net\_profit'].iloc\[0]) if profit\_df\['net\_profit'].iloc\[0] != '' else np.nan,

&#x20;           'ROE(%)': float(balance\_df\['roe'].iloc\[0]) if balance\_df\['roe'].iloc\[0] != '' else np.nan,

&#x20;           '毛利率(%)': float(profit\_df\['gross\_profit\_margin'].iloc\[0]) if profit\_df\['gross\_profit\_margin'].iloc\[0] != '' else np.nan,

&#x20;           '资产负债率(%)': float(balance\_df\['debt\_to\_asset\_ratio'].iloc\[0]) if balance\_df\['debt\_to\_asset\_ratio'].iloc\[0] != '' else np.nan,

&#x20;           '流动比率': float(balance\_df\['current\_ratio'].iloc\[0]) if balance\_df\['current\_ratio'].iloc\[0] != '' else np.nan,

&#x20;           '经营现金流净额(万元)': float(cashflow\_df\['ocf'].iloc\[0]) if cashflow\_df\['ocf'].iloc\[0] != '' else np.nan

&#x20;       }

&#x20;       finance\_data.append(row)

&#x20;  &#x20;

&#x20;   return pd.DataFrame(finance\_data)

\# 4. 批量获取所有A股的财务数据（耗时较长，约10-20分钟，可加进度条）

all\_finance\_df = pd.DataFrame()

for i, code in enumerate(a\_share\_codes):

&#x20;   if i % 100 == 0:

&#x20;       print(f"已处理{i}/{len(a\_share\_codes)}只股票")

&#x20;   stock\_df = get\_stock\_finance(code)

&#x20;   all\_finance\_df = pd.concat(\[all\_finance\_df, stock\_df], ignore\_index=True)

\# 保存原始数据（避免重复请求）

all\_finance\_df.to\_csv('/mnt/a\_share\_finance\_2022\_2024.csv', index=False, encoding='utf-8-sig')

print("财务数据获取完成，共{}条记录".format(len(all\_finance\_df)))
```

#### 4. 第四步：计算成长指标（净利润 / 营收增速）

基于近 3 年的净利润数据，计算同比增速（需注意：2022 年的增速需 2021 年数据，此处假设 2021 年数据已获取，或简化用 2023/2022、2024/2023 的增速）：



```
\# 5. 计算近2年净利润同比增速（2023年增速=2023净利润/2022净利润-1，2024年同理）

growth\_df = all\_finance\_df.pivot(index='股票代码', columns='年份', values='净利润(万元)').reset\_index()

growth\_df.columns = \['股票代码', '净利润\_2022', '净利润\_2023', '净利润\_2024']

\# 计算增速（避免除以0，用np.where处理）

growth\_df\['2023净利润增速(%)'] = np.where(

&#x20;   growth\_df\['净利润\_2022'] != 0,

&#x20;   (growth\_df\['净利润\_2023'] / growth\_df\['净利润\_2022'] - 1) \* 100,

&#x20;   np.nan

)

growth\_df\['2024净利润增速(%)'] = np.where(

&#x20;   growth\_df\['净利润\_2023'] != 0,

&#x20;   (growth\_df\['净利润\_2024'] / growth\_df\['净利润\_2023'] - 1) \* 100,

&#x20;   np.nan

)

\# 合并股票名称

growth\_df\['股票名称'] = growth\_df\['股票代码'].map(stock\_name\_dict)
```

#### 5. 第五步：获取估值数据（PE-TTM、股息率）

结合 Baostock 的日度估值数据，获取 2025-04-30 的 PE-TTM 和股息率（估值需与财务数据时间匹配，避免跨期偏差）：



```
\# 6. 获取估值数据（PE-TTM、股息率）

valuation\_rs = bs.query\_history\_k\_data\_plus(

&#x20;   code=','.join(a\_share\_codes),  # 批量传入股票代码

&#x20;   fields='code,pe\_ttm,dividend\_yield',  # 所需字段

&#x20;   start\_date='2025-04-30',

&#x20;   end\_date='2025-04-30',

&#x20;   frequency='d',

&#x20;   adjustflag='3'  # 复权类型：3=后复权

)

valuation\_df = valuation\_rs.get\_data()

\# 数据类型转换（Baostock返回的是字符串，需转成数值）

valuation\_df\['pe\_ttm'] = pd.to\_numeric(valuation\_df\['pe\_ttm'], errors='coerce')

valuation\_df\['dividend\_yield'] = pd.to\_numeric(valuation\_df\['dividend\_yield'], errors='coerce')

valuation\_df.rename(columns={'code': '股票代码', 'dividend\_yield': '股息率(%)'}, inplace=True)

valuation\_df\['股票名称'] = valuation\_df\['股票代码'].map(stock\_name\_dict)
```

#### 6. 第六步：多条件筛选优质股

将财务数据、成长数据、估值数据合并，应用前文的 “风险排除 + 核心指标筛选” 逻辑，最终得到优质股列表：



```
\# 7. 合并所有数据（财务+成长+估值）

\# 先获取2024年的核心财务指标（最新年报数据）

latest\_finance\_df = all\_finance\_df\[all\_finance\_df\['年份'] == 2024]\[

&#x20;   \['股票代码', '股票名称', 'ROE(%)', '毛利率(%)', '资产负债率(%)', '流动比率', '经营现金流净额(万元)']

]

\# 合并成长数据和估值数据

final\_df = latest\_finance\_df.merge(

&#x20;   growth\_df\[\['股票代码', '2023净利润增速(%)', '2024净利润增速(%)']],

&#x20;   on='股票代码',

&#x20;   how='inner'

).merge(

&#x20;   valuation\_df\[\['股票代码', 'pe\_ttm', '股息率(%)']],

&#x20;   on='股票代码',

&#x20;   how='inner'

)

\# 8. 应用筛选条件（优质股标准）

high\_quality\_df = final\_df\[

&#x20;   \# 1. 风险排除

&#x20;   (final\_df\['资产负债率(%)'] < 80) &  # 低负债

&#x20;   (final\_df\['经营现金流净额(万元)'] > 0) &  # 正经营现金流

&#x20;   (final\_df\['pe\_ttm'] > 0) & (final\_df\['pe\_ttm'] < 50) &  # 估值合理（PE>0避免亏损股）

&#x20;   \# 2. 盈利能力

&#x20;   (final\_df\['ROE(%)'] > 15) &  # 高ROE

&#x20;   (final\_df\['毛利率(%)'] > 30) &  # 高毛利率

&#x20;   \# 3. 成长能力

&#x20;   (final\_df\['2023净利润增速(%)'] > 10) &  # 连续2年增长

&#x20;   (final\_df\['2024净利润增速(%)'] > 10) &

&#x20;   \# 4. 偿债能力

&#x20;   (final\_df\['流动比率'] > 1.5) &  # 短期偿债安全

&#x20;   \# 5. 可选：高股息（价值型）

&#x20;   (final\_df\['股息率(%)'] > 2)

].copy()

\# 按ROE降序排序（ROE优先）

high\_quality\_df = high\_quality\_df.sort\_values('ROE(%)', ascending=False).reset\_index(drop=True)

\# 输出结果

print(f"筛选出的优质股数量：{len(high\_quality\_df)}")

high\_quality\_df.to\_csv('/mnt/high\_quality\_stocks\_2025.csv', index=False, encoding='utf-8-sig')

print("优质股列表（前10只）：")

print(high\_quality\_df\[\['股票代码', '股票名称', 'ROE(%)', 'pe\_ttm', '股息率(%)']].head(10))
```

#### 7. 第七步：登出 Baostock



```
\# 9. 登出

bs.logout()
```

### 四、关键优化：提升选股准确性的技巧



1.  **行业差异化调整指标**：

*   科技股（如半导体）：可放宽 ROE 至 10%，但需要求 “研发投入占比＞15%”（Baostock 的利润表字段`rd_expense`可获取研发投入）；

*   周期股（如钢铁、煤炭）：需结合 “行业景气度”（如 PPI 数据，需额外获取），避免在周期顶部买入，可增加 “市净率 PB＜2” 的条件；

*   金融股（如银行）：资产负债率可放宽至 90%，核心看 “不良贷款率”（Baostock 的银行专项数据可获取）。

1.  **避免 “数据陷阱”**：

*   排除 “一次性收益”：若某公司净利润大增但 “扣非净利润 / 净利润＜0.8”（扣非净利润字段`net_profit_after_nrgal`），说明盈利依赖非经常性收益（如政府补贴），需排除；

*   验证 “现金流与利润匹配”：要求 “经营现金流净额 / 净利润＞1”，确保盈利有现金流支撑。

1.  **结合技术面辅助**：

*   用 Baostock 的 K 线数据（如`query_history_k_data_plus`获取均线），筛选 “股价在 20 日均线上方” 的标的，避免买入处于下跌趋势的股票。

### 五、局限性与替代方案

Baostock 的免费数据存在以下局限，需注意：



*   **数据延迟**：年报数据通常在收盘后 1-3 小时更新，实时行情数据缺失（无法做日内交易）；

*   **数据完整性**：部分小众股票（如 ST 股）的财务数据可能缺失，需手动补充；

*   **缺乏深度数据**：如 “机构持仓比例”“股东人数变化” 等数据无法获取。

若需更专业的分析，可结合：



*   付费数据源：Wind、同花顺 iFinD（提供预计算的 “基本面评分”）；

*   其他免费数据源：Tushare（需积分，可获取机构持仓数据）、巨潮资讯网（官网下载年报 PDF，手动提取细节）。

### 六、总结

通过 Baostock 分析基本面选股的核心是 “**用免费数据搭建标准化框架，先排雷再选优**”。实操中需注意：



1.  数据时间需匹配（如 2024 年年报 + 2025 年 4 月估值）；

2.  指标需结合行业特性调整；

3.  最终结果需人工验证（如查看公司公告，排除业绩变脸风险）。

按照上述流程，可筛选出 “盈利强、增长稳、估值合理” 的优质股，为投资决策提供数据支撑。

> （注：文档部分内容可能由 AI 生成）