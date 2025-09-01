# 项目介绍
尝试通过TRAE（Technical Analysis and Research Engine）技术，对A股股票的基本面数据进行分析和筛选，选出符合条件的股票（即优质股票）。

## 1、基本面选股
本程序的目标是根据股票的基本面数据，进行分析和筛选，选出符合条件的股票（即优质股票）。
本程序所需各类数据应包括多种可信数据源，保证运行时仅可能获取到必要数据，供程序分析筛选。

本程序主要功能包括：

1. 获取A股的全部股票列表

2. 获取A股股票的基本面数据

3. 分析和筛选股票

如下为三个主要功能的描述，作为Trae生成程序的提示词使用。

### 1.1、获取A股的全部股票列表
获取A股的全部真实股票列表（包含股票名称、股票代码、所属行业、市场类型），并将获取的A股股票列表保存到本地（stock_screeners/cache/stockA_list.csv），获取真实数据。更新日志名为stock_screeners/cache/list_update_log.json。程序名为stock_screeners/get_stockA_list.py；

### 1.2、优质股选择<font color=red>Piotroski F-Score</font>
注：建议结合华泰 FFScore（改进版 F-Score），其更适配 A 股市场，例如增加低市净率（PB）筛选和流动性指标。

***<font color=red>f_score includes:</font>***

- "roa_positive"                    '资产回报率or资产收益率为正', 
- "operating_cash_flow_positive"    '经营现金流为正', 
- "roa_growth"                      '资产收益率增长', 
- "cash_flow_gt_net_profit"         '经营现金流大于净利润',
- "leverage_improved"               '杠杆改进'or'杠杆率降低',
- "current_ratio_improved"          '流动比率提高',
- "no_new_equity"                   '无新 equity'or'未发行新股',
- "gross_margin_improved"           '毛利率提高',
- "asset_turnover_improved"         '资产周转率提高'

ROA: 平均资产回报率（Return on Average Assets，ROA）
no_new_equity：F-score 模型是一种用于评估公司财务健康和盈利能力的工具，没有增发新股这一指标能反映公司的资金来源和财务策略。若公司不通过发行新股来融资，可能意味着它能依靠自身盈利或债务融资来满足资金需求，侧面体现公司财务状况较好、经营稳定。


#### 1.2.1、baostock数据源
创建或完善一个程序stock_screeners/get_stock_fscore_baostock.py. 它从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，通过baostock数据源获取列表中各股票的相关数据计算它的Piotroski F-Score值，并把结果（股票名称、股票代码、股票所属行业、F-Score值及其9项财务指标）按照F-Score值进行排序，并保存到stock_screeners/cache/stockA_fscore_baostock.csv。
baostock一次login可以获得多只股票的数据，以减少login次数。
将生成的log保存到stock_screeners/logs/目录中保存。
增量保存，即每获取10条数据 就保存一次到stockA_fscore_baostock.csv。增加断点续传功能，即程序中断重启后将从中断处继续获取数据并保存到以上csv文件中。
不要参考stock_screeners/目录中已有的代码。

#### 1.2.2、akshare数据源
创建一个程序stock_screeners/get_stock_fscore_akshare.py. 它从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，通过akshare数据源获取列表中各股票的相关数据计算它的Piotroski F-Score值，并把结果（股票名称、股票代码、股票所属行业、F-Score值及其9项财务指标）按照F-Score值进行排序，并保存到stock_screeners/cache/stockA_fscore_akshare.csv。
将生成的log保存到stock_screeners/logs/目录中保存。
为避免频繁访问数据接口 导致访问被屏蔽或数据获取失败，增加必要的预防措施.
不要参考stock_screeners/目录中已有的代码。

#### 1.2.3、eastmoney数据源
创建一个程序stock_screeners/get_stock_fscore_eastmoney.py. 它从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，通过akshare数据源获取列表中各股票的相关数据计算它的Piotroski F-Score值，并把结果（股票名称、股票代码、股票所属行业、F-Score值及其9项财务指标）按照F-Score值进行排序，并保存到stock_screeners/cache/stockA_fscore_eastmoney.csv。
将生成的log保存到stock_screeners/logs/目录中保存。
eastmoney接口访问添加反爬机制：包括访问频率控制、随机User-Agent伪装、请求头伪装、重试机制和随机请求延迟等功能。
不要参考stock_screeners/目录中已有的代码。

### 1.3、获取A股股票的基本面数据
从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，并依据A股股票列表中的股票代码，从多个真实数据源获取A股股票的真实基本面数据。基本面数据应包括:股票名称、股票代码、股票上市日期、股票上市地点、股票所属行业、每股收益、市盈率（静）、市盈率（TTM）、毛利率、净利率、资产收益率、资产负债率、净利润增速等。本地可以保存所有A股股票的基本面数据（stock_screeners/cache/stockA_fundamentals.csv）。
基本面数据必须来自真实数据。基本面数据可以分批次获取并存入stockA_fundamentals.csv，而不必全部获取后一起存入。如果获取中断，下次运行程序可以从中断处继续获取并保存。
获取真实数据，并确保获取到完整的A股股票列表，如果某些字段获取不到，可填空。
更新日志名为stock_screeners/cache/fundamentals_update_log.json。程序名为stock_screeners/get_stockA_fundamentals.py


#### 1.3.1 eastmoney数据源
从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，并依据A股股票列表中的股票代码，从eastmoney数据源获取A股股票的真实基本面数据。基本面数据字段可依据数据源所提供的接口适当调整。本地可以保存所有A股股票的基本面数据（stock_screeners/cache/stockA_fundamentals_eastmoney.csv）。
基本面数据必须来自真实数据。基本面数据可以分批次获取并存入stockA_fundamentals_eastmoney.csv，而不必全部获取后一起存入。如果获取中断，下次运行程序可以从中断处继续获取并保存。
获取真实数据，并确保获取到完整的A股股票列表，如果某些字段获取不到，可删除。
更新日志名为stock_screeners/cache/fundamentals_eastmoney_update_log.json。程序名为stock_screeners/get_stockA_fundamentals_eastmoney.py
为避免频繁访问数据接口 导致访问被屏蔽或数据获取失败，增加必要的预防措施：比如eastmoney接口访问添加反爬机制：包括访问频率控制、随机User-Agent伪装、请求头伪装、重试机制和随机请求延迟等功能。

#### 1.3.2 baostock数据源
从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，并依据A股股票列表中的股票代码，从baostock数据源获取A股股票的真实基本面数据。基本面数据字段可依据数据源所提供的接口适当调整。本地可以保存所有A股股票的基本面数据（stock_screeners/cache/stockA_fundamentals_baostock.csv）。
基本面数据必须来自真实数据。基本面数据可以分批次获取并存入stockA_fundamentals_baostock.csv，而不必全部获取后一起存入。如果获取中断，下次运行程序可以从中断处继续获取并保存。
获取真实数据，并确保获取到完整的A股股票列表，如果某些字段获取不到，可删除。
更新日志名为stock_screeners/cache/fundamentals_baostock_update_log.json。程序名为stock_screeners/get_stockA_fundamentals_baostock.py
为避免频繁访问数据接口 导致访问被屏蔽或数据获取失败，增加必要的预防措施：比如baostock函数添加访问控制策略，包括访问频率限制、随机延迟，或其他方式。

#### 1.3.3 akshare数据源
从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，并依据A股股票列表中的股票代码，从akshare数据源获取A股股票的真实基本面数据。基本面数据字段可依据数据源所提供的接口适当调整。本地可以保存所有A股股票的基本面数据（stock_screeners/cache/stockA_fundamentals_akshare.csv）。
基本面数据必须来自真实数据。基本面数据可以分批次获取并存入stockA_fundamentals_akshare.csv，而不必全部获取后一起存入。如果获取中断，下次运行程序可以从中断处继续获取并保存。
获取真实数据，并确保获取到完整的A股股票列表，如果某些字段获取不到，可删除。
更新日志名为stock_screeners/cache/fundamentals_akshare_update_log.json。程序名为stock_screeners/get_stockA_fundamentals_akshare.py
避免频繁访问数据接口 导致访问被屏蔽或数据获取失败，增加必要的预防措施：比如akshare函数添加访问控制策略，包括访问频率限制、随机延迟，或其他方式。


#### 预防措施
为避免频繁访问数据接口 导致访问被屏蔽或数据获取失败，增加必要的预防措施：
- eastmoney接口访问添加反爬机制：包括访问频率控制、随机User-Agent伪装、请求头伪装、重试机制和随机请求延迟等功能。
- baostock函数添加访问控制策略，包括访问频率限制、随机延迟和使用logger记录日志。
- akshare函数添加访问控制策略，包括访问频率限制、随机延迟和使用logger记录日志。

### 1.4、分析和筛选股票
#### 1.4.1、基于eastmoney数据源
根据从eastmoney获取的基本面数据（保存在stock_screeners/cache/stockA_fundamentals_eastmoney.csv）进行策略分析和筛选，以获得优质股票（如发现现价价值被低估的股票）。
根据分析结果，筛选出符合条件的股票，得到20-50支优质股票。
将分析筛选策略和筛选结果存到本地存入stock_screeners/result目录下的result_selected_eastmoney.csv和result_selected_eastmoney.md文件中。并将选中的股票代码和股票名称存入stock_screeners/result/result_selected_eastmoney.json中。程序名为stock_screeners/stock_screeners_eastmoney.py

#### 1.4.2、基于baostock数据源
**策略一：**
根据从baostock获取的基本面数据（保存在stock_screeners/cache/stockA_fundamentals_baostock.csv，其中的值字段为空时，请忽略）。进行策略分析和筛选，以获得优质股票（如发现现价价值被低估的股票）。
根据分析结果，筛选出符合条件的股票，得到20-50支优质股票。
将分析筛选策略和筛选结果存到本地存入stock_screeners/result目录下的result_selected_baostock.csv和result_selected_baostock.md文件中。并将选中的股票代码和股票名称存入stock_screeners/result/result_selected_baostock.json中。程序名为stock_screeners/stock_screeners_baostock.py
不要参考stock_screeners/目录中已有的代码。

**策略二：**
请参考”docs/用 Baostock 通过基本面分析筛选优质股.md“中的说明，并对优质股选择策略进行适当优化，并生成一个选择优质股的程序stock_screeners/stock_screeners_baostock.py文件。
股票选择范围包括A股的所有股票。
baostock 平台介绍：
- http://baostock.com/baostock/index.php/%E9%A6%96%E9%A1%B5
- http://baostock.com/baostock/index.php/%E5%85%AC%E5%BC%8F%E4%B8%8E%E6%95%B0%E6%8D%AE%E6%A0%BC%E5%BC%8F%E8%AF%B4%E6%98%8E
baostock Python API文档可以参考：http://baostock.com/baostock/index.php/Python_API%E6%96%87%E6%A1%A3
根据分析结果，筛选出符合条件的股票，得到20-50支优质股票。
即使获取不到数据源的数据，也不要使用模拟数据。
将分析筛选策略和筛选结果存到本地存入stock_screeners/result目录下的result_selected_baostock.csv和result_selected_baostock.md文件中。并将选中的股票代码和股票名称存入stock_screeners/result/result_selected_baostock.json中。
不要参考stock_screeners/目录中已有的代码。
不必参考stock_screeners/cache/stockA_fundamentals_baostock.csv数据。


### 1.4.3、基于akshare数据源
根据从akshare获取的基本面数据（保存在stock_screeners/cache/stockA_fundamentals_akshare.csv）进行策略分析和筛选，以获得优质股票（如发现现价价值被低估的股票）。
根据分析结果，筛选出符合条件的股票，得到20-50支优质股票。
将分析筛选策略和筛选结果存到本地存入stock_screeners/result目录下的result_selected_akshare.csv和result_selected_akshare.md文件中。并将选中的股票代码和股票名称存入stock_screeners/result/result_selected_akshare.json中。程序名为stock_screeners/stock_screeners_akshare.py


### 1.5、生成代码说明
依据stock_screeners目录中的程序，将主要逻辑、注意事项和使用说明等内容写一份说明文档文档名为（stock_screeners/README_screeners.md）。
程序列表如下：
- get_stockA_list.py
- get_stockA_fundamentals.py
- stock_screeners.py
- ...

## 参考
- [Python破解东方财富反爬机制](https://cloud.tencent.com/developer/article/2542891)


## Trae沟通技巧
问题：自动化生成的基本面数据获取程序经常获取不到数据？
问Trae 还有哪些开发接口 可以免费获得A股股票的基本面数据？