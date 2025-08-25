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

### 1.2、获取A股股票的基本面数据
从文件（stock_screeners/cache/stockA_list.csv）中获取A股的股票列表，并依据A股股票列表中的股票代码，从多个真实数据源获取A股股票的真实基本面数据。基本面数据应包括:股票名称、股票代码、股票上市日期、股票上市地点、股票所属行业、每股收益、市盈率（静）、市盈率（TTM）、毛利率、净利率、资产收益率、资产负债率、净利润增速等。本地可以保存所有A股股票的基本面数据（stock_screeners/cache/stockA_fundamentals.csv）。
基本面数据必须来自真实数据。基本面数据可以分批次获取并存入stockA_fundamentals.csv，而不必全部获取后一起存入。如果获取中断，下次运行程序可以从中断处继续获取并保存。
获取真实数据，并确保获取到完整的A股股票列表，如果某些字段获取不到，可填空。
更新日志名为stock_screeners/cache/fundamentals_update_log.json。程序名为stock_screeners/get_stockA_fundamentals.py

### 1.3、分析和筛选股票
根据基本面数据（stock_screeners/cache/stockA_fundamentals.csv），进行策略分析和筛选，以获得优质股票（如发现现价价值被低估的股票）。
根据分析结果，筛选出符合条件的股票，得到20-50支优质股票。
将分析筛选策略和筛选结果存到本地存入stock_screeners/result目录下的result_selected.csv和result_selected.md文件中。并将选中的股票代码和股票名称存入stock_screeners/result/result_selected.json中。程序名为stock_screeners/stock_screeners.py

### 1.4、生成代码说明
依据stock_screeners目录中的程序，将主要逻辑、注意实现和使用说明等内容写一份说明文档文档名为（stock_screeners/screeners_guide.md）。
程序列表如下：

- get_stockA_list.py
- get_stockA_fundamentals.py
- stock_screeners.py


## FAQ
问题：自动化生成的基本面数据获取程序经常获取不到数据？
问Trae 还有哪些开发接口 可以免费获得A股股票的基本面数据？