# 使用akshare计算A股股票的Piotroski F-Score

## 项目简介

本项目提供了一个使用akshare数据源计算A股股票Piotroski F-Score的工具。Piotroski F-Score是一种用于评估公司财务健康状况的指标体系，由会计教授Joseph Piotroski提出。

该工具从CSV文件获取A股股票列表，通过akshare数据源获取各股票的财务数据，计算Piotroski F-Score值，并将结果按F-Score排序后保存到CSV文件中。

## 文件说明

- `get_stock_fscore_akshare.py`: 主程序，用于计算A股股票的Piotroski F-Score
- `test_fscore_akshare.py`: 测试脚本，用于验证主程序功能（仅处理少量股票）
- `logs/`: 存放日志文件的目录
- `cache/`: 存放股票列表、计算结果和进度文件的目录

## 功能特点

1. **数据获取**: 使用akshare数据源获取A股股票的财务数据
2. **F-Score计算**: 根据Piotroski的9项指标计算F-Score
3. **结果排序**: 按F-Score值对结果进行排序
4. **进度保存**: 支持断点续算，保存处理进度
5. **防屏蔽措施**: 添加随机延迟、批量处理等措施，避免频繁访问数据接口导致被屏蔽
6. **日志记录**: 详细记录程序运行过程和结果

## 安装依赖

在运行程序之前，需要安装以下依赖：

```bash
pip install pandas numpy akshare requests
```

## 使用方法

### 准备股票列表文件

在`stock_screeners/cache/`目录下创建`stockA_list.csv`文件，包含A股股票代码列表。CSV文件格式如下：

```csv
股票代码
600000
600036
600519
...
```

### 运行主程序

```bash
cd /Users/dora/Documents/JavaDemos/HelloStock/stock_screeners
python get_stock_fscore_akshare.py
```

### 测试模式

如果需要快速测试程序功能，可以使用测试模式（只处理前100只股票）：

```bash
python get_stock_fscore_akshare.py --test
```

### 使用测试脚本

也可以使用提供的测试脚本来验证程序功能：

```bash
python test_fscore_akshare.py
```

## 输出结果

程序运行完成后，将在`stock_screeners/cache/`目录下生成以下文件：

- `stockA_fscore_akshare.csv`: 包含所有股票的F-Score计算结果，按F-Score降序排序
- `stockA_fscore_akshare_top10.json`: 前10名高分股票的详细信息（JSON格式）
- `fscore_akshare_progress.json`: 处理进度文件，用于断点续算

日志文件将保存在`stock_screeners/logs/`目录下，文件名格式为`stock_fscore_akshare_YYYY-MM-DD.log`。

## Piotroski F-Score指标说明

Piotroski F-Score基于以下9项财务指标，每项指标符合条件得1分，不符合得0分，总分为9分：

1. **ROA为正**: 净利润除以总资产为正
2. **经营现金流为正**: 经营活动产生的现金流量净额为正
3. **ROA增长**: 当期ROA大于上期ROA
4. **现金流大于净利润**: 经营活动产生的现金流量净额大于净利润
5. **杠杆率改善**: 当期资产负债率小于上期资产负债率
6. **流动比率提高**: 当期流动比率大于上期流动比率
7. **没有发行新股**: 股权没有显著增加（小于10%）
8. **毛利率提高**: 当期毛利率大于上期毛利率
9. **资产周转率提高**: 当期资产周转率大于上期资产周转率

一般来说，F-Score在0-3分之间表示财务状况较弱，4-6分表示财务状况中等，7-9分表示财务状况较强。

## 注意事项

1. 请确保网络连接正常，程序需要从akshare数据源获取数据
2. 由于需要获取大量财务数据，程序运行时间可能较长
3. 为避免触发反爬机制，程序已添加随机延迟和批量处理功能，请不要修改相关参数
4. 如果程序意外中断，可以重新运行，程序会从上次处理的位置继续
5. 运行过程中的详细信息可以在日志文件中查看

## 常见问题

### Q: 运行程序时提示"akshare库未安装"怎么办？
A: 请先安装akshare库：`pip install akshare`

### Q: 程序运行过程中被卡住或报错怎么办？
A: 程序包含重试机制，如果遇到临时网络问题会自动重试。如果持续报错，可以查看日志文件获取详细信息。

### Q: 如何调整处理速度？
A: 为避免触发反爬机制，不建议调整程序中的延迟参数。如果需要更快的处理速度，可以考虑分批处理股票列表。