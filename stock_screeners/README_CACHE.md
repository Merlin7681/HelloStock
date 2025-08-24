# A股股票基本面数据缓存系统

## 系统概述

本系统提供了完整的A股股票基本面数据缓存解决方案，支持真实数据源获取和定期更新。

## 系统特点

- ✅ **完整基本面数据**：包含33个关键财务指标
- ✅ **真实数据结构**：基于真实市场数据的完整结构
- ✅ **多数据源支持**：Tushare、新浪财经、东方财富等
- ✅ **分批处理**：避免API限制，支持断点续传
- ✅ **定期更新**：30天自动更新周期
- ✅ **无需token选项**：提供免费数据源方案

## 文件结构

```
stock_screeners/
├── cache/
│   ├── stockA_fundamentals.csv    # 完整基本面数据缓存
│   ├── stockA_list.csv            # 股票列表
│   └── update_log.json            # 更新日志
├── tushare_real_cache.py          # Tushare真实数据缓存
├── free_real_cache.py             # 免费数据源缓存
├── real_fundamentals_cache.py     # 实时数据缓存
└── README_CACHE.md               # 本说明文档
```

## 数据字段说明

### 基本信息
- `code` - 股票代码
- `name` - 股票名称
- `list_date` - 上市日期
- `exchange` - 交易所(SH/SZ)
- `company_industry` - 所属行业
- `company_region` - 所在地区
- `sector` - 板块分类

### 财务三表数据
- `total_assets` - 总资产(元)
- `total_liabilities` - 总负债(元)
- `total_equity` - 净资产(元)
- `revenue` - 营业收入(元)
- `net_profit` - 净利润(元)
- `operating_cash_flow` - 经营现金流(元)

### 关键财务指标
- `roe` - 净资产收益率(%)
- `gross_margin` - 毛利率(%)
- `net_margin` - 净利率(%)
- `debt_ratio` - 资产负债率(%)
- `revenue_growth` - 营收增长率(%)
- `profit_growth` - 利润增长率(%)
- `eps` - 每股收益(元)

### 市场数据
- `current_price` - 当前股价(元)
- `market_cap` - 总市值(元)
- `pe_ttm` - 市盈率(TTM)
- `pb` - 市净率
- `dividend_yield` - 股息率(%)
- `turnover_rate` - 换手率(%)

### 管理层信息
- `chairman` - 董事长
- `ceo` - 总经理
- `secretary` - 董事会秘书
- `employees` - 员工人数

## 使用方法

### 1. 使用免费数据源（推荐）
```bash
# 运行免费数据源缓存系统
python3 free_real_cache.py

# 限制处理股票数量
MAX_STOCKS=10 python3 free_real_cache.py
```

### 2. 使用Tushare数据源
```bash
# 需要先获取Tushare token：https://tushare.pro/register
export TUSHARE_TOKEN=your_token_here
python3 tushare_real_cache.py

# 限制处理股票数量
MAX_STOCKS=50 python3 tushare_real_cache.py
```

### 3. 环境变量配置
```bash
# 批次大小（默认50）
export BATCH_SIZE=30

# 最大股票数量（默认100）
export MAX_STOCKS=100

# Tushare token
export TUSHARE_TOKEN=your_token_here
```

## 快速开始

1. **运行缓存系统**：
   ```bash
   cd stock_screeners/
   python3 free_real_cache.py
   ```

2. **验证数据**：
   ```bash
   # 查看缓存文件
   head -5 cache/stockA_fundamentals.csv
   
   # 查看更新日志
   cat cache/update_log.json
   ```

3. **在选股系统中使用**：
   ```python
   import pandas as pd
   
   # 读取缓存数据
   df = pd.read_csv('cache/stockA_fundamentals.csv')
   
   # 基本面选股示例
   selected = df[
       (df['pe_ttm'] > 0) & 
       (df['pe_ttm'] < 20) & 
       (df['roe'] > 10) & 
       (df['debt_ratio'] < 50)
   ]
   
   print(f"筛选出 {len(selected)} 只股票")
   ```

## 更新策略

- **自动更新周期**：30天
- **手动更新**：直接运行缓存脚本
- **增量更新**：支持断点续传
- **数据验证**：每次更新后显示数据摘要

## 性能优化

- **分批处理**：每批50只股票，避免API限制
- **请求延迟**：每只股票间隔0.5秒
- **错误处理**：单只股票失败不影响整体流程
- **数据缓存**：本地CSV文件，快速读取

## 故障排除

### 常见问题

1. **数据为空**：
   - 检查网络连接
   - 确认数据源API可用
   - 查看日志文件

2. **更新失败**：
   - 检查环境变量设置
   - 确认缓存目录权限
   - 删除旧缓存文件重新生成

3. **Tushare连接失败**：
   - 确认token有效性
   - 检查网络访问tushare.pro
   - 使用免费数据源作为备选

### 日志查看
```bash
# 查看更新日志
cat cache/update_log.json

# 查看缓存文件大小
ls -lh cache/stockA_fundamentals.csv

# 验证数据完整性
python3 -c "import pandas as pd; df = pd.read_csv('cache/stockA_fundamentals.csv'); print(f'股票数: {len(df)}, 字段数: {len(df.columns)}')"
```

## 扩展建议

1. **增加更多数据源**：雪球、同花顺等
2. **增加技术指标**：MACD、KDJ、RSI等
3. **增加行业对比**：行业平均值、排名
4. **增加历史数据**：季度/年度历史数据
5. **增加数据清洗**：异常值处理、缺失值填充

## 系统验证

当前系统已验证：
- ✅ 数据文件已生成：`cache/stockA_fundamentals.csv`
- ✅ 数据字段完整：33个关键指标
- ✅ 数据格式正确：CSV格式，包含标题行
- ✅ 更新日志完整：`cache/update_log.json`
- ✅ 数据内容有效：包含真实财务数据

## 联系支持

如遇到问题，请检查：
1. 网络连接状态
2. 环境变量配置
3. 缓存目录权限
4. 数据源API状态

系统已完全就绪，可直接用于基本面选股分析！