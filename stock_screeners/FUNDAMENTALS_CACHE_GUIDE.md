# 📊 A股股票基本面数据缓存系统使用指南

## 🎯 系统概述

本系统实现了**完整的A股股票基本面数据缓存功能**，支持：
- ✅ **缓存所有A股股票**的完整基本面数据
- ✅ **定期自动更新**（默认30天）
- ✅ **包含README.md要求的所有数据字段**
- ✅ **支持分批处理和断点续传**
- ✅ **支持数量限制和灵活配置**

## 📋 数据字段说明

### 🏢 公司基本信息
| 字段 | 说明 | 示例 |
|------|------|------|
| `code` | 股票代码 | 000001 |
| `name` | 股票名称 | 平安银行 |
| `list_date` | 上市日期 | 1991-04-03 |
| `exchange` | 交易所 | 深圳证券交易所 |
| `industry` | 所属行业 | 银行业 |
| `sector` | 所属地区 | 广东省 |
| `company_name` | 公司名称 | 平安银行股份有限公司 |
| `company_industry` | 公司行业 | 货币金融服务 |
| `company_region` | 公司地区 | 广东省深圳市 |
| `ceo` | 总经理 | 胡跃飞 |
| `chairman` | 董事长 | 谢永林 |
| `secretary` | 董秘 | 周强 |
| `employees` | 员工人数 | 36000 |

### 📊 财务三表数据
#### 资产负债表
- `total_assets`: 总资产(万元)
- `total_liabilities`: 总负债(万元)
- `total_equity`: 股东权益(万元)
- `current_assets`: 流动资产(万元)
- `current_liabilities`: 流动负债(万元)

#### 利润表
- `revenue`: 营业收入(万元)
- `operating_profit`: 营业利润(万元)
- `net_profit`: 净利润(万元)
- `gross_profit`: 毛利润(万元)

#### 现金流量表
- `operating_cash_flow`: 经营现金流(万元)
- `investing_cash_flow`: 投资现金流(万元)
- `financing_cash_flow`: 筹资现金流(万元)
- `free_cash_flow`: 自由现金流(万元)

### 📈 关键财务指标
| 指标 | 字段 | 单位 |
|------|------|------|
| 每股收益 | `eps` | 元 |
| 市盈率(静) | `pe_static` | 倍 |
| 市盈率(TTM) | `pe_ttm` | 倍 |
| 市净率 | `pb` | 倍 |
| 净资产收益率 | `roe` | % |
| 总资产收益率 | `roa` | % |
| 毛利率 | `gross_margin` | % |
| 净利率 | `net_margin` | % |
| 资产负债率 | `debt_ratio` | % |
| 流动比率 | `current_ratio` | 倍 |
| 速动比率 | `quick_ratio` | 倍 |
| 营收增长率 | `revenue_growth` | % |
| 净利润增长率 | `profit_growth` | % |
| 总资产增长率 | `assets_growth` | % |

### 💰 市场数据
- `current_price`: 当前价格
- `market_cap`: 总市值(亿元)
- `dividend_yield`: 股息率(%)
- `dividend_per_share`: 每股股利(元)

## 🚀 使用方法

### 1️⃣ 快速开始
```bash
cd /Users/dora/Documents/JavaDemos/HelloStock/stock_screeners
python3 stock_fundamentals_cache.py
```

### 2️⃣ 配置环境变量
```bash
# 设置更新周期（天）
export UPDATE_INTERVAL_DAYS=30

# 设置分批处理大小
export BATCH_SIZE=100

# 设置最大处理数量（0表示处理所有）
export MAX_STOCKS=1000

# 运行程序
python3 stock_fundamentals_cache.py
```

### 3️⃣ 命令行参数
```bash
# 全量更新所有股票
python3 stock_fundamentals_cache.py

# 测试100只股票
export MAX_STOCKS=100
python3 stock_fundamentals_cache.py

# 大批次快速处理
export BATCH_SIZE=200
export MAX_STOCKS=500
python3 stock_fundamentals_cache.py
```

## 📁 生成文件

### 缓存文件
```
cache/
├── stockA_fundamentals.csv      # 完整基本面数据
├── stockA_list.csv             # A股股票列表
├── update_log.json             # 更新日志
└── fundamentals_update_checkpoint.json  # 断点续传文件
```

### 数据格式示例
```csv
code,name,list_date,exchange,industry,total_assets,total_liabilities,total_equity,revenue,net_profit,eps,pe_ttm,pb,roe,debt_ratio,gross_margin,current_price,market_cap,update_time
000001,平安银行,1991-04-03,深交所,银行业,450000000,400000000,50000000,18000000,4500000,2.25,8.5,0.8,12.5,88.9,35.2,18.5,3500,2024-01-15T10:30:00
000002,万科A,1991-01-29,深交所,房地产,1800000000,1300000000,500000000,40000000,2500000,2.1,12.3,0.9,8.5,72.2,25.8,25.8,2800,2024-01-15T10:30:00
```

## ⚙️ 高级配置

### 自定义更新周期
```python
# 在代码中设置
from stock_fundamentals_cache import StockFundamentalsCache

cache = StockFundamentalsCache()

# 设置更新周期为7天
cache.UPDATE_INTERVAL_DAYS = 7

# 执行更新
cache.update_fundamentals_cache(batch_size=50, max_stocks=1000)
```

### 数据验证
```python
# 检查缓存信息
info = cache.get_cache_info()
print(json.dumps(info, indent=2, ensure_ascii=False))

# 加载缓存数据
df = cache.load_fundamentals_cache()
print(f"数据概览: {len(df)}只股票, {len(df.columns)}个字段")
```

## 🔧 故障排除

### 常见问题

#### 1. 网络连接问题
```bash
# 检查网络连接
ping finance.sina.com.cn

# 使用代理（如果需要）
export HTTP_PROXY=http://proxy.company.com:8080
export HTTPS_PROXY=http://proxy.company.com:8080
```

#### 2. 内存不足
```bash
# 减少批次大小
export BATCH_SIZE=20
python3 stock_fundamentals_cache.py
```

#### 3. 断点续传
```bash
# 查看断点信息
cat cache/fundamentals_update_checkpoint.json

# 清理断点重新开始
rm cache/fundamentals_update_checkpoint.json
python3 stock_fundamentals_cache.py
```

### 日志监控
```bash
# 实时查看进度
tail -f cache/update_log.json

# 检查数据完整性
python3 -c "
import pandas as pd
df = pd.read_csv('cache/stockA_fundamentals.csv')
print(f'总股票数: {len(df)}')
print(f'数据完整性: {df.isnull().sum()}')
"
```

## 📊 性能优化建议

### 网络环境良好时
- 使用较大的`BATCH_SIZE`（100-200）
- 设置`MAX_STOCKS=0`处理所有股票

### 网络环境一般时
- 使用较小的`BATCH_SIZE`（20-50）
- 设置`MAX_STOCKS=500`先测试小规模

### 开发测试时
- 设置`MAX_STOCKS=100`快速验证
- 使用`BATCH_SIZE=10`观察详细过程

## 🎯 实际使用场景

| 场景 | 推荐配置 | 预期时间 |
|------|----------|----------|
| **首次全量更新** | `BATCH_SIZE=100 MAX_STOCKS=0` | 2-3小时 |
| **定期更新** | `BATCH_SIZE=50 MAX_STOCKS=0` | 1-2小时 |
| **策略验证** | `BATCH_SIZE=100 MAX_STOCKS=1000` | 30分钟 |
| **功能测试** | `BATCH_SIZE=10 MAX_STOCKS=50` | 5分钟 |

## 🔄 与选股系统集成

### 在选股程序中使用缓存
```python
# 在stock_selector.py中集成
from stock_fundamentals_cache import StockFundamentalsCache

cache = StockFundamentalsCache()

# 确保缓存是最新的
if cache.should_update_cache():
    cache.update_fundamentals_cache()

# 加载缓存数据用于选股
df = cache.load_fundamentals_cache()
# 使用df进行选股分析...
```

## 📞 技术支持

如有问题，请检查：
1. ✅ 网络连接是否正常
2. ✅ akshare库是否最新
3. ✅ 磁盘空间是否充足
4. ✅ 文件权限是否正确

系统完全按照README.md要求实现，包含所有指定的数据字段和定期更新机制！