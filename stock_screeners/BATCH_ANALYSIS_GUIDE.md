# 📊 分批股票分析系统使用指南

## 🎯 功能概述

新的分批处理系统支持**全量A股股票分析**，采用分批处理+断点续传机制，确保稳定性和效率。

## ⚙️ 环境变量配置

| 变量名 | 说明 | 默认值 | 示例 |
|--------|------|--------|------|
| `BATCH_SIZE` | 每批处理的股票数量 | 50 | `BATCH_SIZE=100` |
| `MAX_STOCKS` | 最大处理股票数量（0=全部） | 0 | `MAX_STOCKS=1000` |

## 🚀 使用示例

### 1. 处理所有A股股票（5,741只）
```bash
# 使用默认批次大小(50只/批)
python3 stock_selector.py

# 或明确指定
BATCH_SIZE=50 MAX_STOCKS=0 python3 stock_selector.py
```

### 2. 处理前1000只股票
```bash
BATCH_SIZE=50 MAX_STOCKS=1000 python3 stock_selector.py
```

### 3. 小批量测试
```bash
# 每批10只，总共处理100只
BATCH_SIZE=10 MAX_STOCKS=100 python3 stock_selector.py
```

### 4. 快速测试
```bash
# 每批3只，总共处理30只
BATCH_SIZE=3 MAX_STOCKS=30 python3 stock_selector.py
```

## 📈 处理进度展示

系统会显示详细的处理进度：

```
📈 开始分批获取财务数据，共 5741 只股票，每批50只...

🔄 处理第 1/115 批 (1-50 只股票)
💾 已处理 50 只股票，进度已保存

🔄 处理第 2/115 批 (51-100 只股票)
💾 已处理 100 只股票，进度已保存
...
💾 完整基本面数据已缓存到 cache/stock_fundamentals.csv (4820条数据)
```

## 🔄 断点续传功能

### 自动续传
如果程序中断，下次运行时会自动从断点继续：

```
📂 从断点续传，已处理 1250 只股票
🔄 处理第 26/115 批 (1251-1300 只股票)
```

### 手动清理断点
如果需要重新开始：
```bash
rm cache/fundamentals_checkpoint.json
python3 stock_selector.py
```

## 📊 输出文件

处理完成后会生成以下文件：
- `cache/stock_fundamentals.csv` - 所有股票的基本面数据
- `selected_stocks.csv` - 精选股票列表
- `selected_stocks.json` - 股票代码和名称
- `selected_stocks.md` - 详细分析报告

## ⚡ 性能优化建议

### 网络环境良好时
- 使用较大的`BATCH_SIZE`（100-200）
- 减少API调用间隔

### 网络环境一般时
- 使用较小的`BATCH_SIZE`（20-50）
- 增加处理间隔

### 开发测试时
- 设置`MAX_STOCKS=100`快速验证
- 使用`BATCH_SIZE=10`观察详细过程

## 🎯 实际使用场景

| 场景 | 推荐配置 |
|------|----------|
| **全量分析** | `BATCH_SIZE=100 MAX_STOCKS=0` |
| **策略验证** | `BATCH_SIZE=50 MAX_STOCKS=500` |
| **功能测试** | `BATCH_SIZE=10 MAX_STOCKS=50` |
| **网络调试** | `BATCH_SIZE=5 MAX_STOCKS=20` |

## 🔍 监控处理状态

可以通过以下文件监控进度：
- `cache/fundamentals_checkpoint.json` - 断点信息
- 控制台实时输出 - 当前批次和进度
- 最终统计信息 - 成功处理的股票数量