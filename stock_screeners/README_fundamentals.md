# A股股票基本面数据获取工具

## 🎯 功能概述

本工具支持从多个免费数据源获取A股股票基本面数据，包括：

- ✅ **baostock库** - 官方数据源，稳定性最高
- ✅ **东方财富API** - 实时数据，访问限制少
- ✅ **akshare库** - 多源整合，功能丰富

## 🚀 使用方法

### 1. 基本使用

```bash
# 自动选择最佳数据源（推荐）
python3 get_stockA_fundamentals.py

# 指定数据源
python3 get_stockA_fundamentals.py baostock
python3 get_stockA_fundamentals.py eastmoney
python3 get_stockA_fundamentals.py akshare
```

### 2. 数据源优先级

当使用 `auto` 模式时，系统按以下优先级选择数据源：

1. **baostock** - 最稳定的官方数据
2. **东方财富API** - 实时性好，限制少
3. **akshare** - 功能丰富，备用方案

### 3. 数据源特点对比

| 数据源 | 稳定性 | 数据完整性 | 访问限制 | 安装要求 |
|--------|--------|------------|----------|----------|
| baostock | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | 无 | 需安装 |
| 东方财富 | ⭐⭐⭐⭐ | ⭐⭐⭐ | 无 | 无 |
| akshare | ⭐⭐ | ⭐⭐⭐⭐⭐ | 有 | 无 |

## 📦 安装依赖

### 安装baostock（可选，但推荐）
```bash
pip install baostock
```

### 现有依赖
```bash
# 系统已安装
pandas
numpy
requests
akshare
```

## 📊 输出数据

### 数据文件
- `cache/stockA_fundamentals.csv` - 股票基本面数据
- `cache/fundamentals_progress.json` - 获取进度
- `cache/fundamentals_update_log.json` - 更新日志

### 数据字段
- 股票代码
- 股票名称
- 股票上市日期
- 股票上市地点
- 股票所属行业
- 每股收益
- 市盈率（静）
- 市盈率（TTM）
- 毛利率
- 净利率
- 资产收益率
- 资产负债率
- 净利润增速

## 🔄 断点续传

工具支持断点续传功能：
- 自动保存获取进度
- 支持中断后从断点继续
- 避免重复获取已处理股票

## ⚠️ 注意事项

1. **网络限制**：部分数据源可能有访问频率限制
2. **数据延迟**：免费数据可能存在延迟
3. **异常处理**：建议分批处理，避免一次性获取过多数据
4. **数据验证**：获取完成后建议检查数据完整性

## 🔧 故障排除

### 常见问题

1. **baostock未安装**
   ```bash
   pip install baostock
   ```

2. **网络连接问题**
   - 检查网络连接
   - 尝试更换数据源
   - 减小批次大小

3. **数据获取失败**
   - 检查股票代码有效性
   - 尝试不同数据源
   - 查看错误日志

### 调试模式
```bash
# 使用特定数据源调试
python3 get_stockA_fundamentals.py eastmoney
```

## 📈 使用示例

```python
# 在代码中使用
import get_stockA_fundamentals as gf

# 使用自动数据源
main()

# 使用特定数据源
main(data_source='baostock')
```