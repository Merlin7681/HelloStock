# 🌐 网络故障处理指南

## 🔧 常见问题及解决方案

### 1. 连接错误示例
```
获取股票信息失败: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))
```

### 2. 程序已增强的容错功能

✅ **自动重试机制**
- 最多重试3次
- 每次间隔2秒
- 智能退避策略

✅ **多数据源备份**
- 东方财富实时行情
- 新浪实时行情
- 历史数据备用
- 模拟数据兜底

✅ **智能降级处理**
- 网络故障时使用模拟数据
- 确保程序始终能生成报告
- 在报告中标注数据来源

### 3. 手动解决方案

#### 方法1: 检查网络连接
```bash
# 测试网络连通性
ping www.baidu.com
# 或
curl -I http://quote.eastmoney.com
```

#### 方法2: 更换网络环境
- 尝试切换到其他WiFi
- 使用手机热点
- 检查防火墙设置

#### 方法3: 使用代理（如需）
```bash
# 设置HTTP代理（如有需要）
export HTTP_PROXY=http://your-proxy:port
export HTTPS_PROXY=http://your-proxy:port
```

### 4. 验证修复效果

#### 测试单只股票
```bash
cd /Users/dora/Documents/JavaDemos/ChaoGu/stock_recommendation
python3 stock_recommendation.py 601318
```

#### 批量测试
```bash
python3 test_recommendation.py
```

### 5. 数据源优先级

1. **实时行情**（网络正常时）
   - 东方财富接口
   - 新浪接口

2. **历史数据**（实时接口失败时）
   - 最近5日K线数据
   - 昨日收盘价

3. **模拟数据**（所有接口失败时）
   - 基于历史均价的合理估算
   - 随机波动±10%
   - 包含完整技术指标

### 6. 识别数据来源

在生成的报告中，数据来源会显示为：
- ✅ "成功获取实时行情" - 实时数据
- ✅ "成功获取历史数据" - 历史数据
- ⚠️ "使用模拟数据作为示例" - 模拟数据

### 7. 网络恢复后

程序会自动优先使用真实数据，无需手动干预。如需强制刷新：

```bash
# 删除旧报告重新生成
rm stock_recommendation_*.md
python3 stock_recommendation.py 601318
```

### 8. 技术支持

如果持续遇到网络问题：

1. **检查akshare版本**
```bash
pip install --upgrade akshare
```

2. **查看错误日志**
程序会显示详细的错误信息，便于诊断问题

3. **联系数据源**
- 东方财富：https://quote.eastmoney.com
- 新浪财经：https://finance.sina.com.cn

---

**注意**: 即使网络故障，程序也能正常工作，只是使用模拟数据进行演示。真实交易前请确保获取到真实数据。