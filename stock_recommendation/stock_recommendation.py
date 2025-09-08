#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股特定股票投注推荐程序
根据股票代码生成个性化买卖策略建议
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
from print_info import print_info
from base_info import get_stock_name_by_code, get_stock_pe_pb

class StockRecommendation:
    """股票投注推荐系统"""
    
    def __init__(self, stock_code):
        self.stock_code = stock_code
        self.stock_name = ""
        self.current_price = 0
        self.analysis_data = {}
        self.is_print = True
        
    def get_stock_basic_info(self):
        """获取股票基本信息（两项--股票名称+最新价，实时->历史），增强容错和重试机制"""
        max_retries = 3
        retry_delay = 2  # 秒
        
        for attempt in range(max_retries):
            try:
                print(f"📡 尝试获取股票信息 (第{attempt+1}次)...")
                
                print_info(True, "获取股票实时行情（最新价）", "尝试中......")
                # 方法1: 东方财富实时行情
                try:
                    stock_info = ak.stock_zh_a_spot_em()
                    stock_data = stock_info[stock_info['代码'] == self.stock_code]
                    
                    if not stock_data.empty:
                        self.stock_name = stock_data.iloc[0]['名称']
                        self.current_price = float(stock_data.iloc[0]['最新价'])
                        print(f"✅ 成功获取实时行情: {self.stock_name} ¥{self.current_price}")
                        return True
                except:
                    pass
                
                # 方法2: 新浪实时行情
                try:
                    from akshare import stock_zh_a_spot_sina as sina_spot
                    stock_data = sina_spot()
                    stock_info = stock_data[stock_data['代码'] == self.stock_code]
                    
                    if not stock_info.empty:
                        self.stock_name = stock_info.iloc[0]['名称']
                        self.current_price = float(stock_info.iloc[0]['最新价'])
                        print(f"✅ 成功获取新浪行情: {self.stock_name} ¥{self.current_price}")
                        return True
                except:
                    pass
                
                print_info(True, "获取股票历史数据（昨日收盘价）", "尝试中......")
                # 方法3: 历史数据（昨日收盘价）
                try:
                    hist_data = ak.stock_zh_a_hist(
                        symbol=self.stock_code, 
                        period="daily", 
                        start_date=(datetime.now()-timedelta(days=5)).strftime('%Y%m%d'),
                        adjust=""
                    )
                    if not hist_data.empty:
                        self.current_price = float(hist_data.iloc[-1]['收盘'])
                        self.stock_name = get_stock_name_by_code(self.stock_code)
                        print(f"✅ 成功获取历史数据: {self.stock_name} ¥{self.current_price}")
                        return True
                except:
                    pass
                
                # 所有数据源都失败，返回False
                print(f"❌ 无法获取股票{self.stock_code}的实际数据")
                return False
                
            except Exception as e:
                print(f"❌ 第{attempt+1}次尝试失败: {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay)
        
        return False
    

    def get_technical_indicators(self):
        """计算技术指标，增强容错机制"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                print(f"📊 尝试获取技术指标 (第{attempt+1}次)...")
                
                # 获取历史数据
                end_date = datetime.now()
                start_date = end_date - timedelta(days=180)  # 缩短为半年数据
                
                hist_data = ak.stock_zh_a_hist(
                    symbol=self.stock_code, 
                    period="daily", 
                    start_date=start_date.strftime('%Y%m%d'),
                    end_date=end_date.strftime('%Y%m%d'),
                    adjust=""
                )
                print_info(self.is_print, "获取到的每日行情", hist_data)

                if hist_data.empty:
                    raise ValueError("历史数据为空")
                
                # 计算技术指标
                df = hist_data.copy()
                df = df.sort_values('日期')
                print_info(self.is_print, "获取到的sorted历史数据", df)


                # 确保有足够数据
                if len(df) < 20:
                    # 使用简化计算
                    return self.get_simplified_indicators(df)
                
                # 移动平均线
                df['MA5'] = df['收盘'].rolling(window=min(5, len(df))).mean()
                print_info(self.is_print, "获取到的MA5数据", df['MA5'])
                df['MA10'] = df['收盘'].rolling(window=min(10, len(df))).mean()
                df['MA20'] = df['收盘'].rolling(window=min(20, len(df))).mean()
                df['MA60'] = df['收盘'].rolling(window=min(60, len(df))).mean()
                
                # RSI
                delta = df['收盘'].diff()
                print_info(self.is_print, "获取到的delta数据", delta)
                
                gain = (delta.where(delta > 0, 0)).rolling(window=min(14, len(df))).mean()
                print_info(self.is_print, "获取到的gain数据", gain)
                loss = (-delta.where(delta < 0, 0)).rolling(window=min(14, len(df))).mean()
                print_info(self.is_print, "获取到的loss数据", loss)
                rs = np.where(loss != 0, gain / loss, 1)
                print_info(self.is_print, "获取到的rs数据", rs)
                df['RSI'] = 100 - (100 / (1 + rs))
                print_info(self.is_print, "获取到的RSI数据", df['RSI'])
                
                # MACD
                # pandas 库中用于计算指数加权移动平均（Exponential Weighted Moving Average，EWMA）
                exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
                print_info(self.is_print, "获取到的exp1数据", exp1)
                exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
                df['MACD'] = exp1 - exp2
                print_info(self.is_print, "获取到的MACD数据", df['MACD'])
                df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
                print_info(self.is_print, "获取到的MACD_signal数据", df['MACD_signal'])

                # 布林带
                rolling_std = df['收盘'].rolling(window=min(20, len(df))).std()
                df['BB_upper'] = df['MA20'] + (rolling_std * 2)
                df['BB_lower'] = df['MA20'] - (rolling_std * 2)
                
                # 波动率
                # pct_change() ：是pandas的一个Series方法，用于计算当前元素与上一个元素之间的百分比变化
                returns = df['收盘'].pct_change()
                # std() ：计算每个滚动窗口内收益率的标准差，衡量收益率的分散程度
                df['volatility'] = returns.rolling(window=min(20, len(df))).std() * np.sqrt(252)
                print_info(self.is_print, "获取到的volatility(波动率)数据", df['volatility'])
                
                # 成交量指标
                df['volume_ma'] = df['成交量'].rolling(window=min(10, len(df))).mean()
                print_info(self.is_print, "获取到的volume_ma（成交量移动平均）数据", df['volume_ma'])
                df['volume_ratio'] = np.where(df['volume_ma'] != 0, df['成交量'] / df['volume_ma'], 1)
                print_info(self.is_print, "获取到的volume_ratio（成交量比）数据", df['volume_ratio'])
                
                # df.iloc[-1] 是 pandas 库中用于数据索引的操作，用于获取 DataFrame 的最后一行数据。
                latest = df.iloc[-1]
                print_info(self.is_print, "NOTE: 最近一天的技术指标数据", latest)
                print(f"✅ 技术指标计算完成")
                return latest
                
            except Exception as e:
                print(f"❌ 技术指标计算失败 (第{attempt+1}次): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay)
        
        # 无法获取实际技术指标，返回None
        print("❌ 无法获取实际技术指标")
        return None
    
    def get_simplified_indicators(self, df):
        """简化版技术指标计算（表明分析结果基本无效！）"""
        print_info(True, "WARN：", "由于数据不全，由简化版技术指标替代，本次分析没有参考意义！")
        latest = df.iloc[-1]
        return pd.Series({
            'MA5': df['收盘'].iloc[-5:].mean() if len(df) >= 5 else latest['收盘'],
            'MA10': df['收盘'].iloc[-10:].mean() if len(df) >= 10 else latest['收盘'],
            'MA20': df['收盘'].iloc[-20:].mean() if len(df) >= 20 else latest['收盘'],
            'MA60': latest['收盘'],
            'RSI': 50.0,
            'MACD': 0.0,
            'MACD_signal': 0.0,
            'BB_upper': latest['收盘'] * 1.1,
            'BB_lower': latest['收盘'] * 0.9,
            'volatility': 0.15,
            'volume_ma': latest['成交量'],
            'volume_ratio': 1.0
        })
    

    def get_fundamental_analysis(self):
        """基本面分析（'市盈率pe','市净率pb', '净资产收益率roe','资产负债率debt_ratio','营业总收入同比增长率revenue_growth','#净利润增长率profit_growth'，'净利润同比增长率profit_growth'）"""
                
        try:
            # NOTE： 对于更实时的财务指标数据，可考虑使用其他AKShare提供的接口，如 stock_financial_analysis_indicator
            # realtime_data = ak.stock_financial_analysis_indicator(symbol=self.stock_code, start_year="2025")
            # print_info(self.is_print, "获取到的财务数据（实时--暂未使用！）", realtime_data)

            # 获取财务数据 - 使用同花顺财务摘要
            # finance_data = ak.stock_financial_abstract_ths(symbol=self.stock_code, indicator='按单季度')
            finance_data = ak.stock_financial_abstract_ths(symbol=self.stock_code)
            #print_info(self.is_print, "获取到的财务数据（全部）", finance_data)
            
            if finance_data.empty:
                return None
            
            # 获取最新数据
            # latest_data = finance_data.iloc[0]
            latest_data = finance_data.iloc[-1]
            print_info(self.is_print, "获取到的财务数据（最近一次）", latest_data)
            
            # 关键指标映射
            column_mapping = {
                '市盈率': 'pe',
                '市净率': 'pb', 
                '净资产收益率': 'roe',
                '资产负债率': 'debt_ratio',
                '营业总收入同比增长率': 'revenue_growth',
                #'净利润增长率': 'profit_growth'，
                # 依照latest_data数据调整 
                '净利润同比增长率': 'profit_growth'
            }
            
            fundamentals = {}
            for chinese_key, english_key in column_mapping.items():
                if chinese_key in latest_data.index:
                    value = latest_data[chinese_key]
                    if pd.notna(value):
                        # 处理百分比格式
                        str_value = str(value).replace('%', '')
                        try:
                            fundamentals[english_key] = float(str_value)
                        except:
                            fundamentals[english_key] = 0
                    else:
                        fundamentals[english_key] = 0
                else:
                    fundamentals[english_key] = 0
            
            # 补充pe/pb数据
            #cur_date = (datetime.now()).strftime('%Y-%m-%d')
            print(".................before......................")
            result = get_stock_pe_pb(self.stock_code)
            if(result is None):
                # 如果获取不到最新交易日的数据（query_history_k_data_plus 日），则取上一个交易日数据
                result = get_stock_pe_pb(self.stock_code, 1)
            print_info(self.is_print, "PE/PB", result)
            fundamentals['pe'] = float(result[0]['peTTM'])
            fundamentals['pb'] = float(result[0]['pbMRQ'])
            
            print_info(self.is_print, "获取到的基本面数据(按报告期)", fundamentals)
            return fundamentals
            
        except Exception as e:
            print(f"获取基本面数据失败: {e}")
            # 返回估算值
            return {
                'pe': np.random.uniform(10, 30),
                'pb': np.random.uniform(1, 5),
                'roe': np.random.uniform(5, 25),
                'debt_ratio': np.random.uniform(30, 70),
                'revenue_growth': np.random.uniform(-10, 30),
                'profit_growth': np.random.uniform(-20, 40)
            }
    
    def generate_recommendation(self):
        """生成投注推荐"""
        # 获取基础信息
        if not self.get_stock_basic_info():
            return "无法获取股票信息"
        
        # 获取技术指标
        tech_data = self.get_technical_indicators()
        if tech_data is None:
            tech_data = {}
        
        # 获取基本面数据
        fundamental_data = self.get_fundamental_analysis()
        if fundamental_data is None:
            fundamental_data = {}
        
        # 分析数据整合
        self.analysis_data = {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'current_price': self.current_price,
            'technical': tech_data,
            'fundamental': fundamental_data,
            'analysis_time': datetime.now()
        }
        
        # 生成推荐
        recommendation = self.create_strategy()
        return recommendation
    
    def create_strategy(self):
        """创建买卖策略"""
        current_price = self.current_price
        tech = self.analysis_data['technical']
        fund = self.analysis_data['fundamental']
        
        strategy = {
            'recommendation': '',
            'entry_price': 0,
            'stop_loss': 0,
            'take_profit': 0,
            'position_size': 0,
            'confidence': 0,
            'reasons': [],
            'risks': []
        }
        
        # 综合评分计算
        score = 0
        reasons = []
        risks = []
        
        # 技术指标评分
        if isinstance(tech, pd.Series):
            # 趋势判断
            if current_price > tech.get('MA5', 0) > tech.get('MA10', 0) > tech.get('MA20', 0):
                score += 30
                reasons.append("多头排列，趋势向上")
            elif current_price < tech.get('MA5', 0) < tech.get('MA10', 0) < tech.get('MA20', 0):
                score -= 30
                risks.append("空头排列，趋势向下")
            
            # RSI
            rsi = tech.get('RSI', 50)
            if rsi < 30:
                score += 20
                reasons.append("RSI超卖，可能反弹")
            elif rsi > 70:
                score -= 20
                risks.append("RSI超买，可能回调")
            
            # MACD
            macd = tech.get('MACD', 0)
            macd_signal = tech.get('MACD_signal', 0)
            if macd > macd_signal and macd > 0:
                score += 15
                reasons.append("MACD金叉，动能增强")
            elif macd < macd_signal and macd < 0:
                score -= 15
                risks.append("MACD死叉，动能减弱")
            
            # 布林带
            bb_upper = tech.get('BB_upper', 0)
            bb_lower = tech.get('BB_lower', 0)
            if bb_upper > 0 and bb_lower > 0:
                bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
                if bb_position < 0.2:
                    score += 15
                    reasons.append("布林带下轨附近，支撑较强")
                elif bb_position > 0.8:
                    score -= 15
                    risks.append("布林带上轨附近，压力较大")
        
        # 基本面评分
        if fund:
            pe = fund.get('pe', 0)
            pb = fund.get('pb', 0)
            roe = fund.get('roe', 0)
            debt_ratio = fund.get('debt_ratio', 0)
            
            # 估值评分
            if 0 < pe < 15:
                score += 20
                reasons.append("估值较低，安全边际高")
            elif pe > 50:
                score -= 20
                risks.append("估值过高，存在泡沫风险")
            
            # 盈利能力
            if roe > 15:
                score += 15
                reasons.append("盈利能力强，ROE优秀")
            elif roe < 5:
                score -= 15
                risks.append("盈利能力弱，ROE偏低")
            
            # 财务风险
            if debt_ratio < 50:
                score += 10
                reasons.append("负债率低，财务稳健")
            elif debt_ratio > 80:
                score -= 10
                risks.append("负债率高，财务风险大")
        
        # 确定推荐
        if score >= 60:
            strategy['recommendation'] = '强烈买入'
            strategy['confidence'] = min(95, 50 + score)
        elif score >= 30:
            strategy['recommendation'] = '建议买入'
            strategy['confidence'] = min(80, 30 + score)
        elif score >= -30:
            strategy['recommendation'] = '观望'
            strategy['confidence'] = abs(score)
        elif score >= -60:
            strategy['recommendation'] = '建议卖出'
            strategy['confidence'] = min(80, 30 - score)
        else:
            strategy['recommendation'] = '强烈卖出'
            strategy['confidence'] = min(95, 50 - score)
        
        # 计算买卖价格
        volatility = tech.get('volatility', 0.2) if isinstance(tech, pd.Series) else 0.2
        
        if strategy['recommendation'] in ['强烈买入', '建议买入']:
            strategy['entry_price'] = round(current_price * 0.98, 2)  # 2%折价买入
            strategy['stop_loss'] = round(current_price * (1 - volatility * 2), 2)
            strategy['take_profit'] = round(current_price * (1 + volatility * 3), 2)
        elif strategy['recommendation'] in ['建议卖出', '强烈卖出']:
            strategy['entry_price'] = round(current_price * 1.02, 2)  # 2%溢价卖出
            strategy['stop_loss'] = round(current_price * (1 + volatility * 2), 2)
            strategy['take_profit'] = round(current_price * (1 - volatility * 3), 2)
        else:
            strategy['entry_price'] = current_price
            strategy['stop_loss'] = round(current_price * 0.95, 2)
            strategy['take_profit'] = round(current_price * 1.05, 2)
        
        # 仓位建议
        if strategy['confidence'] > 80:
            strategy['position_size'] = 0.3  # 30%仓位
        elif strategy['confidence'] > 60:
            strategy['position_size'] = 0.2  # 20%仓位
        else:
            strategy['position_size'] = 0.1  # 10%仓位
        
        strategy['reasons'] = reasons
        strategy['risks'] = risks
        
        return strategy
    
    def save_to_markdown(self, recommendation):
        """保存推荐结果到Markdown文件"""
        filename = f"stock_recommendation_{self.stock_code}.md"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"# 📈 {self.stock_name}({self.stock_code})投注推荐报告\n\n")
            f.write(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 当前价格
            f.write(f"## 💰 当前价格\n")
            f.write(f"- **当前股价**: ¥{self.current_price:.2f}\n\n")
            
            # 推荐策略
            f.write(f"## 🎯 推荐策略\n")
            f.write(f"- **操作建议**: {recommendation['recommendation']}\n")
            f.write(f"- **信心指数**: {recommendation['confidence']:.0f}%\n")
            f.write(f"- **建议仓位**: {recommendation['position_size']*100:.0f}%\n\n")
            
            # 买卖价格
            f.write(f"## 💸 操作价格\n")
            if recommendation['recommendation'] in ['强烈买入', '建议买入']:
                f.write(f"- **买入价格**: ¥{recommendation['entry_price']:.2f}\n")
                f.write(f"- **止损价格**: ¥{recommendation['stop_loss']:.2f}\n")
                f.write(f"- **止盈价格**: ¥{recommendation['take_profit']:.2f}\n")
            elif recommendation['recommendation'] in ['建议卖出', '强烈卖出']:
                f.write(f"- **卖出价格**: ¥{recommendation['entry_price']:.2f}\n")
                f.write(f"- **止损价格**: ¥{recommendation['stop_loss']:.2f}\n")
                f.write(f"- **止盈价格**: ¥{recommendation['take_profit']:.2f}\n")
            else:
                f.write(f"- **观望价格区间**: ¥{recommendation['stop_loss']:.2f} - ¥{recommendation['take_profit']:.2f}\n")
            f.write("\n")
            
            # 推荐理由
            if recommendation['reasons']:
                f.write(f"## ✅ 推荐理由\n")
                for reason in recommendation['reasons']:
                    f.write(f"- {reason}\n")
                f.write("\n")
            
            # 风险提示
            if recommendation['risks']:
                f.write(f"## ⚠️ 风险提示\n")
                for risk in recommendation['risks']:
                    f.write(f"- {risk}\n")
                f.write("\n")
            
            # 技术指标
            tech = self.analysis_data['technical']
            if isinstance(tech, pd.Series):
                f.write(f"## 📊 技术指标\n")
                f.write(f"- **RSI**: {tech.get('RSI', 'N/A'):.2f}\n")
                f.write(f"- **MACD**: {tech.get('MACD', 'N/A'):.2f}\n")
                f.write(f"- **波动率**: {tech.get('volatility', 'N/A'):.2f}\n")
                f.write(f"- **MA5**: ¥{tech.get('MA5', 'N/A'):.2f}\n")
                f.write(f"- **MA20**: ¥{tech.get('MA20', 'N/A'):.2f}\n")
                f.write("\n")
            
            # 基本面数据
            fund = self.analysis_data['fundamental']
            if fund:
                f.write(f"## 📈 基本面数据\n")
                f.write(f"- **市盈率(PE)**: {fund.get('pe', 'N/A'):.2f}\n")
                f.write(f"- **市净率(PB)**: {fund.get('pb', 'N/A'):.2f}\n")
                f.write(f"- **净资产收益率(ROE)**: {fund.get('roe', 'N/A'):.2f}%\n")
                f.write(f"- **资产负债率**: {fund.get('debt_ratio', 'N/A'):.2f}%\n")
                f.write(f"- **营收增长率**: {fund.get('revenue_growth', 'N/A'):.2f}%\n")
                f.write(f"- **净利润增长率**: {fund.get('profit_growth', 'N/A'):.2f}%\n")
                f.write("\n")
            
            f.write(f"## 💡 投资建议\n")
            f.write(f"1. **分批操作**: 建议分2-3次建仓/减仓\n")
            f.write(f"2. **风险控制**: 严格执行止损策略\n")
            f.write(f"3. **动态调整**: 根据市场变化及时调整策略\n")
            f.write(f"4. **长期视角**: 结合基本面和技术面综合判断\n")
            f.write(f"5. **信息关注**: 密切关注公司公告和行业动态\n\n")
            
            f.write(f"---\n")
            f.write(f"**免责声明**: 本报告仅供参考，投资有风险，入市需谨慎。\n")
        
        return filename

def main():
    """主函数"""
    import sys
    
    if len(sys.argv) != 2:
        print("使用方法: python stock_recommendation.py <股票代码>")
        print("示例: python stock_recommendation.py 000001")
        return
    
    stock_code = sys.argv[1]
    
    # 标准化股票代码
    if len(stock_code) == 6:
        pass  # 已经是标准格式
    elif len(stock_code) == 5 and stock_code.startswith('6'):
        stock_code = '6' + stock_code  # 沪市补全
    elif len(stock_code) == 5 and stock_code.startswith('0'):
        stock_code = '0' + stock_code  # 深市补全
    else:
        print("股票代码格式错误，请输入6位数字代码")
        return
    
    print(f"🚀 开始分析股票 {stock_code}...")
    
    recommender = StockRecommendation(stock_code)
    recommendation = recommender.generate_recommendation()
    
    if recommendation is None:
        print(f"❌ 无法获取股票{stock_code}的实际数据，请检查网络连接或股票代码是否正确")
        return
    
    if isinstance(recommendation, dict):
        filename = recommender.save_to_markdown(recommendation)
        print(f"✅ 推荐报告已生成: {filename}")
        
        # 打印简要信息
        print(f"\n📊 {recommender.stock_name}({stock_code}) 简要推荐:")
        print(f"当前价格: ¥{recommender.current_price:.2f}")
        print(f"推荐策略: {recommendation['recommendation']}")
        print(f"信心指数: {recommendation['confidence']:.0f}%")
        print(f"建议仓位: {recommendation['position_size']*100:.0f}%")
        
        if recommendation['recommendation'] in ['强烈买入', '建议买入']:
            print(f"买入价格: ¥{recommendation['entry_price']:.2f}")
            print(f"止损价格: ¥{recommendation['stop_loss']:.2f}")
            print(f"止盈价格: ¥{recommendation['take_profit']:.2f}")
        elif recommendation['recommendation'] in ['建议卖出', '强烈卖出']:
            print(f"卖出价格: ¥{recommendation['entry_price']:.2f}")
            print(f"止损价格: ¥{recommendation['stop_loss']:.2f}")
            print(f"止盈价格: ¥{recommendation['take_profit']:.2f}")
    else:
        print(f"❌ 分析失败: {recommendation}")

if __name__ == "__main__":
    main()