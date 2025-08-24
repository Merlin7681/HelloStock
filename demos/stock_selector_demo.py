#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多样化优质股票选择策略系统 - 演示版本
使用模拟数据展示多种选股策略
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

class StockSelectorDemo:
    """股票选择器演示类"""
    
    def __init__(self):
        # 创建模拟股票数据
        self.create_demo_data()
    
    def create_demo_data(self):
        """创建演示用的股票数据"""
        # 模拟50只A股股票数据
        np.random.seed(42)  # 确保结果可重复
        
        # 创建基础股票列表
        base_stocks = [
            {'code': '600519', 'name': '贵州茅台', 'price': 1800.0, 'market_cap': 22000},
            {'code': '000858', 'name': '五粮液', 'price': 180.5, 'market_cap': 7000},
            {'code': '000002', 'name': '万科A', 'price': 15.8, 'market_cap': 1800},
            {'code': '600036', 'name': '招商银行', 'price': 35.2, 'market_cap': 8800},
            {'code': '300750', 'name': '宁德时代', 'price': 240.8, 'market_cap': 10500},
            {'code': '002594', 'name': '比亚迪', 'price': 280.5, 'market_cap': 8100},
            {'code': '300124', 'name': '汇川技术', 'price': 68.9, 'market_cap': 1800},
            {'code': '002371', 'name': '北方华创', 'price': 320.6, 'market_cap': 1650},
            {'code': '600900', 'name': '长江电力', 'price': 22.8, 'market_cap': 5500},
            {'code': '600028', 'name': '中国石化', 'price': 5.2, 'market_cap': 6200},
            {'code': '600019', 'name': '宝钢股份', 'price': 7.1, 'market_cap': 1600},
            {'code': '601088', 'name': '中国神华', 'price': 35.6, 'market_cap': 7100}
        ]
        
        # 添加随机股票
        random_stocks = [
            {'code': f'600{i:03d}', 'name': f'股票{i}', 'price': np.random.uniform(5, 500), 'market_cap': np.random.uniform(100, 8000)} 
            for i in range(101, 139)
        ]
        
        stocks = base_stocks + random_stocks
        
        # 创建DataFrame
        df = pd.DataFrame(stocks)
        
        # 添加财务指标
        df['pe'] = np.random.normal(20, 10, len(df))  # 市盈率
        df['pe'] = np.clip(df['pe'], 5, 100)  # 限制范围
        
        df['pb'] = np.random.normal(2.5, 1.5, len(df))  # 市净率
        df['pb'] = np.clip(df['pb'], 0.5, 10)
        
        df['roe'] = np.random.normal(15, 8, len(df))  # 净资产收益率
        df['roe'] = np.clip(df['roe'], 2, 40)
        
        df['debt_ratio'] = np.random.normal(45, 15, len(df))  # 资产负债率
        df['debt_ratio'] = np.clip(df['debt_ratio'], 10, 85)
        
        df['revenue_growth'] = np.random.normal(20, 30, len(df))  # 营收增长率
        df['revenue_growth'] = np.clip(df['revenue_growth'], -20, 100)
        
        df['profit_growth'] = np.random.normal(25, 35, len(df))  # 净利润增长率
        df['profit_growth'] = np.clip(df['profit_growth'], -25, 120)
        
        df['current_ratio'] = np.random.normal(2.0, 1.0, len(df))  # 流动比率
        df['current_ratio'] = np.clip(df['current_ratio'], 0.8, 5.0)
        
        # 修正知名股票的财务数据
        famous_stocks = {
            '600519': {'pe': 35, 'pb': 12, 'roe': 28, 'debt_ratio': 15, 'revenue_growth': 18, 'profit_growth': 20},
            '000858': {'pe': 25, 'pb': 8, 'roe': 22, 'debt_ratio': 25, 'revenue_growth': 15, 'profit_growth': 18},
            '300750': {'pe': 45, 'pb': 6, 'roe': 18, 'debt_ratio': 45, 'revenue_growth': 45, 'profit_growth': 55},
            '002594': {'pe': 55, 'pb': 8, 'roe': 15, 'debt_ratio': 65, 'revenue_growth': 35, 'profit_growth': 40},
            '600036': {'pe': 8, 'pb': 1.2, 'roe': 16, 'debt_ratio': 92, 'revenue_growth': 8, 'profit_growth': 12},
            '600900': {'pe': 18, 'pb': 2.8, 'roe': 12, 'debt_ratio': 55, 'revenue_growth': 5, 'profit_growth': 8},
        }
        
        for code, metrics in famous_stocks.items():
            mask = df['code'] == code
            for key, value in metrics.items():
                df.loc[mask, key] = value
        
        self.stock_data = df
    
    def value_strategy(self, df):
        """价值投资策略：低估值+稳定盈利"""
        conditions = (
            (df['pe'] < 15) & (df['pe'] > 0) &
            (df['pb'] < 2) & (df['pb'] > 0) &
            (df['roe'] > 12) &
            (df['debt_ratio'] < 50) &
            (df['market_cap'] > 500)
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '价值投资'
        selected['reason'] = '低估值+高ROE+稳健财务'
        selected['score'] = (100/df['pe']) * 0.3 + (100/df['pb']) * 0.2 + df['roe'] * 0.5
        
        return selected.sort_values('score', ascending=False).head(8)
    
    def growth_strategy(self, df):
        """成长投资策略：高增长+合理估值"""
        conditions = (
            (df['revenue_growth'] > 25) &
            (df['profit_growth'] > 25) &
            (df['roe'] > 15) &
            (df['pe'] < 50) & (df['pe'] > 0) &
            (df['debt_ratio'] < 60)
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '成长投资'
        selected['reason'] = '高增长+合理估值+行业前景'
        selected['score'] = df['revenue_growth'] * 0.25 + df['profit_growth'] * 0.25 + df['roe'] * 0.5
        
        return selected.sort_values('score', ascending=False).head(8)
    
    def quality_strategy(self, df):
        """质量投资策略：高ROE+低负债"""
        conditions = (
            (df['roe'] > 18) &
            (df['debt_ratio'] < 40) &
            (df['current_ratio'] > 1.5) &
            (df['profit_growth'] > 10) &
            (df['market_cap'] > 1000)
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '质量投资'
        selected['reason'] = '高ROE+低负债+现金流好'
        selected['score'] = df['roe'] * 0.6 + (100-df['debt_ratio']) * 0.2 + df['current_ratio'] * 0.2
        
        return selected.sort_values('score', ascending=False).head(8)
    
    def dividend_strategy(self, df):
        """股息投资策略：高分红+稳定盈利"""
        # 模拟股息率计算（实际应用中需要真实股息数据）
        df['dividend_yield'] = np.random.normal(3.5, 2.0, len(df))
        df['dividend_yield'] = np.clip(df['dividend_yield'], 0, 8)
        
        # 修正知名股票的股息率
        dividend_stocks = {
            '600519': 1.2, '000858': 2.1, '600036': 4.8, '600900': 3.2,
            '600028': 6.5, '601088': 7.2, '600019': 4.1
        }
        
        for code, yield_rate in dividend_stocks.items():
            mask = df['code'] == code
            df.loc[mask, 'dividend_yield'] = yield_rate
        
        conditions = (
            (df['dividend_yield'] > 3) &
            (df['roe'] > 10) &
            (df['debt_ratio'] < 55) &
            (df['profit_growth'] > 0)
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '股息投资'
        selected['reason'] = '高分红+稳定盈利+现金流好'
        selected['score'] = df['dividend_yield'] * 0.4 + df['roe'] * 0.4 + (100-df['debt_ratio']) * 0.2
        
        return selected.sort_values('score', ascending=False).head(8)
    
    def small_cap_strategy(self, df):
        """小盘成长策略：小市值+高成长"""
        conditions = (
            (df['market_cap'] < 500) & (df['market_cap'] > 50) &
            (df['revenue_growth'] > 30) &
            (df['profit_growth'] > 30) &
            (df['roe'] > 15) &
            (df['debt_ratio'] < 45)
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '小盘成长'
        selected['reason'] = '小市值+高成长+低负债'
        selected['score'] = df['profit_growth'] * 0.4 + df['revenue_growth'] * 0.3 + df['roe'] * 0.3
        
        return selected.sort_values('score', ascending=False).head(8)
    
    def run_all_strategies(self):
        """运行所有选股策略"""
        print("🚀 开始执行多样化股票选择策略...")
        print("📊 使用模拟数据演示各种选股策略...")
        
        strategies = [
            ('价值投资', self.value_strategy),
            ('成长投资', self.growth_strategy),
            ('质量投资', self.quality_strategy),
            ('股息投资', self.dividend_strategy),
            ('小盘成长', self.small_cap_strategy)
        ]
        
        all_results = []
        
        for strategy_name, strategy_func in strategies:
            try:
                result = strategy_func(self.stock_data)
                if not result.empty:
                    result['strategy_name'] = strategy_name
                    all_results.append(result)
                    print(f"✅ {strategy_name}: 选出 {len(result)} 只股票")
            except Exception as e:
                print(f"⚠️ {strategy_name}: {len(strategy_func(self.stock_data))} 只股票")
        
        # 合并所有结果
        if all_results:
            final_results = pd.concat(all_results, ignore_index=True)
            # 去重并排序
            final_results = final_results.sort_values('score', ascending=False).drop_duplicates(subset=['code'], keep='first')
            return final_results.head(30)
        else:
            return pd.DataFrame()
    
    def format_results(self, results):
        """格式化输出结果"""
        if results.empty:
            return "未找到符合条件的股票"
        
        output = []
        output.append("=" * 90)
        output.append("🏆 多样化优质股票选择策略结果")
        output.append("=" * 90)
        output.append("")
        
        # 按策略分组
        strategy_groups = results.groupby('strategy_name')
        
        for strategy_name, group in strategy_groups:
            output.append(f"🎯 {strategy_name}策略")
            output.append("-" * 50)
            
            for idx, stock in group.iterrows():
                output.append(f"  📈 {stock['name']} ({stock['code']})")
                output.append(f"     💰 价格: ¥{stock['price']:.2f} | 市值: ¥{stock['market_cap']:.0f}亿")
                output.append(f"     📊 PE: {stock['pe']:.1f} | PB: {stock['pb']:.1f} | ROE: {stock['roe']:.1f}%")
                
                if pd.notna(stock.get('dividend_yield')):
                    output.append(f"     💵 股息率: {stock['dividend_yield']:.1f}%")
                
                output.append(f"     📈 营收增长: {stock['revenue_growth']:.1f}% | 利润增长: {stock['profit_growth']:.1f}%")
                output.append(f"     🔍 选择原因: {stock['reason']}")
                output.append(f"     ⭐ 评分: {stock['score']:.2f}")
                output.append("")
        
        # 策略统计
        strategy_summary = results['strategy_name'].value_counts()
        output.append("📊 策略分布统计:")
        for strategy, count in strategy_summary.items():
            output.append(f"   • {strategy}: {count} 只股票")
        
        output.append("")
        output.append("💡 投资建议:")
        output.append("   • 价值投资: 适合稳健型投资者，长期持有")
        output.append("   • 成长投资: 适合积极型投资者，关注行业前景")
        output.append("   • 质量投资: 适合价值投资者，重视财务质量")
        output.append("   • 股息投资: 适合收益型投资者，追求稳定分红")
        output.append("   • 小盘成长: 适合激进型投资者，高风险高收益")
        
        return "\n".join(output)

def main():
    """主函数"""
    print("🚀 启动多样化股票选择系统...")
    
    selector = StockSelectorDemo()
    results = selector.run_all_strategies()
    
    if not results.empty:
        formatted_output = selector.format_results(results)
        print(formatted_output)
        
        # 保存结果
        results.to_csv('selected_stocks_demo.csv', index=False, encoding='utf-8-sig')
        print("\n✅ 结果已保存到 selected_stocks_demo.csv")
        
        # 显示前10名综合推荐
        print("\n🎖️ 综合推荐TOP10:")
        top10 = results.head(10)
        for i, (_, stock) in enumerate(top10.iterrows(), 1):
            print(f"{i:2d}. {stock['name']} ({stock['code']}) - {stock['strategy_name']} - 评分:{stock['score']:.1f}")
    else:
        print("❌ 选股过程遇到问题")

if __name__ == "__main__":
    main()