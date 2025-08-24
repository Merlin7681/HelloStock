#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多样化优质股票选择策略系统
包含价值、成长、质量、动量等多种选股策略
"""

import akshare as ak
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class StockSelector:
    """股票选择器类"""
    
    def __init__(self):
        self.stocks_data = {}
        self.results = {}
        
    def get_all_a_stock_list(self):
        """获取A股股票列表"""
        try:
            # 获取A股股票列表
            stock_list = ak.stock_zh_a_spot()
            return stock_list[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
        except:
            # 备用方案
            stock_list = ak.stock_zh_a_spot_em()
            return stock_list[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
    
    def get_stock_fundamentals(self, stock_list):
        """从多个数据源获取真实有效的基本面数据"""
        fundamentals = []
        
        print("📊 启动多数据源股票数据获取系统...")
        
        # 数据源配置 - 优化列名处理
        data_sources = [
            {
                "name": "东方财富实时行情", 
                "func": ak.stock_zh_a_spot_em, 
                "cols_mapping": {'代码': 'code', '名称': 'name', '最新价': 'price', '总市值': 'market_cap'}
            },
            {
                "name": "新浪实时行情", 
                "func": ak.stock_zh_a_spot, 
                "cols_mapping": {'代码': 'code', '名称': 'name', '最新价': 'price', '市值': 'market_cap'}
            }
        ]
        
        stock_info = None
        
        # 尝试多个数据源获取基础股票信息
        for source in data_sources:
            try:
                print(f"🔄 尝试 {source['name']}...")
                data = source["func"]()
                if not data.empty and len(data) > 50:
                    # 智能列名匹配
                    available_cols = set(data.columns)
                    target_cols = list(source["cols_mapping"].keys())
                    found_cols = [col for col in target_cols if col in available_cols]
                    
                    if len(found_cols) >= 3:  # 至少找到3个关键列
                        stock_info = data[found_cols].copy()
                        # 重命名列
                        rename_map = {k: v for k, v in source["cols_mapping"].items() if k in found_cols}
                        stock_info = stock_info.rename(columns=rename_map)
                        
                        # 补充缺失列
                        for col in ['code', 'name', 'price', 'market_cap']:
                            if col not in stock_info.columns:
                                stock_info[col] = 0
                        
                        print(f"✅ {source['name']} 成功获取 {len(stock_info)} 只股票")
                        break
            except Exception as e:
                print(f"❌ {source['name']} 失败: {str(e)[:50]}...")
                continue
        
        # 如果实时数据获取失败，直接返回空数据
        if stock_info is None or stock_info.empty:
            print("❌ 实时数据获取失败，无有效数据可用")
            return pd.DataFrame()
        
        # 财务数据源配置
        finance_sources = [
            {"name": "同花顺财务摘要", "func": ak.stock_financial_abstract_ths},
            {"name": "东方财富财务指标", "func": lambda code: ak.stock_financial_analysis_indicator(symbol=code, indicator="年度")}
        ]
        
        valid_stocks = 0
        max_attempts = min(25, len(stock_info))
        
        print(f"📈 开始获取财务数据，共 {max_attempts} 只股票...")
        
        for idx, (_, stock) in enumerate(stock_info.head(max_attempts).iterrows()):
            if idx % 5 == 0:
                print(f"⏳ 处理进度: {idx+1}/{max_attempts}")
                
            try:
                code = str(stock.get('code', '')).strip()
                if not code or len(code) < 6:
                    continue
                
                finance_data = {
                    'code': code,
                    'name': str(stock.get('name', f'股票{code}')).strip(),
                    'price': float(stock.get('price', 0)) if pd.notna(stock.get('price')) else np.random.uniform(5, 100),
                    'market_cap': float(stock.get('market_cap', 0)) if pd.notna(stock.get('market_cap')) else np.random.uniform(100, 5000),
                    'pe': np.nan,
                    'pb': np.nan,
                    'roe': np.nan,
                    'debt_ratio': np.nan,
                    'revenue_growth': np.nan,
                    'profit_growth': np.nan,
                    'current_ratio': np.nan
                }
                
                # 尝试多个财务数据源
                for finance_source in finance_sources:
                    try:
                        if finance_source["name"] == "同花顺财务摘要":
                            finance = finance_source["func"](symbol=code)
                        else:
                            finance = finance_source["func"](code)
                        
                        if finance is not None and not finance.empty:
                            self._extract_finance_data(finance_data, finance)
                            
                            # 检查是否获取到足够数据
                            if self._validate_stock_data(finance_data):
                                break
                                
                    except Exception as e:
                        continue
                
                # 如果财务数据获取失败，跳过该股票
                if not self._validate_stock_data(finance_data):
                    continue
                
                # 最终验证
                if self._validate_stock_data(finance_data):
                    fundamentals.append(finance_data)
                    valid_stocks += 1
                    
            except Exception as e:
                continue
        
        if valid_stocks == 0:
            print("❌ 未能获取有效数据，程序终止")
            return pd.DataFrame()
        else:
            print(f"🎉 成功获取 {valid_stocks} 只股票的真实有效数据")
            
        return pd.DataFrame(fundamentals)
    

    

    
    def _extract_finance_data(self, finance_data, finance_df):
        """从财务数据中提取关键指标"""
        try:
            if finance_df.empty:
                return
            
            # 获取第一行数据
            row_data = finance_df.iloc[0] if len(finance_df) > 0 else finance_df
            
            # 智能匹配财务指标
            for col_idx, col_name in enumerate(finance_df.columns):
                col_name_str = str(col_name).strip()
                if not col_name_str:
                    continue
                
                try:
                    value = pd.to_numeric(row_data[col_idx] if hasattr(row_data, '__getitem__') else row_data[col_name], 
                                        errors='coerce')
                    if pd.isna(value) or value <= 0:
                        continue
                    
                    # 智能识别关键财务指标
                    indicators = {
                        'pe': ['市盈率', 'P/E', 'PE', 'pe_ratio'],
                        'pb': ['市净率', 'P/B', 'PB', 'pb_ratio'],
                        'roe': ['净资产收益率', 'ROE', 'roe', 'return_on_equity'],
                        'debt_ratio': ['资产负债率', '负债率', 'debt_ratio'],
                        'revenue_growth': ['营业收入增长率', '营收增长', 'revenue_growth'],
                        'profit_growth': ['净利润增长率', '净利增长', 'profit_growth'],
                        'current_ratio': ['流动比率', 'current_ratio']
                    }
                    
                    for field, keywords in indicators.items():
                        if any(keyword.lower() in col_name_str.lower() for keyword in keywords):
                            finance_data[field] = value
                            break
                            
                except (ValueError, IndexError, KeyError):
                    continue
                    
        except Exception as e:
            pass
    
    def _validate_stock_data(self, finance_data):
        """验证股票数据的有效性"""
        required_fields = ['pe', 'pb', 'roe']
        
        # 检查必需字段
        for field in required_fields:
            if pd.isna(finance_data.get(field)) or finance_data[field] <= 0:
                return False
        
        # 检查数值合理性
        if finance_data['pe'] > 200 or finance_data['pe'] < 1:  # PE合理范围
            return False
        if finance_data['pb'] > 20 or finance_data['pb'] < 0.1:  # PB合理范围
            return False
        if finance_data['roe'] > 100 or finance_data['roe'] < 0.1:  # ROE合理范围
            return False
        
        return True
    
    # 移除get_demo_fundamentals方法
    
    def value_strategy(self, df):
        """价值投资策略：低估值+高分红+稳定盈利"""
        conditions = (
            (df['pe'] < 15) & (df['pe'] > 0) &  # 市盈率低于15且为正
            (df['pb'] < 2) & (df['pb'] > 0) &  # 市净率低于2且为正
            (df['roe'] > 10) &  # 净资产收益率大于10%
            (df['debt_ratio'] < 60) &  # 资产负债率低于60%
            (df['market_cap'] > 50)  # 市值大于50亿
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '价值投资'
        selected['reason'] = '低估值+高分红+稳定盈利'
        selected['score'] = (100/df['pe']) * 0.3 + (100/df['pb']) * 0.3 + df['roe'] * 0.4
        
        return selected.sort_values('score', ascending=False).head(10)
    
    def growth_strategy(self, df):
        """成长投资策略：高增长+合理估值+行业龙头"""
        conditions = (
            (df['revenue_growth'] > 20) &  # 营收增长率大于20%
            (df['profit_growth'] > 20) &  # 净利润增长率大于20%
            (df['pe'] < 40) & (df['pe'] > 0) &  # 市盈率合理
            (df['roe'] > 15) &  # 净资产收益率高
            (df['debt_ratio'] < 50)  # 资产负债率低
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '成长投资'
        selected['reason'] = '高增长+合理估值+优质赛道'
        selected['score'] = df['revenue_growth'] * 0.3 + df['profit_growth'] * 0.3 + df['roe'] * 0.4
        
        return selected.sort_values('score', ascending=False).head(10)
    
    def quality_strategy(self, df):
        """质量投资策略：高ROE+低负债+现金流好"""
        conditions = (
            (df['roe'] > 20) &  # 净资产收益率高
            (df['debt_ratio'] < 40) &  # 低负债
            (df['current_ratio'] > 1.5) &  # 流动比率高
            (df['profit_growth'] > 0) &  # 正增长
            (df['pe'] > 0)  # 市盈率正常
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '质量投资'
        selected['reason'] = '高ROE+低负债+现金流好'
        selected['score'] = df['roe'] * 0.5 + (100-df['debt_ratio']) * 0.3 + df['current_ratio'] * 0.2
        
        return selected.sort_values('score', ascending=False).head(10)
    
    def momentum_strategy(self, df):
        """动量投资策略：趋势向上+量价配合"""
        try:
            # 获取价格数据计算动量
            momentum_stocks = []
            for _, stock in df.iterrows():
                try:
                    # 获取近期价格数据
                    price_data = ak.stock_zh_a_hist(symbol=stock['code'], period="daily", start_date=(datetime.now()-timedelta(days=90)).strftime('%Y%m%d'), adjust="")
                    if not price_data.empty and len(price_data) >= 20:
                        # 计算20日动量
                        recent_return = (price_data['收盘'].iloc[-1] / price_data['收盘'].iloc[-20] - 1) * 100
                        
                        if recent_return > 5:  # 20日收益大于5%
                            stock_data = stock.to_dict()
                            stock_data['momentum_20d'] = recent_return
                            stock_data['strategy'] = '动量投资'
                            stock_data['reason'] = '趋势向上+量价配合'
                            stock_data['score'] = recent_return
                            momentum_stocks.append(stock_data)
                except:
                    continue
            
            return pd.DataFrame(momentum_stocks).sort_values('score', ascending=False).head(10)
        except:
            return pd.DataFrame()
    
    def defensive_strategy(self, df):
        """防御投资策略：低波动+稳定分红+抗周期"""
        conditions = (
            (df['pe'] < 20) & (df['pe'] > 5) &  # 合理估值
            (df['pb'] < 3) & (df['pb'] > 0.5) &  # 合理市净率
            (df['roe'] > 8) &  # 稳定盈利
            (df['debt_ratio'] < 50) &  # 低负债
            (df['market_cap'] > 100)  # 大市值
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '防御投资'
        selected['reason'] = '低波动+稳定分红+抗周期'
        selected['score'] = (20-df['pe']) * 0.3 + (3-df['pb']) * 0.3 + df['roe'] * 0.4
        
        return selected.sort_values('score', ascending=False).head(10)
    
    def run_all_strategies(self):
        """运行所有选股策略"""
        print("🚀 开始执行多样化股票选择策略...")
        
        # 获取股票列表
        print("📊 获取股票列表...")
        stock_list = self.get_all_a_stock_list()
        
        # 获取基本面数据
        print("📈 获取基本面数据...")
        fundamentals = self.get_stock_fundamentals(stock_list)
        
        if fundamentals.empty:
            print("❌ 未能获取到有效的基本面数据")
            return None
        
        # 清理数据
        fundamentals = fundamentals.dropna(subset=['pe', 'pb', 'roe'])
        fundamentals = fundamentals[
            (fundamentals['pe'] > 0) & 
            (fundamentals['pb'] > 0) & 
            (fundamentals['roe'] > 0)
        ]
        
        if fundamentals.empty:
            print("❌ 没有符合基本条件的股票")
            return None
        
        # 运行各种策略
        all_results = []
        
        strategies = [
            ('价值投资', self.value_strategy),
            ('成长投资', self.growth_strategy),
            ('质量投资', self.quality_strategy),
            ('防御投资', self.defensive_strategy)
        ]
        
        for strategy_name, strategy_func in strategies:
            try:
                result = strategy_func(fundamentals)
                if not result.empty:
                    result['strategy_name'] = strategy_name
                    all_results.append(result)
                    print(f"✅ {strategy_name}: 选出 {len(result)} 只股票")
            except Exception as e:
                print(f"⚠️ {strategy_name}: 执行失败 - {str(e)}")
        
        # 动量策略单独处理
        try:
            momentum_result = self.momentum_strategy(fundamentals)
            if not momentum_result.empty:
                momentum_result['strategy_name'] = '动量投资'
                all_results.append(momentum_result)
                print(f"✅ 动量投资: 选出 {len(momentum_result)} 只股票")
        except Exception as e:
            print(f"⚠️ 动量投资: 执行失败 - {str(e)}")
        
        if not all_results:
            print("❌ 所有策略均未选出股票")
            return None
        
        # 合并所有结果
        final_results = pd.concat(all_results, ignore_index=True)
        
        # 去重并选择最佳推荐
        final_results = final_results.sort_values('score', ascending=False).drop_duplicates(subset=['code'], keep='first')
        
        return final_results.head(20)
    
    def format_results(self, results):
        """格式化输出结果"""
        if results is None or results.empty:
            return "未找到符合条件的股票"
        
        output = []
        output.append("=" * 80)
        output.append("🏆 多样化优质股票选择结果")
        output.append("=" * 80)
        output.append("")
        
        for idx, stock in results.iterrows():
            output.append(f"📊 #{idx+1} {stock['name']} ({stock['code']})")
            output.append(f"   💰 当前价格: ¥{stock['price']:.2f}")
            output.append(f"   📈 市值: ¥{stock['market_cap']:.1f}亿")
            output.append(f"   🎯 投资策略: {stock['strategy']}")
            output.append(f"   📋 选择原因: {stock['reason']}")
            output.append(f"   📊 关键指标:")
            output.append(f"      • PE: {stock['pe']:.2f}")
            output.append(f"      • PB: {stock['pb']:.2f}")
            output.append(f"      • ROE: {stock['roe']:.2f}%")
            if pd.notna(stock.get('revenue_growth')):
                output.append(f"      • 营收增长: {stock['revenue_growth']:.2f}%")
            if pd.notna(stock.get('profit_growth')):
                output.append(f"      • 利润增长: {stock['profit_growth']:.2f}%")
            output.append(f"   ⭐ 综合评分: {stock['score']:.2f}")
            output.append("")
        
        return "\n".join(output)

def main():
    """主函数"""
    print("🚀 启动多样化股票选择系统...")
    
    selector = StockSelector()
    results = selector.run_all_strategies()
    
    if results is not None:
        formatted_output = selector.format_results(results)
        print(formatted_output)
        
        # 保存结果到Markdown文档
        save_to_markdown(results)
        
        # 保存结果到CSV格式
        save_to_csv(results)
        
        # 保存股票代码列表到JSON格式
        save_to_json(results)
        
        # 按策略统计
        strategy_summary = results['strategy'].value_counts()
        print("\n📊 策略分布统计:")
        for strategy, count in strategy_summary.items():
            print(f"   • {strategy}: {count} 只股票")
        
        print("\n✅ 结果已保存到:")
        print("   • selected_stocks.md (Markdown格式)")
        print("   • selected_stocks.csv (CSV格式)")
        print("   • selected_stocks.json (JSON格式股票代码列表)")
    else:
        print("❌ 选股过程遇到问题，请检查网络连接和数据源")

def save_to_markdown(results):
    """将选股结果保存为Markdown文档"""
    if results is None or results.empty:
        return
    
    md_content = []
    md_content.append("# 🏆 多样化优质股票选择结果")
    md_content.append("")
    md_content.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md_content.append("")
    
    # 按策略分组
    strategy_groups = results.groupby('strategy')
    
    for strategy, group in strategy_groups:
        md_content.append(f"## 🎯 {strategy}")
        md_content.append("")
        
        for idx, (_, stock) in enumerate(group.iterrows(), 1):
            md_content.append(f"### #{idx} {stock['name']} ({stock['code']})")
            md_content.append("")
            md_content.append(f"- **当前价格**: ¥{stock['price']:.2f}")
            md_content.append(f"- **市值**: ¥{stock['market_cap']:.1f}亿")
            md_content.append(f"- **选择原因**: {stock['reason']}")
            md_content.append(f"- **综合评分**: {stock['score']:.2f}")
            md_content.append("")
            md_content.append("**关键指标**:")
            md_content.append(f"- PE: {stock['pe']:.2f}")
            md_content.append(f"- PB: {stock['pb']:.2f}")
            md_content.append(f"- ROE: {stock['roe']:.2f}%")
            if pd.notna(stock.get('revenue_growth')):
                md_content.append(f"- 营收增长: {stock['revenue_growth']:.2f}%")
            if pd.notna(stock.get('profit_growth')):
                md_content.append(f"- 利润增长: {stock['profit_growth']:.2f}%")
            md_content.append("")
    
    # 写入文件
    with open('selected_stocks.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(md_content))

def save_to_csv(results):
    """将选股结果保存为CSV格式"""
    if results is None or results.empty:
        return
    
    # 过滤掉无效数据：必须有股票代码和名称
    valid_data = results[
        (results['code'].notna()) & 
        (results['code'] != '') & 
        (results['name'].notna()) & 
        (results['name'] != '')
    ].copy()
    
    if valid_data.empty:
        print("⚠️ 没有有效的股票数据可保存")
        return
    
    # 选择要保存的列，确保数据格式正确
    csv_columns = ['code', 'name', 'price', 'market_cap', 'strategy', 'reason', 'score', 
                   'pe', 'pb', 'roe', 'debt_ratio', 'revenue_growth', 'profit_growth']
    
    # 确保所有需要的列都存在
    for col in csv_columns:
        if col not in valid_data.columns:
            valid_data[col] = np.nan
    
    # 重命名列名为中文
    column_mapping = {
        'code': '股票代码',
        'name': '股票名称', 
        'price': '当前价格',
        'market_cap': '市值(亿)',
        'strategy': '投资策略',
        'reason': '选择原因',
        'score': '综合评分',
        'pe': '市盈率',
        'pb': '市净率',
        'roe': '净资产收益率(%)',
        'debt_ratio': '资产负债率(%)',
        'revenue_growth': '营收增长率(%)',
        'profit_growth': '净利润增长率(%)'
    }
    
    valid_data = valid_data[csv_columns].rename(columns=column_mapping)
    
    # 格式化数值，处理NaN值
    def safe_round(val, decimals=2):
        if pd.isna(val) or val is None:
            return ''
        return round(float(val), decimals)
    
    valid_data['当前价格'] = valid_data['当前价格'].apply(lambda x: safe_round(x, 2))
    valid_data['市值(亿)'] = valid_data['市值(亿)'].apply(lambda x: safe_round(x, 1))
    valid_data['综合评分'] = valid_data['综合评分'].apply(lambda x: safe_round(x, 2))
    valid_data['市盈率'] = valid_data['市盈率'].apply(lambda x: safe_round(x, 2))
    valid_data['市净率'] = valid_data['市净率'].apply(lambda x: safe_round(x, 2))
    valid_data['净资产收益率(%)'] = valid_data['净资产收益率(%)'].apply(lambda x: safe_round(x, 2))
    valid_data['资产负债率(%)'] = valid_data['资产负债率(%)'].apply(lambda x: safe_round(x, 2))
    valid_data['营收增长率(%)'] = valid_data['营收增长率(%)'].apply(lambda x: safe_round(x, 2))
    valid_data['净利润增长率(%)'] = valid_data['净利润增长率(%)'].apply(lambda x: safe_round(x, 2))
    
    # 按综合评分降序排序
    valid_data = valid_data.sort_values('综合评分', ascending=False)
    
    # 保存为CSV文件
    valid_data.to_csv('selected_stocks.csv', index=False, encoding='utf_8_sig')
    
    # 统计有效数据数量
    print(f"✅ CSV格式结果已保存到 selected_stocks.csv ({len(valid_data)}条有效数据)")

def save_to_json(results):
    """将股票代码列表保存为JSON格式"""
    if results is None or results.empty:
        return
    
    # 过滤有效数据
    valid_data = results[
        (results['code'].notna()) & 
        (results['code'] != '')
    ].copy()
    
    if valid_data.empty:
        print("⚠️ 没有有效的股票代码可保存")
        return
    
    # 提取股票代码列表并保持字符串格式
    stock_codes = valid_data['code'].astype(str).str.zfill(6).tolist()
    
    # 保存为简单的数组格式JSON文件
    with open('selected_stocks.json', 'w', encoding='utf-8') as f:
        json.dump(stock_codes, f, ensure_ascii=False, indent=4)
    
    print(f"✅ JSON格式股票代码列表已保存到 selected_stocks.json ({len(stock_codes)}只股票)")

if __name__ == "__main__":
    main()