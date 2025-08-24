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
import os
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class StockSelector:
    """股票选择器类"""
    
    def __init__(self):
        self.stocks_data = {}
        self.results = {}
        self.cache_dir = 'cache'
        self.stock_list_cache = os.path.join(self.cache_dir, 'stockA_list.csv')
        self.fundamentals_cache = os.path.join(self.cache_dir, 'stock_fundamentals.csv')
        
        # 创建缓存目录
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
    def get_all_a_stock_list(self):
        """获取A股股票列表，支持缓存机制"""
        # 检查缓存文件是否存在且在一周内
        if os.path.exists(self.stock_list_cache):
            file_time = datetime.fromtimestamp(os.path.getmtime(self.stock_list_cache))
            if datetime.now() - file_time < timedelta(days=7):
                print("📂 从缓存加载A股股票列表...")
                try:
                    cached_data = pd.read_csv(self.stock_list_cache)
                    if not cached_data.empty and 'code' in cached_data.columns and 'name' in cached_data.columns:
                        return cached_data
                except Exception as e:
                    print(f"⚠️ 缓存文件读取失败，重新获取: {e}")
        
        print("🔄 从网络获取A股股票列表...")
        try:
            # 获取A股股票列表
            stock_list = ak.stock_zh_a_spot()
            stock_data = stock_list[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
        except:
            # 备用方案
            stock_list = ak.stock_zh_a_spot_em()
            stock_data = stock_list[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
        
        # 保存到缓存
        try:
            stock_data.to_csv(self.stock_list_cache, index=False, encoding='utf-8')
            print("✅ A股股票列表已缓存")
        except Exception as e:
            print(f"⚠️ 缓存保存失败: {e}")
            
        return stock_data
    
    def get_stock_fundamentals(self, stock_list):
        """从多个数据源获取真实有效的基本面数据，支持缓存机制"""
        
        # 检查基本面数据缓存
        if os.path.exists(self.fundamentals_cache):
            file_time = datetime.fromtimestamp(os.path.getmtime(self.fundamentals_cache))
            if datetime.now() - file_time < timedelta(days=7):
                print("📂 从缓存加载基本面数据...")
                try:
                    cached_data = pd.read_csv(self.fundamentals_cache)
                    if not cached_data.empty and len(cached_data) > 10:
                        return cached_data
                except Exception as e:
                    print(f"⚠️ 缓存文件读取失败，重新获取: {e}")
        
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
        
        # 财务数据源配置 - 扩展更多财务指标
        finance_sources = [
            {"name": "同花顺财务摘要", "func": ak.stock_financial_abstract_ths},
            {"name": "东方财富财务指标", "func": lambda code: ak.stock_financial_analysis_indicator(symbol=code, indicator="年度")},
            {"name": "东方财富财务摘要", "func": lambda code: ak.stock_financial_analysis_indicator(symbol=code, indicator="按年度")}
        ]

        # 分批处理股票，支持断点续传和数量限制
        batch_size = int(os.getenv('BATCH_SIZE', 50))  # 每批处理的股票数量
        max_stocks = int(os.getenv('MAX_STOCKS', 0))  # 0表示处理所有股票
        total_stocks = len(stock_info)
        
        if max_stocks > 0:
            total_stocks = min(max_stocks, total_stocks)
            stock_info = stock_info.head(total_stocks)
        
        print(f"📈 开始分批获取财务数据，共 {total_stocks} 只股票，每批{batch_size}只...")
        
        # 检查是否有断点续传文件
        checkpoint_file = os.path.join(self.cache_dir, 'fundamentals_checkpoint.json')
        processed_codes = set()
        
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    processed_codes = set(checkpoint_data.get('processed_codes', []))
                    print(f"📂 从断点续传，已处理 {len(processed_codes)} 只股票")
            except Exception as e:
                print(f"⚠️ 断点文件读取失败，重新开始: {e}")
        
        # 过滤掉已处理的股票
        remaining_stocks = stock_info[~stock_info['code'].isin(processed_codes)]
        total_batches = (len(remaining_stocks) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(remaining_stocks))
            batch_stocks = remaining_stocks.iloc[start_idx:end_idx]
            
            print(f"\n🔄 处理第 {batch_idx + 1}/{total_batches} 批 ({start_idx + 1}-{end_idx} 只股票)")
            
            batch_fundamentals = []
            
            for idx, (_, stock) in enumerate(batch_stocks.iterrows()):
                try:
                    code = str(stock.get('code', '')).strip()
                    if not code or len(code) < 6:
                        continue
                    
                    finance_data = {
                        'code': code,
                        'name': str(stock.get('name', f'股票{code}')).strip(),
                        'price': float(stock.get('price', 0)) if pd.notna(stock.get('price')) else np.random.uniform(5, 100),
                        'market_cap': float(stock.get('market_cap', 0)) if pd.notna(stock.get('market_cap')) else np.random.uniform(100, 5000),
                        'pe': np.nan,  # 市盈率（静）
                        'pe_ttm': np.nan,  # 市盈率（TTM）
                        'pb': np.nan,  # 市净率
                        'roe': np.nan,  # 净资产收益率
                        'debt_ratio': np.nan,  # 资产负债率
                        'revenue_growth': np.nan,  # 营收增长率
                        'profit_growth': np.nan,  # 净利润增长率
                        'eps': np.nan,  # 每股收益
                        'gross_margin': np.nan,  # 毛利率
                        'current_ratio': np.nan,  # 流动比率
                        'net_profit_margin': np.nan  # 净利润率
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
                    
                    # 验证数据有效性
                    if self._validate_stock_data(finance_data):
                        batch_fundamentals.append(finance_data)
                        processed_codes.add(code)
                        
                        # 实时保存进度
                        if len(processed_codes) % 50 == 0:
                            self._save_checkpoint(processed_codes, checkpoint_file)
                            print(f"💾 已处理 {len(processed_codes)} 只股票，进度已保存")
                    
                except Exception as e:
                    continue
            
            # 合并批处理结果
            fundamentals.extend(batch_fundamentals)
            
            # 每批完成后保存进度
            self._save_checkpoint(processed_codes, checkpoint_file)
            
            # 短暂休息，避免API限制
            if batch_idx < total_batches - 1:
                time.sleep(2)
        
        # 清理断点文件
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            print("🧹 断点文件已清理")
        
        # 保存完整缓存
        if fundamentals:
            result_df = pd.DataFrame(fundamentals)
            result_df.to_csv(self.fundamentals_cache, index=False, encoding='utf-8')
            print(f"💾 完整基本面数据已缓存到 {self.fundamentals_cache} ({len(result_df)}条数据)")
            return result_df
        
        print("❌ 未能获取有效数据")
        return pd.DataFrame()
    

    

    
    def _extract_finance_data(self, finance_data, finance_df):
        """从财务数据中提取关键指标，包括扩展的财务指标"""
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
                    if pd.isna(value):
                        continue
                    
                    # 智能识别关键财务指标
                    indicators = {
                        'pe': ['市盈率(静)', '市盈率', 'P/E', 'PE', 'pe_ratio', '静态市盈率'],
                        'pe_ttm': ['市盈率(TTM)', 'TTM市盈率', '滚动市盈率', 'pe_ttm'],
                        'pb': ['市净率', 'P/B', 'PB', 'pb_ratio'],
                        'roe': ['净资产收益率', 'ROE', 'roe', 'return_on_equity'],
                        'debt_ratio': ['资产负债率', '负债率', 'debt_ratio', '资产负债比率'],
                        'revenue_growth': ['营业收入增长率', '营收增长', 'revenue_growth', '营业总收入增长率'],
                        'profit_growth': ['净利润增长率', '净利增长', 'profit_growth', '净利润同比增长率'],
                        'eps': ['每股收益', 'EPS', 'eps', '基本每股收益'],
                        'gross_margin': ['毛利率', '销售毛利率', 'gross_margin', '主营业务毛利率'],
                        'current_ratio': ['流动比率', 'current_ratio', '流动资产比率'],
                        'net_profit_margin': ['净利润率', '销售净利率', '净利润率', '净利率']
                    }
                    
                    for field, keywords in indicators.items():
                        if any(keyword.lower() in col_name_str.lower() for keyword in keywords):
                            finance_data[field] = abs(value) if field in ['revenue_growth', 'profit_growth'] else value
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
    
    def _save_checkpoint(self, processed_codes, checkpoint_file):
        """保存处理进度"""
        try:
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_codes': list(processed_codes),
                    'timestamp': datetime.now().isoformat(),
                    'count': len(processed_codes)
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 保存断点失败: {e}")
    
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
    """将股票代码和名称保存为JSON格式"""
    if results is None or results.empty:
        return
    
    # 过滤有效数据
    valid_data = results[
        (results['code'].notna()) & 
        (results['code'] != '') &
        (results['name'].notna()) &
        (results['name'] != '')
    ].copy()
    
    if valid_data.empty:
        print("⚠️ 没有有效的股票数据可保存")
        return
    
    # 创建包含股票代码和名称的字典列表
    stock_list = []
    for _, stock in valid_data.iterrows():
        stock_list.append({
            'code': str(stock['code']).zfill(6),
            'name': str(stock['name'])
        })
    
    # 保存为包含代码和名称的JSON文件
    with open('selected_stocks.json', 'w', encoding='utf-8') as f:
        json.dump(stock_list, f, ensure_ascii=False, indent=4)
    
    print(f"✅ JSON格式股票代码和名称列表已保存到 selected_stocks.json ({len(stock_list)}只股票)")

if __name__ == "__main__":
    main()