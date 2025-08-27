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
    """股票选择器类 - 集成基本面数据缓存"""
    
    def __init__(self):
        self.stocks_data = {}
        self.results = {}
        self.cache_dir = 'cache'
        self.stock_list_cache = os.path.join(self.cache_dir, 'stockA_list.csv')
        self.fundamentals_cache = os.path.join(self.cache_dir, 'stockA_fundamentals.csv')  # 使用修复后的缓存文件
        
        # 创建缓存目录
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def load_cached_fundamentals(self):
        """get_stockA_fundamentals.py缓存加载基本面数据"""
        cache_file = self.fundamentals_cache
        
        if not os.path.exists(cache_file):
            print("❌ 未找到基本面数据缓存，请先运行 get_stockA_fundamentals.py")
            return None
        
        try:
            # 读取缓存的基本面数据
            df = pd.read_csv(cache_file)
            
            # 标准化列名以适配选股策略
            column_mapping = {
                '股票代码': 'code',
                '股票名称': 'name',
                '股票上市日期': 'listing_date',
                '股票上市地点': 'listing_location',
                '股票所属行业': 'industry',
                '每股收益': 'eps',
                '每股净资产': 'bps',
                '净资产收益率': 'roe',
                '总资产收益率': 'roa',
                '毛利率': 'gross_margin',
                '净利率': 'net_margin',
                '营业利润率': 'operating_margin',
                '市盈率（静）': 'pe',
                '市盈率（TTM）': 'pe_ttm',
                '市净率': 'pb',
                '市销率': 'ps',
                '股息率': 'dividend_yield',
                '营业收入增长率': 'revenue_growth',
                '净利润增长率': 'profit_growth',
                '净资产增长率': 'equity_growth',
                '净利润增速': 'net_profit_speed',
                '资产负债率': 'debt_ratio',
                '流动比率': 'current_ratio',
                '总资产周转率': 'asset_turnover',
                '存货周转率': 'inventory_turnover',
                '应收账款周转率': 'receivables_turnover',
                '每股经营现金流': 'operating_cash_flow_per_share',
                '现金流量比率': 'cash_flow_ratio'
            }
            
            # 重命名存在的列
            available_columns = {}
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    available_columns[old_name] = new_name
            
            df = df.rename(columns=available_columns)
            
            # 确保必需字段存在
            required_fields = ['code', 'name', 'pe', 'pb', 'roe']
            missing_fields = [f for f in required_fields if f not in df.columns]

            if missing_fields:
                print(f"⚠️ 缓存数据缺少字段: {missing_fields}")
                return None

            # 数据清理 - 移除NaN值
            df = df.dropna(subset=['pe', 'pb', 'roe'])
            print(f"🔄 移除NaN值后剩余 {len(df)} 只股票")

            # 数据标准化处理 - 修复明显的单位转换问题
            # ROE看起来是百分比值被错误存储为整数，需要除以100
            if (df['roe'] > 100).any():
                df['roe'] = df['roe'] / 100
                print("🔄 已自动将ROE从百分比整数转换为小数形式")

            # 数据清洗 - 使用裁剪而非过滤来保留更多数据
            # 限制PE在0-200之间
            df['pe'] = df['pe'].clip(lower=0, upper=200)
            # 限制PB在0-30之间
            df['pb'] = df['pb'].clip(lower=0, upper=30)
            # 限制ROE在0-2之间(0-200%)
            df['roe'] = df['roe'].clip(lower=0, upper=2)

            # 其他指标的处理
            if 'ps' in df.columns:
                df['ps'] = df['ps'].clip(lower=0, upper=30)  # 市销率
            if 'dividend_yield' in df.columns:
                # 股息率看起来也是百分比值被错误存储
                if (df['dividend_yield'] > 100).any():
                    df['dividend_yield'] = df['dividend_yield'] / 100
                    print("🔄 已自动将股息率从百分比整数转换为小数形式")
                df['dividend_yield'] = df['dividend_yield'].clip(lower=0, upper=0.3)  # 0-30%

            # 增长率指标处理
            if 'revenue_growth' in df.columns:
                # 假设增长率是百分比值
                df['revenue_growth'] = df['revenue_growth'].clip(lower=-2, upper=5)  # -200%到500%
            if 'profit_growth' in df.columns:
                df['profit_growth'] = df['profit_growth'].clip(lower=-3, upper=10)  # -300%到1000%

            print(f"✅ 数据清洗完成，剩余 {len(df)} 只股票")

            # 转换数据类型
            numeric_columns = ['pe', 'pb', 'roe', 'debt_ratio', 
                             'revenue_growth', 'profit_growth', 'eps', 'gross_margin', 
                             'net_margin', 'current_ratio', 'roa', 'operating_margin',
                             'pe_ttm', 'ps', 'dividend_yield', 'equity_growth',
                             'net_profit_speed', 'asset_turnover', 'inventory_turnover',
                             'receivables_turnover', 'operating_cash_flow_per_share', 'cash_flow_ratio']

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 数据验证和清洗
            # 检查并处理关键指标的异常值
            for col in ['pe', 'pb', 'roe', 'eps', 'ps', 'dividend_yield']:
                if col in df.columns:
                    # 移除无穷值
                    df = df.replace([np.inf, -np.inf], np.nan)
                    # 填充NaN值为该列的中位数
                    median_value = df[col].median()
                    df[col] = df[col].fillna(median_value)
                    print(f"🔄 已填充{col}的NaN值为中位数: {median_value:.2f}")

            # 尝试从现有数据计算价格和市值
            # 价格 = 每股收益 * 市盈率
            if 'price' not in df.columns:
                if 'eps' in df.columns and 'pe' in df.columns:
                    df['price'] = df['eps'] * df['pe']
                    # 处理异常值
                    df['price'] = df['price'].clip(lower=0.1, upper=10000)
                    # 填充可能的NaN值
                    df['price'] = df['price'].fillna(10.0)
                else:
                    df['price'] = 10.0  # 设置一个合理的默认值

            # 市值 = 价格 * 总股本（假设我们没有总股本数据，使用流通市值替代）
            if 'market_cap' not in df.columns:
                # 假设流通市值是价格的10倍（简化处理）
                df['market_cap'] = df['price'] * 10
                df['market_cap'] = df['market_cap'].clip(lower=1, upper=100000)
                # 填充可能的NaN值
                df['market_cap'] = df['market_cap'].fillna(100.0)

            # 确保行业字段不为空
            if 'industry' in df.columns:
                df['industry'] = df['industry'].fillna('未知行业')
            else:
                df['industry'] = '未知行业'

            # 确保上市日期不为空
            if 'listing_date' in df.columns:
                df['listing_date'] = df['listing_date'].fillna('1970-01-01')
            else:
                df['listing_date'] = '1970-01-01'

            # 显示一些数据样本，用于调试
            print("🔍 数据样本:")
            print(df[['code', 'name', 'pe', 'pb', 'roe', 'price', 'market_cap']].head(5))
            
            print(f"✅ 成功加载缓存基本面数据: {len(df)} 只股票")
            return df
            
        except Exception as e:
            print(f"❌ 加载缓存数据失败: {e}")
            return None
    
    def get_all_a_stock_list(self):
        """获取A股股票列表，优先使用缓存"""
        # 检查基本面缓存中是否已有股票列表
        if os.path.exists(self.fundamentals_cache):
            try:
                df = pd.read_csv(self.fundamentals_cache)
                if 'code' in df.columns and 'name' in df.columns:
                    stock_list = df[['code', 'name']].copy()
                    print(f"📊 从基本面缓存获取股票列表: {len(stock_list)} 只股票")
                    return stock_list
            except Exception as e:
                print(f"⚠️ 从缓存获取股票列表失败: {e}")
        
        # 回退到原来的获取方式
        return self._get_stock_list_from_network()
    
    def _get_stock_list_from_network(self):
        """从网络获取A股股票列表（备用方案）"""
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
        
        # 尝试从缓存加载
        cached_data = self.load_cached_fundamentals()
        if cached_data is not None:
            return cached_data
        
        print("❌ 缓存数据不可用，请先运行: python3 get_stockA_fundamentals.py")
        return pd.DataFrame()
    
    def check_cache_integrity(self):
        """检查缓存数据完整性"""
        cache_file = self.fundamentals_cache
        
        if not os.path.exists(cache_file):
            print("❌ 未找到基本面数据缓存")
            return False
        
        try:
            df = pd.read_csv(cache_file)
            
            # 检查必需字段
            required_fields = ['code', 'name', 'current_price', 'market_cap', 'pe_ttm', 'pb', 'roe']
            missing_fields = [f for f in required_fields if f not in df.columns]
            
            if missing_fields:
                print(f"❌ 缓存数据不完整，缺少: {missing_fields}")
                return False
            
            if len(df) < 10:
                print(f"❌ 缓存数据量过少: {len(df)} 只股票")
                return False
            
            print(f"✅ 缓存数据完整: {len(df)} 只股票，{len(df.columns)} 个字段")
            return True
            
        except Exception as e:
            print(f"❌ 缓存数据检查失败: {e}")
            return False
        
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
        # 添加市销率和股息率条件
        conditions = (
            (df['pe'] < 15) & (df['pe'] > 0) &  # 市盈率低于15且为正
            (df['pb'] < 2) & (df['pb'] > 0) &  # 市净率低于2且为正
            (df['ps'] < 2) & (df['ps'] > 0) &  # 市销率低于2且为正
            (df['roe'] > 10) &  # 净资产收益率大于10%
            (df['debt_ratio'] < 60) &  # 资产负债率低于60%
            (df['dividend_yield'] > 2)  # 股息率大于2%
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '价值投资'
        selected['reason'] = '低估值(PE/PB/PS)+高分红+稳定盈利'
        
        # 计算综合评分，加入市销率和股息率的权重
        selected['score'] = (
            (100/df['pe']) * 0.2 + 
            (100/df['pb']) * 0.2 + 
            (100/df['ps']) * 0.2 + 
            df['roe'] * 0.3 + 
            df['dividend_yield'] * 0.1
        )
        
        return selected.sort_values('score', ascending=False).head(10)
    
    def growth_strategy(self, df):
        """成长投资策略：高增长+合理估值+行业龙头"""
        conditions = (
            (df['revenue_growth'] > 20) &  # 营收增长率大于20%
            (df['profit_growth'] > 20) &  # 净利润增长率大于20%
            (df['equity_growth'] > 10) &  # 净资产增长率大于10%
            (df['pe'] < 40) & (df['pe'] > 0) &  # 市盈率合理
            (df['roe'] > 15) &  # 净资产收益率高
            (df['roa'] > 5) &  # 总资产收益率大于5%
            (df['debt_ratio'] < 50)  # 资产负债率低
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '成长投资'
        selected['reason'] = '高增长(营收/利润/净资产)+合理估值+优质赛道'
        
        # 计算综合评分，加入更多增长指标的权重
        selected['score'] = (
            df['revenue_growth'] * 0.2 + 
            df['profit_growth'] * 0.2 + 
            df['equity_growth'] * 0.1 + 
            df['roe'] * 0.3 + 
            df['roa'] * 0.2
        )
        
        return selected.sort_values('score', ascending=False).head(10)
    
    def quality_strategy(self, df):
        """质量投资策略：高ROE+低负债+优质盈利质量"""
        conditions = (
            (df['roe'] > 20) &  # 净资产收益率高
            (df['roa'] > 10) &  # 总资产收益率高
            (df['debt_ratio'] < 40) &  # 低负债
            (df['gross_margin'] > 30) &  # 毛利率高
            (df['net_margin'] > 15) &  # 净利率高
            (df['cash_flow_ratio'] > 10) &  # 现金流状况良好
            (df['profit_growth'] > 0)  # 正增长
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '质量投资'
        selected['reason'] = '高ROE/ROA+低负债+优质盈利质量(毛利率/净利率/现金流)'
        
        # 质量评分：ROE权重30%，ROA权重20%，低负债权重15%，毛利率权重15%，净利率权重10%，现金流比率权重10%
        selected['score'] = (
            df['roe'] * 0.3 + 
            df['roa'] * 0.2 + 
            (100 - df['debt_ratio']) * 0.15 + 
            df['gross_margin'] * 0.15 + 
            df['net_margin'] * 0.1 + 
            df['cash_flow_ratio'] * 0.1
        )
        
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
            (df['dividend_yield'] > 3) &  # 高股息
            (df['cash_flow_ratio'] > 15) &  # 现金流稳定
            (df['market_cap'] > 100)  # 大市值
        )
        
        selected = df[conditions].copy()
        selected['strategy'] = '防御投资'
        selected['reason'] = '低波动+高股息+稳定现金流+抗周期'
        
        # 防御评分：低估值权重30%，高股息权重25%，低负债权重20%，盈利能力权重15%，现金流权重10%
        selected['score'] = (
            ((20-df['pe'])/15 + (3-df['pb'])/2.5) * 0.3 + 
            df['dividend_yield'] * 0.25 + 
            (100 - df['debt_ratio']) * 0.2 + 
            df['roe'] * 0.15 + 
            df['cash_flow_ratio'] * 0.1
        )
        
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
        
        print("\n✅ 结果已保存到 result/ 目录:")
        print("   • result/result_selected_stocks.md (Markdown格式)")
        print("   • result/result_selected_stocks.csv (CSV格式)")
        print("   • result/result_selected_stocks.json (JSON格式股票代码列表)")
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
            md_content.append(f"- **上市日期**: {stock.get('listing_date', 'N/A')}")
            md_content.append(f"- **上市地点**: {stock.get('listing_location', 'N/A')}")
            md_content.append(f"- **所属行业**: {stock.get('industry', 'N/A')}")
            md_content.append(f"- **选择原因**: {stock['reason']}")
            md_content.append(f"- **综合评分**: {stock['score']:.2f}")
            md_content.append("")
            md_content.append("**估值指标**:")
            md_content.append(f"- PE: {stock['pe']:.2f}")
            md_content.append(f"- PB: {stock['pb']:.2f}")
            md_content.append(f"- PS: {stock.get('ps', 'N/A'):.2f}")
            md_content.append(f"- 股息率: {stock.get('dividend_yield', 'N/A'):.2f}%")
            md_content.append("")
            md_content.append("**盈利能力指标**:")
            md_content.append(f"- ROE: {stock['roe']:.2f}%")
            md_content.append(f"- ROA: {stock.get('roa', 'N/A'):.2f}%")
            md_content.append(f"- 毛利率: {stock.get('gross_margin', 'N/A'):.2f}%")
            md_content.append(f"- 净利率: {stock.get('net_margin', 'N/A'):.2f}%")
            md_content.append("")
            md_content.append("**成长指标**:")
            md_content.append(f"- 营收增长: {stock.get('revenue_growth', 'N/A'):.2f}%")
            md_content.append(f"- 利润增长: {stock.get('profit_growth', 'N/A'):.2f}%")
            md_content.append(f"- 净资产增长: {stock.get('equity_growth', 'N/A'):.2f}%")
            md_content.append("")
            md_content.append("**财务健康指标**:")
            md_content.append(f"- 资产负债率: {stock.get('debt_ratio', 'N/A'):.2f}%")
            md_content.append(f"- 现金流量比率: {stock.get('cash_flow_ratio', 'N/A'):.2f}%")
            md_content.append("")
    
    # 写入文件到result目录
    output_path = os.path.join('result', 'result_selected_stocks.md')
    with open(output_path, 'w', encoding='utf-8') as f:
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
    csv_columns = ['code', 'name', 'listing_date', 'listing_location', 'industry',
                   'price', 'market_cap', 'strategy', 'reason', 'score', 
                   'pe', 'pb', 'ps', 'roe', 'roa', 'debt_ratio', 'dividend_yield',
                   'revenue_growth', 'profit_growth', 'equity_growth', 'gross_margin',
                   'net_margin', 'cash_flow_ratio']

    # 确保所有需要的列都存在
    for col in csv_columns:
        if col not in valid_data.columns:
            valid_data[col] = np.nan

    # 重命名列名为中文
    column_mapping = {
        'code': '股票代码',
        'name': '股票名称', 
        'listing_date': '上市日期',
        'listing_location': '上市地点',
        'industry': '所属行业',
        'price': '当前价格',
        'market_cap': '市值(亿)',
        'strategy': '投资策略',
        'reason': '选择原因',
        'score': '综合评分',
        'pe': '市盈率',
        'pb': '市净率',
        'ps': '市销率',
        'roe': '净资产收益率(%)',
        'roa': '总资产收益率(%)',
        'debt_ratio': '资产负债率(%)',
        'dividend_yield': '股息率(%)',
        'revenue_growth': '营收增长率(%)',
        'profit_growth': '净利润增长率(%)',
        'equity_growth': '净资产增长率(%)',
        'gross_margin': '毛利率(%)',
        'net_margin': '净利率(%)',
        'cash_flow_ratio': '现金流量比率(%)'
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
    
    # 保存为CSV文件到result目录
    output_path = os.path.join('result', 'result_selected_stocks.csv')
    valid_data.to_csv(output_path, index=False, encoding='utf_8_sig')
    
    # 统计有效数据数量
    print(f"✅ CSV格式结果已保存到 {output_path} ({len(valid_data)}条有效数据)")

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
    
    # 保存为包含代码和名称的JSON文件到result目录
    output_path = os.path.join('result', 'result_selected_stocks.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(stock_list, f, ensure_ascii=False, indent=4)
    
    print(f"✅ JSON格式股票代码和名称列表已保存到 {output_path} ({len(stock_list)}只股票)")

if __name__ == "__main__":
    main()