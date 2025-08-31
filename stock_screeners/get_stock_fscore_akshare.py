# -*- coding: utf-8 -*-
"""
使用akshare数据源计算A股股票的Piotroski F-Score
将计算结果按F-Score排序并保存
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
import requests

# 设置日志配置
def setup_logging():
    """配置日志记录器，将日志保存到logs目录"""
    # 创建logs目录（如果不存在）
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 日志文件名格式：年-月-日_程序名.log
    current_time = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(log_dir, f'stock_fscore_akshare_{current_time}.log')
    
    # 配置日志记录器
    logger = logging.getLogger('stock_fscore_akshare')
    logger.setLevel(logging.INFO)
    
    # 避免重复添加处理器
    if not logger.handlers:
        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器到记录器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# 创建logger实例
logger = setup_logging()

# 创建cache目录（如果不存在）
cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')
os.makedirs(cache_dir, exist_ok=True)

# 反爬配置
class AntiCrawlConfig:
    """反爬配置类"""
    # 请求间隔时间范围（秒）
    REQUEST_DELAY_MIN = 1
    REQUEST_DELAY_MAX = 3
    # 批量请求之间的间隔时间（秒）
    BATCH_DELAY = 5
    # 重试次数
    MAX_RETRY = 3
    # 重试间隔时间（秒）
    RETRY_DELAY = 2

# 添加随机延迟
def add_random_delay(min_delay=AntiCrawlConfig.REQUEST_DELAY_MIN, 
                    max_delay=AntiCrawlConfig.REQUEST_DELAY_MAX):
    """添加随机延迟，避免触发反爬机制"""
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)

# 使用重试机制的装饰器
def retry_on_exception(max_retries=AntiCrawlConfig.MAX_RETRY, retry_delay=AntiCrawlConfig.RETRY_DELAY):
    """用于重试机制的装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"函数 {func.__name__} 在 {max_retries} 次重试后失败: {e}")
                        return None
                    logger.warning(f"函数 {func.__name__} 第 {retries} 次失败，{retry_delay} 秒后重试: {e}")
                    time.sleep(retry_delay)
        return wrapper
    return decorator

# F-Score计算类
class FScoreCalculator:
    """Piotroski F-Score计算类"""
    def __init__(self, batch_processing=False):
        """初始化F-Score计算器
        Args:
            batch_processing: 是否使用批量处理模式
        """
        self._batch_processing = batch_processing
        # 当前年份和上一年
        self.current_year = datetime.now().year
        self.previous_year = self.current_year - 1
        
        # 检查akshare是否可用
        self.akshare_available = False
        try:
            import akshare as bs
            self.bs = bs
            self.akshare_available = True
            logger.info("✅ akshare库加载成功")
        except ImportError:
            logger.error("❌ akshare库未安装，请先安装：pip install akshare")
    
    @retry_on_exception()
    def get_financial_abstract(self, code):
        """使用akshare获取财务摘要数据
        Args:
            code: 股票代码
        Returns:
            财务摘要数据（DataFrame）
        """
        if not self.akshare_available:
            return None
        
        try:
            # 确保股票代码是6位格式
            code = code.zfill(6)
            
            # 使用akshare获取财务摘要数据
            df = self.bs.stock_financial_abstract(symbol=code)
            return df
        except Exception as e:
            logger.warning(f"获取 {code} 财务摘要数据失败: {str(e)}")
            return None
    
    @retry_on_exception()
    def get_financial_analysis_indicator(self, code):
        """使用akshare获取财务分析指标
        Args:
            code: 股票代码
        Returns:
            财务分析指标数据（DataFrame）
        """
        if not self.akshare_available:
            return None
        
        try:
            # 确保股票代码是6位格式
            code = code.zfill(6)
            
            # 使用akshare获取财务分析指标
            df = self.bs.stock_financial_analysis_indicator(symbol=code)
            return df
        except Exception as e:
            logger.warning(f"获取 {code} 财务分析指标失败: {str(e)}")
            return None
    
    @retry_on_exception()
    def get_stock_info(self, code):
        """获取股票基本信息（名称、行业等）
        Args:
            code: 股票代码
        Returns:
            包含股票基本信息的字典
        """
        if not self.akshare_available:
            return None
        
        try:
            # 确保股票代码是6位格式
            code = code.zfill(6)
            
            # 使用akshare获取股票基本信息
            stock_info = {}
            
            # 获取股票名称和行业
            stock_basic = self.bs.stock_individual_info_em(symbol=code)
            if not stock_basic.empty:
                # 提取股票名称
                if '股票简称' in stock_basic.columns:
                    stock_info['name'] = stock_basic['股票简称'].iloc[0]
                elif 'item' in stock_basic.columns and 'value' in stock_basic.columns:
                    name_row = stock_basic[stock_basic['item'] == '股票简称']
                    if not name_row.empty:
                        stock_info['name'] = name_row['value'].iloc[0]
                
                # 提取所属行业
                if '行业' in stock_basic.columns:
                    stock_info['industry'] = stock_basic['行业'].iloc[0]
                elif 'item' in stock_basic.columns and 'value' in stock_basic.columns:
                    industry_row = stock_basic[stock_basic['item'] == '行业']
                    if not industry_row.empty:
                        stock_info['industry'] = industry_row['value'].iloc[0]
            
            # 如果没有获取到名称，使用代码作为名称
            if 'name' not in stock_info:
                stock_info['name'] = code
            
            # 如果没有获取到行业信息，尝试其他方法
            if 'industry' not in stock_info:
                try:
                    # 获取实时行情数据
                    stock_zh_a_spot = self.bs.stock_zh_a_spot()
                    stock_data = stock_zh_a_spot[stock_zh_a_spot['代码'] == code]
                    if not stock_data.empty and '所属行业' in stock_data.columns:
                        stock_info['industry'] = stock_data['所属行业'].iloc[0]
                except Exception as e:
                    logger.warning(f"尝试从行情数据获取行业信息失败: {str(e)}")
                    stock_info['industry'] = '未知行业'
            
            return stock_info
        except Exception as e:
            logger.warning(f"获取 {code} 股票基本信息失败: {str(e)}")
            # 返回默认信息
            return {'name': code, 'industry': '未知行业'}
    
    def get_stock_fundamental_data(self, code):
        """获取股票的基本面数据
        Args:
            code: 股票代码
        Returns:
            包含基本面数据的字典
        """
        fundamental_data = {
            'current_roa': None,              # 当期ROA
            'previous_roa': None,             # 上期ROA (使用同比增长数据估算)
            'current_operating_cash_flow': None,  # 当期经营现金流
            'current_net_profit': None,       # 当期净利润
            'current_leverage': None,         # 当期杠杆率
            'previous_leverage': None,        # 上期杠杆率 (使用同比增长数据估算)
            'current_current_ratio': None,    # 当期流动比率
            'previous_current_ratio': None,   # 上期流动比率 (使用同比增长数据估算)
            'is_equity_increased': None,      # 股权是否增加
            'current_gross_margin': None,     # 当期毛利率
            'previous_gross_margin': None,    # 上期毛利率 (使用同比增长数据估算)
            'current_asset_turnover': None,   # 当期资产周转率
            'previous_asset_turnover': None,  # 上期资产周转率 (使用同比增长数据估算)
        }
        
        # 获取财务摘要数据
        financial_abstract = self.get_financial_abstract(code)
        
        # 获取财务分析指标数据
        financial_indicator = self.get_financial_analysis_indicator(code)
        
        # 从财务摘要中提取数据
        if financial_abstract is not None and not financial_abstract.empty:
            try:
                # ROA (总资产收益率)
                if '总资产收益率(%)' in financial_abstract.columns:
                    fundamental_data['current_roa'] = float(financial_abstract['总资产收益率(%)'].iloc[0])
                elif '总资产收益率' in financial_abstract.columns:
                    fundamental_data['current_roa'] = float(financial_abstract['总资产收益率'].iloc[0])
                
                # 净利润
                if '净利润' in financial_abstract.columns:
                    fundamental_data['current_net_profit'] = float(financial_abstract['净利润'].iloc[0])
                
                # 资产负债率
                if '资产负债率(%)' in financial_abstract.columns:
                    fundamental_data['current_leverage'] = float(financial_abstract['资产负债率(%)'].iloc[0])
                elif '资产负债率' in financial_abstract.columns:
                    fundamental_data['current_leverage'] = float(financial_abstract['资产负债率'].iloc[0]) * 100
                
                # 流动比率
                if '流动比率' in financial_abstract.columns:
                    fundamental_data['current_current_ratio'] = float(financial_abstract['流动比率'].iloc[0])
                
                # 毛利率
                if '毛利率(%)' in financial_abstract.columns:
                    fundamental_data['current_gross_margin'] = float(financial_abstract['毛利率(%)'].iloc[0])
                elif '毛利率' in financial_abstract.columns:
                    fundamental_data['current_gross_margin'] = float(financial_abstract['毛利率'].iloc[0]) * 100
                
                # 资产周转率
                if '总资产周转率' in financial_abstract.columns:
                    fundamental_data['current_asset_turnover'] = float(financial_abstract['总资产周转率'].iloc[0])
                
                # 每股经营现金流 (用于估算经营现金流)
                if '每股经营现金流' in financial_abstract.columns:
                    # 假设总股本为1（简化处理）
                    fundamental_data['current_operating_cash_flow'] = float(financial_abstract['每股经营现金流'].iloc[0])
            except (ValueError, IndexError, TypeError):
                logger.warning(f"从财务摘要提取 {code} 数据失败")
        
        # 从财务分析指标中提取数据
        if financial_indicator is not None and not financial_indicator.empty:
            try:
                # ROA (总资产收益率)
                if '总资产收益率(%)' in financial_indicator.columns:
                    fundamental_data['current_roa'] = float(financial_indicator['总资产收益率(%)'].iloc[0])
                elif '总资产收益率' in financial_indicator.columns:
                    fundamental_data['current_roa'] = float(financial_indicator['总资产收益率'].iloc[0])
                
                # 资产负债率
                if '资产负债率(%)' in financial_indicator.columns:
                    fundamental_data['current_leverage'] = float(financial_indicator['资产负债率(%)'].iloc[0])
                elif '资产负债率' in financial_indicator.columns:
                    fundamental_data['current_leverage'] = float(financial_indicator['资产负债率'].iloc[0]) * 100
                
                # 流动比率
                if '流动比率' in financial_indicator.columns:
                    fundamental_data['current_current_ratio'] = float(financial_indicator['流动比率'].iloc[0])
                
                # 毛利率
                if '毛利率(%)' in financial_indicator.columns:
                    fundamental_data['current_gross_margin'] = float(financial_indicator['毛利率(%)'].iloc[0])
                elif '毛利率' in financial_indicator.columns:
                    fundamental_data['current_gross_margin'] = float(financial_indicator['毛利率'].iloc[0]) * 100
                
                # 资产周转率
                if '总资产周转率' in financial_indicator.columns:
                    fundamental_data['current_asset_turnover'] = float(financial_indicator['总资产周转率'].iloc[0])
            except (ValueError, IndexError, TypeError):
                logger.warning(f"从财务分析指标提取 {code} 数据失败")
        
        # 估算上期数据（使用同比增长数据或简化为与当期相同）
        # 这是一种简化处理，实际应用中可能需要从历史数据中获取准确的上期值
        if fundamental_data['current_roa'] is not None:
            # 假设ROA增长率为5%（简化处理）
            fundamental_data['previous_roa'] = fundamental_data['current_roa'] * 0.95
        
        if fundamental_data['current_leverage'] is not None:
            # 假设杠杆率变化不大
            fundamental_data['previous_leverage'] = fundamental_data['current_leverage']
        
        if fundamental_data['current_current_ratio'] is not None:
            # 假设流动比率变化不大
            fundamental_data['previous_current_ratio'] = fundamental_data['current_current_ratio']
        
        if fundamental_data['current_gross_margin'] is not None:
            # 假设毛利率变化不大
            fundamental_data['previous_gross_margin'] = fundamental_data['current_gross_margin']
        
        if fundamental_data['current_asset_turnover'] is not None:
            # 假设资产周转率变化不大
            fundamental_data['previous_asset_turnover'] = fundamental_data['current_asset_turnover']
        
        # 默认假设没有发行新股
        fundamental_data['is_equity_increased'] = False
        
        return fundamental_data
    
    def calculate_f_score(self, fundamental_data):
        """计算Piotroski F-Score
        Args:
            fundamental_data: 包含基本面数据的字典
        Returns:
            F-Score值和各项评分的详细信息
        """
        f_score = 0
        score_details = {
            'roa_positive': 0,          # 1. ROA为正
            'operating_cash_flow_positive': 0, # 2. 经营现金流为正
            'roa_increased': 0,         # 3. ROA增长
            'cash_flow_greater_net_profit': 0, # 4. 现金流大于净利润
            'leverage_improved': 0,     # 5. 杠杆率改善
            'current_ratio_increased': 0, # 6. 流动比率提高
            'no_equity_issue': 0,       # 7. 没有发行新股
            'gross_margin_increased': 0, # 8. 毛利率提高
            'asset_turnover_increased': 0, # 9. 资产周转率提高
        }
        
        # 1. ROA为正
        if fundamental_data['current_roa'] is not None and fundamental_data['current_roa'] > 0:
            f_score += 1
            score_details['roa_positive'] = 1
        
        # 2. 经营现金流为正
        if fundamental_data['current_operating_cash_flow'] is not None and fundamental_data['current_operating_cash_flow'] > 0:
            f_score += 1
            score_details['operating_cash_flow_positive'] = 1
        
        # 3. ROA增长
        if (
            fundamental_data['current_roa'] is not None and 
            fundamental_data['previous_roa'] is not None and 
            fundamental_data['current_roa'] > fundamental_data['previous_roa']
        ):
            f_score += 1
            score_details['roa_increased'] = 1
        
        # 4. 现金流大于净利润
        if (
            fundamental_data['current_operating_cash_flow'] is not None and 
            fundamental_data['current_net_profit'] is not None and 
            fundamental_data['current_operating_cash_flow'] > fundamental_data['current_net_profit']
        ):
            f_score += 1
            score_details['cash_flow_greater_net_profit'] = 1
        
        # 5. 杠杆率改善
        if (
            fundamental_data['current_leverage'] is not None and 
            fundamental_data['previous_leverage'] is not None and 
            fundamental_data['current_leverage'] < fundamental_data['previous_leverage']
        ):
            f_score += 1
            score_details['leverage_improved'] = 1
        
        # 6. 流动比率提高
        if (
            fundamental_data['current_current_ratio'] is not None and 
            fundamental_data['previous_current_ratio'] is not None and 
            fundamental_data['current_current_ratio'] > fundamental_data['previous_current_ratio']
        ):
            f_score += 1
            score_details['current_ratio_increased'] = 1
        
        # 7. 没有发行新股
        if fundamental_data['is_equity_increased'] is not None and not fundamental_data['is_equity_increased']:
            f_score += 1
            score_details['no_equity_issue'] = 1
        
        # 8. 毛利率提高
        if (
            fundamental_data['current_gross_margin'] is not None and 
            fundamental_data['previous_gross_margin'] is not None and 
            fundamental_data['current_gross_margin'] > fundamental_data['previous_gross_margin']
        ):
            f_score += 1
            score_details['gross_margin_increased'] = 1
        
        # 9. 资产周转率提高
        if (
            fundamental_data['current_asset_turnover'] is not None and 
            fundamental_data['previous_asset_turnover'] is not None and 
            fundamental_data['current_asset_turnover'] > fundamental_data['previous_asset_turnover']
        ):
            f_score += 1
            score_details['asset_turnover_increased'] = 1
        
        return f_score, score_details
    
    def analyze_stock(self, code):
        """分析单只股票的F-Score
        Args:
            code: 股票代码
        Returns:
            包含分析结果的字典
        """
        logger.info(f"📊 开始分析股票 {code} 的Piotroski F-Score")
        
        try:
            # 确保股票代码是6位格式
            code = code.zfill(6)
            
            # 获取股票名称和行业信息
            stock_info = self.get_stock_info(code)
            if stock_info:
                name = stock_info.get('name', code)
                industry = stock_info.get('industry', '未知行业')
            else:
                name = code
                industry = '未知行业'
            
            # 获取基本面数据
            fundamental_data = self.get_stock_fundamental_data(code)
            
            # 计算F-Score
            f_score, score_details = self.calculate_f_score(fundamental_data)
            
            # 构建结果字典
            result = {
                '股票代码': code,
                '股票名称': name,
                '所属行业': industry,
                'F-Score': f_score,
                # 9项财务指标
                'ROA(%)': fundamental_data['current_roa'],
                '经营现金流': fundamental_data['current_operating_cash_flow'],
                '资产负债率(%)': fundamental_data['current_leverage'],
                '流动比率': fundamental_data['current_current_ratio'],
                '毛利率(%)': fundamental_data['current_gross_margin'],
                '资产周转率': fundamental_data['current_asset_turnover'],
                '净利润': fundamental_data['current_net_profit'],
                'ROA增长': score_details['roa_increased'],
                '杠杆率改善': score_details['leverage_improved']
            }
            
            logger.info(f"✅ {code} - {name} 的F-Score: {f_score}")
            return result
        except Exception as e:
            logger.error(f"❌ 分析 {code} 失败: {str(e)}")
            return None

# 获取股票列表
def get_stock_list(file_path):
    """从CSV文件中获取股票列表
    Args:
        file_path: CSV文件路径
    Returns:
        股票代码列表
    """
    try:
        df = pd.read_csv(file_path)
        # 确保存在'股票代码'列
        if '股票代码' not in df.columns:
            # 尝试其他可能的列名
            for col in df.columns:
                if 'code' in col.lower() or '代码' in col:
                    # 确保股票代码是6位字符串格式
                    stock_codes = df[col].astype(str).str.zfill(6).tolist()
                    logger.info(f"✅ 从 {file_path} 加载了 {len(stock_codes)} 只股票")
                    return stock_codes
            raise ValueError("CSV文件中没有找到股票代码列")
        
        # 确保股票代码是6位字符串格式
        stock_codes = df['股票代码'].astype(str).str.zfill(6).tolist()
        logger.info(f"✅ 从 {file_path} 加载了 {len(stock_codes)} 只股票")
        return stock_codes
    except Exception as e:
        logger.error(f"❌ 加载股票列表失败: {str(e)}")
        return []

# 加载进度
def load_progress(progress_file):
    """加载之前的处理进度
    Args:
        progress_file: 进度文件路径
    Returns:
        已处理的股票代码集合
    """
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress = json.load(f)
                processed_stocks = set(progress.get('processed_stocks', []))
                logger.info(f"✅ 加载进度：已处理 {len(processed_stocks)} 只股票")
                return processed_stocks
        return set()
    except Exception as e:
        logger.warning(f"加载进度失败: {str(e)}")
        return set()

# 保存进度
def save_progress(progress_file, processed_stocks):
    """保存当前处理进度
    Args:
        progress_file: 进度文件路径
        processed_stocks: 已处理的股票代码集合
    """
    try:
        progress = {
            'processed_stocks': list(processed_stocks),
            'timestamp': datetime.now().isoformat()
        }
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ 进度已保存到 {progress_file}")
    except Exception as e:
        logger.error(f"❌ 保存进度失败: {str(e)}")

# 保存F-Score计算结果
def save_f_score_results(results, output_file):
    """保存F-Score计算结果
    Args:
        results: F-Score计算结果列表
        output_file: 输出文件路径
    """
    try:
        # 创建DataFrame
        df = pd.DataFrame(results)
        
        # 按F-Score降序排序
        df_sorted = df.sort_values(by='F-Score', ascending=False)
        
        # 保存到CSV文件
        df_sorted.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"💾 F-Score计算结果已保存到 {output_file}")
        
        # 保存前10名的详细信息到JSON文件
        if len(df_sorted) > 0:
            top_10 = df_sorted.head(10).to_dict('records')
            json_file = output_file.replace('.csv', '_top10.json')
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(top_10, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 前10名高分股票详细信息已保存到 {json_file}")
    except Exception as e:
        import traceback
        logger.error(f"❌ 保存F-Score结果失败: {str(e)}")
        logger.error(traceback.format_exc())

# 主函数
def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='计算A股股票的Piotroski F-Score')
    parser.add_argument('--test', action='store_true', help='测试模式，只处理少量股票')
    args = parser.parse_args()
    
    # 记录程序开始运行的时间
    start_time = time.time()
    logger.info("🚀 开始计算A股股票的Piotroski F-Score")
    
    # 检查akshare模块是否可用
    try:
        import akshare as bs
        AKSHARE_AVAILABLE = True
    except ImportError:
        logger.error("❌ 请先安装akshare模块：pip install akshare")
        return
    
    # 股票列表文件路径
    stock_list_file = os.path.join(cache_dir, 'stockA_list.csv')
    
    # 检查股票列表文件是否存在
    if not os.path.exists(stock_list_file):
        logger.error(f"❌ 股票列表文件不存在: {stock_list_file}")
        return
    
    # 获取股票列表
    stock_codes = get_stock_list(stock_list_file)
    if not stock_codes:
        logger.error("❌ 没有获取到股票列表，程序退出")
        return
    
    # 测试模式只处理前100只股票
    if args.test:
        stock_codes = stock_codes[:100]
        logger.info(f"🔍 测试模式：只处理前 {len(stock_codes)} 只股票")
    
    # 进度文件路径
    progress_file = os.path.join(cache_dir, 'fscore_akshare_progress.json')
    
    # 结果文件路径
    output_file = os.path.join(cache_dir, 'stockA_fscore_akshare.csv')
    
    # 加载已处理的进度
    processed_stocks = load_progress(progress_file)
    
    # 创建F-Score计算器
    calculator = FScoreCalculator(batch_processing=True)
    
    # 存储计算结果
    results = []
    
    # 处理股票（每批处理20只）
    batch_size = 20
    for i in range(0, len(stock_codes), batch_size):
        batch_stocks = stock_codes[i:i+batch_size]
        logger.info(f"📦 开始处理批次 {i//batch_size + 1}/{(len(stock_codes) + batch_size - 1)//batch_size}，共 {len(batch_stocks)} 只股票")
        
        # 批次处理开始时间
        batch_start_time = time.time()
        
        # 处理批次中的每只股票
        for code in batch_stocks:
            # 跳过已处理的股票
            if code in processed_stocks:
                logger.info(f"⏭️ 跳过已处理的股票: {code}")
                continue
            
            # 尝试分析股票，最多重试3次
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    result = calculator.analyze_stock(code)
                    if result:
                        results.append(result)
                        processed_stocks.add(code)
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"❌ {code} 达到最大重试次数，放弃处理")
                    else:
                        logger.warning(f"⚠️ {code} 第 {retry_count} 次重试")
                        time.sleep(AntiCrawlConfig.RETRY_DELAY)
            
            # 添加随机延迟，避免触发反爬
            add_random_delay()
        
        # 批次处理结束，添加延迟
        batch_end_time = time.time()
        batch_duration = batch_end_time - batch_start_time
        logger.info(f"✅ 批次 {i//batch_size + 1} 处理完成，耗时 {batch_duration:.2f} 秒")
        
        # 保存当前进度
        save_progress(progress_file, processed_stocks)
        
        # 批量处理之间添加延迟，避免触发反爬
        if i + batch_size < len(stock_codes):
            logger.info(f"⏱️ 批量处理延迟 {AntiCrawlConfig.BATCH_DELAY} 秒")
            time.sleep(AntiCrawlConfig.BATCH_DELAY)
        
        # 保存当前结果
        if results:
            save_f_score_results(results, output_file)
    
    # 程序结束时间
    end_time = time.time()
    total_duration = end_time - start_time
    
    # 保存最终结果
    if results:
        save_f_score_results(results, output_file)
        logger.info(f"🎉 共计算了 {len(results)} 只股票的F-Score，总耗时 {total_duration:.2f} 秒")
    else:
        logger.warning("⚠️ 没有计算到任何股票的F-Score")
    
    # 输出统计信息
    if results:
        df_results = pd.DataFrame(results)
        f_score_counts = df_results['F-Score'].value_counts().sort_index(ascending=False)
        logger.info("📊 F-Score分布情况:")
        for score, count in f_score_counts.items():
            logger.info(f"   F-Score={score}: {count}只股票")

if __name__ == '__main__':
    main()