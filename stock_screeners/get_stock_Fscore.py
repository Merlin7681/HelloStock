import pandas as pd
import numpy as np
import json
import os
import time
from datetime import datetime
import random
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("stock_fscore_calculator.log"), logging.StreamHandler()])
logger = logging.getLogger('stock_fscore_calculator')

# 检查baostock可用性
try:
    import baostock as bs
    BAOSTOCK_AVAILABLE = True
    print("✅ 成功导入baostock库")
except ImportError:
    BAOSTOCK_AVAILABLE = False
    logger.warning("⚠️  baostock库未安装，运行 `pip install baostock` 以启用baostock数据源")

# 确保cache目录存在
if not os.path.exists('cache'):
    os.makedirs('cache')

# 反爬配置
class AntiCrawlConfig:
    # 请求延迟配置 (秒)
    MIN_DELAY = 0.5  # 最小延迟
    MAX_DELAY = 2.0  # 最大延迟
    
    # 批次处理延迟配置 (秒)
    BATCH_MIN_DELAY = 3.0  # 批次间最小延迟
    BATCH_MAX_DELAY = 5.0  # 批次间最大延迟
    
    # 单只股票处理间隔 (秒)
    STOCK_MIN_INTERVAL = 0.5  # 股票间最小间隔
    
    # 重试配置
    MAX_RETRIES = 3  # 最大重试次数

# 创建反爬配置实例
ANTI_CRAWL_CONFIG = AntiCrawlConfig()

# 添加随机延迟
def add_random_delay(min_delay=None, max_delay=None):
    min_delay = min_delay or AntiCrawlConfig.MIN_DELAY
    max_delay = max_delay or AntiCrawlConfig.MAX_DELAY
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)

# 数据访问控制类
class RateLimiter:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RateLimiter, cls).__new__(cls)
            cls._instance.reset()
        return cls._instance
    
    def reset(self):
        self.request_counts = {
            'baostock': 0
        }
        self.last_reset_times = {
            'baostock': time.time()
        }
        self.rate_limits = {
            'baostock': 100  # 每分钟最多请求数
        }
    
    def check_rate_limit(self, source):
        current_time = time.time()
        elapsed = current_time - self.last_reset_times[source]
        
        # 每分钟重置计数
        if elapsed >= 60:
            self.request_counts[source] = 0
            self.last_reset_times[source] = current_time
        
        # 检查是否超过限制
        if self.request_counts[source] >= self.rate_limits[source]:
            wait_time = 60 - elapsed + 1  # 等待到下一分钟再继续
            logger.warning(f"[{source}] 已达请求限制，等待 {wait_time:.1f} 秒")
            time.sleep(wait_time)
            self.request_counts[source] = 0
            self.last_reset_times[source] = time.time()
        
        # 增加计数
        self.request_counts[source] += 1

# 初始化访问控制器
rate_limiter = RateLimiter()

class FScoreCalculator:
    def __init__(self):
        self.current_year = datetime.now().year
        self.current_quarter = (datetime.now().month - 1) // 3 + 1
    
    def login_baostock(self):
        """登录baostock系统"""
        if not BAOSTOCK_AVAILABLE:
            return None
            
        try:
            # 检查访问频率限制
            rate_limiter.check_rate_limit('baostock')
            
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"❌ baostock登录失败: {lg.error_msg}")
                return None
            logger.info("✅ baostock登录成功")
            return lg
        except Exception as e:
            logger.error(f"❌ baostock登录异常: {str(e)}")
            return None
    
    def get_stock_fundamental_data(self, code):
        """获取股票的基本面数据，包括近两年的财务指标和行业信息"""
        if not BAOSTOCK_AVAILABLE:
            logger.error("⚠️  baostock库不可用，无法获取数据")
            return None
        
        # 登录baostock
        login_result = self.login_baostock()
        if not login_result:
            return None
        
        try:
            # 转换股票代码格式
            market_code = f"sz.{code}" if str(code).startswith(('0', '3')) else f"sh.{code}"
            
            # 初始化数据字典
            fundamental_data = {
                'stock_code': code,
                'stock_name': '',
                'industry': '',  # 股票所属行业
                # 当前年度数据
                'current_roa': None,
                'current_operating_cash_flow': None,
                'current_net_profit': None,
                'current_leverage': None,  # 当前资产负债率
                'current_current_ratio': None,  # 当前流动比率
                'current_gross_margin': None,  # 当前毛利率
                'current_asset_turnover': None,  # 当前资产周转率
                # 上一年度数据
                'previous_roa': None,
                'previous_leverage': None,
                'previous_current_ratio': None,
                'previous_gross_margin': None,
                'previous_asset_turnover': None,
                # 股票发行信息
                'has_new_equity': False
            }
            
            # 获取公司基本信息
            rs_stock_basic = bs.query_stock_basic(code=market_code)
            if rs_stock_basic.error_code == '0':
                basic_data = []
                while rs_stock_basic.error_code == '0' and rs_stock_basic.next():
                    basic_data.append(rs_stock_basic.get_row_data())
                if basic_data:
                    data = basic_data[0]
                    fundamental_data['stock_name'] = str(data[1]) if len(data) > 1 else ''
                    fundamental_data['industry'] = str(data[7]) if len(data) > 7 else ''
            
            # 添加随机延迟
            add_random_delay()
            
            # 尝试获取当前和上一年的数据
            for year_offset in range(0, 2):
                year = self.current_year - year_offset
                if year < 2007:  # baostock从2007年开始提供数据
                    break
                
                # 获取年度盈利能力数据
                rs_profit = bs.query_profit_data(code=market_code, year=year, quarter=4)  # 第4季度代表全年数据
                if rs_profit.error_code == '0':
                    profit_data = []
                    while rs_profit.error_code == '0' and rs_profit.next():
                        profit_data.append(rs_profit.get_row_data())
                    
                    if profit_data:
                        data = profit_data[0]
                        if year_offset == 0:  # 当前年度
                            # 总资产收益率 = 净利润/总资产平均余额
                            if len(data) > 4:  # 假设第5列是总资产收益率
                                fundamental_data['current_roa'] = float(data[4]) if data[4] else None
                            # 净利润
                            if len(data) > 5:  # 假设第6列是净利率，需要结合其他数据计算净利润
                                fundamental_data['current_net_profit'] = float(data[5]) if data[5] else None
                            # 当前毛利率
                            if len(data) > 7:  # 假设第8列是毛利率
                                fundamental_data['current_gross_margin'] = float(data[7]) if data[7] else None
                        else:  # 上一年度
                            if len(data) > 4:
                                fundamental_data['previous_roa'] = float(data[4]) if data[4] else None
                            # 毛利率 = (营业收入-营业成本)/营业收入
                            if len(data) > 7:  # 假设第8列是毛利率
                                fundamental_data['previous_gross_margin'] = float(data[7]) if data[7] else None
                
                # 添加随机延迟
                add_random_delay()
                
                # 获取年度资产负债表数据
                rs_balance = bs.query_balance_data(code=market_code, year=year, quarter=4)
                if rs_balance.error_code == '0':
                    balance_data = []
                    while rs_balance.error_code == '0' and rs_balance.next():
                        balance_data.append(rs_balance.get_row_data())
                    
                    if balance_data:
                        data = balance_data[0]
                        if year_offset == 0:  # 当前年度
                            # 资产负债率 = 总负债/总资产
                            if len(data) > 13:  # 假设第14列是资产负债率
                                fundamental_data['current_leverage'] = float(data[13]) if data[13] else None
                            # 流动比率 = 流动资产/流动负债
                            if len(data) > 14:  # 假设第15列是流动比率
                                fundamental_data['current_current_ratio'] = float(data[14]) if data[14] else None
                        else:  # 上一年度
                            # 资产负债率 = 总负债/总资产
                            if len(data) > 13:  # 假设第14列是资产负债率
                                fundamental_data['previous_leverage'] = float(data[13]) if data[13] else None
                            # 流动比率 = 流动资产/流动负债
                            if len(data) > 14:  # 假设第15列是流动比率
                                fundamental_data['previous_current_ratio'] = float(data[14]) if data[14] else None
                
                # 添加随机延迟
                add_random_delay()
                
                # 获取年度现金流量表数据
                rs_cash = bs.query_cash_flow_data(code=market_code, year=year, quarter=4)
                if rs_cash.error_code == '0':
                    cash_data = []
                    while rs_cash.error_code == '0' and rs_cash.next():
                        cash_data.append(rs_cash.get_row_data())
                    
                    if cash_data:
                        data = cash_data[0]
                        if year_offset == 0:  # 仅获取当前年度的经营现金流
                            # 每股经营现金流
                            if len(data) > 24:  # 假设第25列是每股经营现金流
                                fundamental_data['current_operating_cash_flow'] = float(data[24]) if data[24] else None
                
                # 添加随机延迟
                add_random_delay()
                
                # 获取资产周转率数据（使用年度营业收入和总资产计算）
                rs_kdata = bs.query_history_k_data_plus(market_code, 
                                                      "totalAssets,operatingRevenue",
                                                      start_date=f"{year}-01-01", 
                                                      end_date=f"{year}-12-31", 
                                                      frequency="d", 
                                                      adjustflag="3")
                
                if rs_kdata.error_code == '0':
                    kdata_list = []
                    while rs_kdata.error_code == '0' and rs_kdata.next():
                        kdata_list.append(rs_kdata.get_row_data())
                    
                    if kdata_list and len(kdata_list[-1]) > 1:
                        total_assets = float(kdata_list[-1][0]) if kdata_list[-1][0] else 0
                        operating_revenue = float(kdata_list[-1][1]) if kdata_list[-1][1] else 0
                        if total_assets > 0:
                            asset_turnover = operating_revenue / total_assets
                            if year_offset == 0:
                                fundamental_data['current_asset_turnover'] = asset_turnover
                            else:
                                fundamental_data['previous_asset_turnover'] = asset_turnover
                
                # 添加随机延迟
                add_random_delay()
                
                # 获取股票发行信息来判断是否有新股发行
                # 这里使用简化方法，实际可能需要更复杂的查询
                if year_offset == 0:  # 当前年度
                    # 简化判断：如果市值有显著增加，则可能有新股发行
                    # 实际应用中应该查询具体的股权变动信息
                    pass
            
            # 计算各项指标的变化
            # 杠杆率变化：如果当前杠杆率小于上一年，则为改善
            if (fundamental_data['current_leverage'] is not None and 
                fundamental_data['previous_leverage'] is not None):
                fundamental_data['leverage_improved'] = fundamental_data['current_leverage'] < fundamental_data['previous_leverage']
            
            # 流动比率变化
            if (fundamental_data['current_current_ratio'] is not None and 
                fundamental_data['previous_current_ratio'] is not None):
                fundamental_data['current_ratio_improved'] = fundamental_data['current_current_ratio'] > fundamental_data['previous_current_ratio']
            
            # 毛利率变化
            if (fundamental_data['current_gross_margin'] is not None and 
                fundamental_data['previous_gross_margin'] is not None):
                fundamental_data['gross_margin_improved'] = fundamental_data['current_gross_margin'] > fundamental_data['previous_gross_margin']
            
            # 资产周转率变化
            if (fundamental_data['current_asset_turnover'] is not None and 
                fundamental_data['previous_asset_turnover'] is not None):
                fundamental_data['asset_turnover_improved'] = fundamental_data['current_asset_turnover'] > fundamental_data['previous_asset_turnover']
            
            bs.logout()
            return fundamental_data
            
        except Exception as e:
            logger.error(f"❌ 获取 {code} 数据失败: {str(e)}")
            if BAOSTOCK_AVAILABLE:
                try:
                    bs.logout()
                except:
                    pass
            return None
    
    def get_stock_fundamental_data(self, code):
        """获取股票的基本面数据，包括近两年的财务指标"""
        if not BAOSTOCK_AVAILABLE:
            logger.error("⚠️  baostock库不可用，无法获取数据")
            return None
        
        # 检查访问频率限制
        rate_limiter.check_rate_limit('baostock')
        
        # 登录baostock
        login_result = self.login_baostock()
        if not login_result:
            return None
        
        try:
            # 转换股票代码格式
            market_code = f"sz.{code}" if str(code).startswith(('0', '3')) else f"sh.{code}"
            
            # 初始化数据字典
            fundamental_data = {
                'stock_code': code,
                'stock_name': '',
                'industry': '',
                # 当前年度数据
                'current_roa': None,
                'current_operating_cash_flow': None,
                'current_net_profit': None,
                'current_leverage': None,
                'current_current_ratio': None,
                'current_gross_margin': None,
                # 上一年度数据
                'previous_roa': None,
                'previous_leverage': None,
                'previous_current_ratio': None,
                'previous_gross_margin': None,
                'previous_asset_turnover': None,
                # 股票发行信息
                'has_new_equity': False
            }
            
            # 获取公司基本信息
            rs_stock_basic = bs.query_stock_basic(code=market_code)
            if rs_stock_basic.error_code == '0':
                basic_data = []
                while rs_stock_basic.error_code == '0' and rs_stock_basic.next():
                    basic_data.append(rs_stock_basic.get_row_data())
                if basic_data:
                    data = basic_data[0]
                    fundamental_data['stock_name'] = str(data[1]) if len(data) > 1 else ''
                    fundamental_data['industry'] = str(data[7]) if len(data) > 7 else ''
            
            # 添加随机延迟
            add_random_delay()
            
            # 尝试获取当前和上一年的数据
            for year_offset in range(0, 2):
                year = self.current_year - year_offset
                if year < 2007:  # baostock从2007年开始提供数据
                    break
                
                # 获取年度盈利能力数据
                rs_profit = bs.query_profit_data(code=market_code, year=year, quarter=4)  # 第4季度代表全年数据
                if rs_profit.error_code == '0':
                    profit_data = []
                    while rs_profit.error_code == '0' and rs_profit.next():
                        profit_data.append(rs_profit.get_row_data())
                    
                    if profit_data:
                        data = profit_data[0]
                        if year_offset == 0:  # 当前年度
                            # 总资产收益率
                            if len(data) > 4:  # 假设第5列是总资产收益率
                                fundamental_data['current_roa'] = float(data[4]) if data[4] else None
                            # 净利润
                            if len(data) > 5:  # 假设第6列是净利率
                                fundamental_data['current_net_profit'] = float(data[5]) if data[5] else None
                            # 毛利率
                            if len(data) > 7:  # 假设第8列是毛利率
                                fundamental_data['current_gross_margin'] = float(data[7]) if data[7] else None
                        else:  # 上一年度
                            if len(data) > 4:
                                fundamental_data['previous_roa'] = float(data[4]) if data[4] else None
                            if len(data) > 7:
                                fundamental_data['previous_gross_margin'] = float(data[7]) if data[7] else None
                
                # 添加随机延迟
                add_random_delay()
                
                # 获取年度资产负债表数据
                rs_balance = bs.query_balance_data(code=market_code, year=year, quarter=4)
                if rs_balance.error_code == '0':
                    balance_data = []
                    while rs_balance.error_code == '0' and rs_balance.next():
                        balance_data.append(rs_balance.get_row_data())
                    
                    if balance_data:
                        data = balance_data[0]
                        if year_offset == 0:  # 当前年度
                            # 资产负债率
                            if len(data) > 13:  # 假设第14列是资产负债率
                                fundamental_data['current_leverage'] = float(data[13]) if data[13] else None
                            # 流动比率
                            if len(data) > 14:  # 假设第15列是流动比率
                                fundamental_data['current_current_ratio'] = float(data[14]) if data[14] else None
                        else:  # 上一年度
                            if len(data) > 13:
                                fundamental_data['previous_leverage'] = float(data[13]) if data[13] else None
                            if len(data) > 14:
                                fundamental_data['previous_current_ratio'] = float(data[14]) if data[14] else None
                
                # 添加随机延迟
                add_random_delay()
                
                # 获取年度现金流量表数据
                rs_cash = bs.query_cash_flow_data(code=market_code, year=year, quarter=4)
                if rs_cash.error_code == '0':
                    cash_data = []
                    while rs_cash.error_code == '0' and rs_cash.next():
                        cash_data.append(rs_cash.get_row_data())
                    
                    if cash_data:
                        data = cash_data[0]
                        if year_offset == 0:  # 仅获取当前年度的经营现金流
                            # 每股经营现金流
                            if len(data) > 24:  # 假设第25列是每股经营现金流
                                fundamental_data['current_operating_cash_flow'] = float(data[24]) if data[24] else None
                
                # 添加随机延迟
                add_random_delay()
            
            bs.logout()
            return fundamental_data
            
        except Exception as e:
            logger.error(f"❌ 获取 {code} 数据失败: {str(e)}")
            if BAOSTOCK_AVAILABLE:
                try:
                    bs.logout()
                except:
                    pass
            return None
    
    def calculate_f_score(self, fundamental_data):
        """根据基本面数据计算Piotroski F-Score"""
        if not fundamental_data:
            logger.error("❌ 没有可用的基本面数据，无法计算F-Score")
            return None
        
        f_score = 0
        score_details = {
            'roa_positive': 0,             # 资产收益率为正
            'operating_cash_flow_positive': 0,  # 经营现金流为正
            'roa_growth': 0,               # 资产收益率增长
            'cash_flow_gt_net_profit': 0,  # 经营现金流大于净利润
            'leverage_improved': 0,        # 杠杆率降低
            'current_ratio_improved': 0,   # 流动比率提高
            'no_new_equity': 1,            # 未发行新股（默认为1）
            'gross_margin_improved': 0,    # 毛利率提高
            'asset_turnover_improved': 0   # 资产周转率提高
        }
        
        # 1. 盈利能力：资产收益率为正（ROA > 0）
        if fundamental_data.get('current_roa') and fundamental_data['current_roa'] > 0:
            f_score += 1
            score_details['roa_positive'] = 1
        
        # 2. 现金流：经营活动现金流为正
        if fundamental_data.get('current_operating_cash_flow') and fundamental_data['current_operating_cash_flow'] > 0:
            f_score += 1
            score_details['operating_cash_flow_positive'] = 1
        
        # 3. 盈利能力变化：资产收益率增长
        if (fundamental_data.get('current_roa') and fundamental_data.get('previous_roa') and 
            fundamental_data['current_roa'] > fundamental_data['previous_roa']):
            f_score += 1
            score_details['roa_growth'] = 1
        
        # 4. 应计项目：经营活动现金流 > 净利润
        if (fundamental_data.get('current_operating_cash_flow') and fundamental_data.get('current_net_profit') and 
            fundamental_data['current_operating_cash_flow'] > fundamental_data['current_net_profit']):
            f_score += 1
            score_details['cash_flow_gt_net_profit'] = 1
        
        # 5. 杠杆率变化：使用预计算的杠杆率改善指标
        if fundamental_data.get('leverage_improved', False):
            f_score += 1
            score_details['leverage_improved'] = 1
        
        # 6. 流动比率变化：使用预计算的流动比率改善指标
        if fundamental_data.get('current_ratio_improved', False):
            f_score += 1
            score_details['current_ratio_improved'] = 1
        
        # 7. 股票发行：没有发行新股（默认为1）
        if not fundamental_data.get('has_new_equity', False):
            f_score += 1
            score_details['no_new_equity'] = 1
        else:
            score_details['no_new_equity'] = 0
        
        # 8. 毛利率变化：使用预计算的毛利率改善指标
        if fundamental_data.get('gross_margin_improved', False):
            f_score += 1
            score_details['gross_margin_improved'] = 1
        
        # 9. 资产周转率变化：使用预计算的资产周转率改善指标
        if fundamental_data.get('asset_turnover_improved', False):
            f_score += 1
            score_details['asset_turnover_improved'] = 1
        
        result = {
            'stock_code': fundamental_data.get('stock_code', ''),
            'stock_name': fundamental_data.get('stock_name', ''),
            'industry': fundamental_data.get('industry', ''),
            'f_score': f_score,
            'score_details': score_details,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return result
    
    def analyze_stock(self, code):
        """分析指定股票的F-Score，支持行业信息提取"""
        logger.info(f"📊 开始分析股票 {code} 的Piotroski F-Score")
        
        # 获取基本面数据
        fundamental_data = self.get_stock_fundamental_data(code)
        if not fundamental_data:
            logger.error(f"❌ 无法获取 {code} 的基本面数据")
            return None
        
        # 计算F-Score
        f_score_result = self.calculate_f_score(fundamental_data)
        
        if f_score_result:
            # 确保行业信息被正确提取和集成
            if 'industry' not in f_score_result or not f_score_result['industry']:
                f_score_result['industry'] = fundamental_data.get('industry', '')
                
            # 包含9项财务指标的原始数据用于更全面的分析
            f_score_result.update({
                'current_roa': fundamental_data.get('current_roa', None),
                'current_operating_cash_flow': fundamental_data.get('current_operating_cash_flow', None),
                'current_leverage': fundamental_data.get('current_leverage', None),
                'current_current_ratio': fundamental_data.get('current_current_ratio', None),
                'current_gross_margin': fundamental_data.get('current_gross_margin', None),
                'current_asset_turnover': fundamental_data.get('current_asset_turnover', None),
                'previous_roa': fundamental_data.get('previous_roa', None),
                'previous_leverage': fundamental_data.get('previous_leverage', None),
                'previous_current_ratio': fundamental_data.get('previous_current_ratio', None),
                'previous_gross_margin': fundamental_data.get('previous_gross_margin', None),
                'previous_asset_turnover': fundamental_data.get('previous_asset_turnover', None)
            })
        
        return f_score_result

# 辅助函数
def get_stock_list():
    """从本地文件读取股票列表"""
    try:
        stock_list = pd.read_csv('cache/stockA_list.csv')
        logger.info(f"✅ 成功读取股票列表，共{len(stock_list)}只股票")
        return stock_list
    except Exception as e:
        logger.error(f"❌ 读取股票列表失败: {e}")
        return None

def load_progress():
    """加载进度信息"""
    progress_file = 'cache/fscore_progress.json'
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {"last_index": 0, "completed_codes": []}

def save_progress(index, completed_codes):
    """保存进度信息"""
    progress_file = 'cache/fscore_progress.json'
    try:
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump({"last_index": index, "completed_codes": completed_codes}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"⚠️  保存进度失败: {e}")

def save_f_score_results(results):
    """保存F-Score计算结果到CSV文件"""
    try:
        # 准备要保存的数据
        data_to_save = []
        for result in results:
            # 提取所需字段
            row = {
                '股票名称': result['stock_name'],
                '股票代码': result['stock_code'],
                '股票所属行业': result['industry'],
                'F-Score值': result['f_score']
            }
            
            # 添加9项指标的详细信息
            row.update({
                '资产收益率为正': result['score_details']['positive_roa'],
                '经营现金流为正': result['score_details']['positive_operating_cash_flow'],
                '资产收益率增长': result['score_details']['roa_improved'],
                '经营现金流大于净利润': result['score_details']['accruals'],
                '杠杆率降低': result['score_details']['leverage_improved'],
                '流动比率提高': result['score_details']['current_ratio_improved'],
                '未发行新股': result['score_details']['no_new_equity'],
                '毛利率提高': result['score_details']['gross_margin_improved'],
                '资产周转率提高': result['score_details']['asset_turnover_improved']
            })
            
            data_to_save.append(row)
        
        # 创建DataFrame
        df = pd.DataFrame(data_to_save)
        
        # 按F-Score值排序
        df_sorted = df.sort_values(by='F-Score值', ascending=False)
        
        # 保存到CSV文件
        csv_path = 'cache/stockA__PiotroskiFscore.csv'
        df_sorted.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"💾 F-Score计算结果已保存到 {csv_path}")
        
        # 同时保存前10个高分股票的详细信息到JSON文件
        top_10_results = results[:10] if len(results) > 10 else results
        json_path = 'cache/stockA_fscore_top10.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(top_10_results, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        logger.error(f"❌ 保存F-Score结果失败: {e}")
        return False

# 主函数
def main():
    """主函数：批量计算A股股票的Piotroski F-Score"""
    logger.info("🚀 开始批量计算A股股票的Piotroski F-Score...")
    
    # 显示可用数据源状态
    logger.info("📡 数据源状态:")
    logger.info(f"   baostock: {'✅ 可用' if BAOSTOCK_AVAILABLE else '❌ 未安装'}")
    
    if not BAOSTOCK_AVAILABLE:
        logger.error("❌ baostock库未安装，无法获取数据，请运行 pip install baostock 后重试")
        return
    
    # 获取股票列表
    stock_list = get_stock_list()
    if stock_list is None:
        return
    
    stock_codes = stock_list['code'].astype(str).str.zfill(6).tolist()
    total_stocks = len(stock_codes)
    
    # 加载进度
    progress = load_progress()
    start_index = progress["last_index"]
    completed_codes = set(progress["completed_codes"])
    
    logger.info(f"📊 共{total_stocks}只股票，从第{start_index+1}只开始计算...")
    logger.info(f"✅ 已完成{len(completed_codes)}只股票")
    
    if start_index >= total_stocks:
        logger.info("🎉 所有股票的F-Score已计算完成！")
        return
    
    # 获取待处理的股票
    remaining_codes = [code for code in stock_codes[start_index:] if code not in completed_codes]
    
    if not remaining_codes:
        logger.info("🎉 没有需要计算的股票数据")
        return
    
    # 分批处理股票
    batch_size = 20  # 每批处理20只股票
    all_results = []
    success_count = len(completed_codes)
    
    try:
        # 创建计算器实例
        calculator = FScoreCalculator()
        
        for i in range(0, len(remaining_codes), batch_size):
            batch_codes = remaining_codes[i:i+batch_size]
            batch_results = []
            
            current_start = start_index + i + 1
            current_end = min(start_index + i + batch_size, total_stocks)
            logger.info(f"\n🔄 处理第{current_start}-{current_end}只股票...")
            
            # 检查当前批次的总体访问频率
            if i > 0:  # 不是第一批
                # 根据配置添加批次间延迟
                batch_delay = random.uniform(ANTI_CRAWL_CONFIG.BATCH_MIN_DELAY, ANTI_CRAWL_CONFIG.BATCH_MAX_DELAY)
                logger.info(f"   ⏱️  批次间延迟: {batch_delay:.2f}秒")
                time.sleep(batch_delay)
            
            for j, code in enumerate(batch_codes):
                # 重试机制
                retry_count = 0
                f_score_result = None
                
                while retry_count < AntiCrawlConfig.MAX_RETRIES:
                    f_score_result = calculator.analyze_stock(code)
                    if f_score_result:
                        break
                    retry_count += 1
                    logger.warning(f"   🔄 重试计算 {code} F-Score ({retry_count}/{AntiCrawlConfig.MAX_RETRIES})")
                    time.sleep(random.uniform(2, 5))  # 重试前增加较长延迟
                
                if f_score_result:
                    batch_results.append(f_score_result)
                    all_results.append(f_score_result)
                    success_count += 1
                    completed_codes.add(code)
                    logger.info(f"   ✅ {code} - {f_score_result['stock_name']} 的F-Score: {f_score_result['f_score']}")
                else:
                    logger.warning(f"   ⚠️  {code} 数据获取失败或不完整，无法计算F-Score")
                
                # 间隔时间避免请求过快
                time.sleep(ANTI_CRAWL_CONFIG.STOCK_MIN_INTERVAL)
            
            # 保存进度
            if batch_results:
                current_index = start_index + i + len(batch_codes)
                save_progress(current_index, list(completed_codes))
                logger.info(f"   💾 进度已保存 ({len(batch_results)}条记录)")
            else:
                logger.warning(f"   ⚠️  本批次无有效计算结果")
        
        # 保存最终结果
        if all_results:
            # 按F-Score排序
            all_results.sort(key=lambda x: x['f_score'], reverse=True)
            save_f_score_results(all_results)
            
            logger.info(f"\n🎉 F-Score计算完成！")
            logger.info(f"📊 成功计算 {success_count}/{total_stocks} 只股票的F-Score")
            logger.info(f"📁 结果已保存到 cache/stockA_PiotroskiFscore.csv")
            
            # 显示统计信息
            if len(all_results) > 0:
                # 统计F-Score分布
                f_scores = [result['f_score'] for result in all_results]
                f_score_counts = pd.Series(f_scores).value_counts().sort_index(ascending=False)
                logger.info(f"\n📊 F-Score分布统计:")
                for score, count in f_score_counts.items():
                    logger.info(f"   F-Score {score}: {count}只股票 ({count/len(all_results)*100:.1f}%)")
                
                # 显示前5个高分股票
                logger.info(f"\n🏆 前5名高分股票:")
                for i, result in enumerate(all_results[:5]):
                    logger.info(f"   {i+1}. {result['stock_code']} - {result['stock_name']} (行业: {result['industry']}) - F-Score: {result['f_score']}")
        
    except KeyboardInterrupt:
        logger.warning(f"\n⚠️  用户中断，进度已保存")
        logger.warning(f"   已完成 {success_count} 只股票的F-Score计算")
        save_progress(success_count, list(completed_codes))
        if all_results:
            all_results.sort(key=lambda x: x['f_score'], reverse=True)
            save_f_score_results(all_results)
    
    except Exception as e:
        logger.error(f"\n❌ 程序异常: {e}")
        logger.error(f"   已完成 {success_count} 只股票的F-Score计算")
        save_progress(success_count, list(completed_codes))

if __name__ == "__main__":
    main()