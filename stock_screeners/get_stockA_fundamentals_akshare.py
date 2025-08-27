import pandas as pd
import numpy as np
import requests
import json
import os
import time
from datetime import datetime
import warnings
import random
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# 配置日志
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("stock_data_akshare.log"), logging.StreamHandler()])
logger = logging.getLogger('stock_data_akshare_fetcher')

# 检查akshare可用性
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    logger.warning("⚠️  akshare库未安装，运行 `pip install akshare` 以启用akshare数据源")

# 确保cache目录存在
if not os.path.exists('cache'):
    os.makedirs('cache')

# 反爬配置
class AntiCrawlConfig:
    # 随机User-Agent列表
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36 Edg/96.0.1054.29"
    ]
    
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
    RETRY_BACKOFF_FACTOR = 0.3  # 重试退避因子

# 创建带重试机制的session
def create_session():
    session = requests.Session()
    retry = Retry(total=AntiCrawlConfig.MAX_RETRIES,
                 backoff_factor=AntiCrawlConfig.RETRY_BACKOFF_FACTOR,
                 status_forcelist=[500, 502, 503, 504, 429])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

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
            'akshare': 0,
            'eastmoney': 0
        }
        self.last_reset_times = {
            'akshare': time.time(),
            'eastmoney': time.time()
        }
        self.rate_limits = {
            'akshare': 60,  # 每分钟最多请求数
            'eastmoney': 50
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

warnings.filterwarnings('ignore')

def get_stock_list():
    """从本地文件读取股票列表"""
    try:
        stock_list = pd.read_csv('cache/stockA_list.csv')
        print(f"✅ 成功读取股票列表，共{len(stock_list)}只股票")
        return stock_list
    except Exception as e:
        print(f"❌ 读取股票列表失败: {e}")
        return None

def load_progress():
    """加载进度信息"""
    progress_file = 'cache/fundamentals_akshare_progress.json'
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {"last_index": 0, "completed_codes": []}

def save_progress(index, completed_codes):
    """保存进度信息"""
    progress_file = 'cache/fundamentals_akshare_progress.json'
    try:
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump({"last_index": index, "completed_codes": completed_codes}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  保存进度失败: {e}")

def get_fundamentals_from_akshare(code):
    """使用akshare获取股票基本面数据，包含访问控制策略"""
    if not AKSHARE_AVAILABLE:
        return None
    
    try:
        import akshare as ak  # 确保ak变量在函数内部可用
        
        # 检查访问频率限制
        rate_limiter.check_rate_limit('akshare')
        
        # 获取股票基本信息
        try:
            stock_info = ak.stock_individual_info_em(symbol=code)
            stock_name = str(stock_info.loc[stock_info['item'] == '股票简称', 'value'].iloc[0]) if not stock_info.empty else ''
            ipo_date = str(stock_info.loc[stock_info['item'] == '上市时间', 'value'].iloc[0]) if not stock_info.empty else ''
            industry = str(stock_info.loc[stock_info['item'] == '行业', 'value'].iloc[0]) if not stock_info.empty else ''
            
            # 添加随机延迟，防止请求过快
            add_random_delay()
        except Exception as e:
            logger.warning(f"获取股票基本信息失败: {e}")
            stock_name = ''
            ipo_date = ''
            industry = ''
        
        # 获取财务指标数据 - 使用多个备用接口
        latest_data = {}
        profit_data = {}
        valuation_data = {}
        
        # 方法1: 使用财务摘要
        try:
            financial_indicator = ak.stock_financial_abstract(symbol=code)
            if not financial_indicator.empty:
                latest_data = financial_indicator.iloc[0].to_dict() if hasattr(financial_indicator.iloc[0], 'to_dict') else {}
            
            # 添加随机延迟
            add_random_delay()
        except Exception as e:
            logger.warning(f"获取财务摘要失败: {e}")
            pass
        
        # 方法2: 使用财务分析指标
        try:
            profit_ability = ak.stock_financial_analysis_indicator(symbol=code)
            if not profit_ability.empty:
                profit_data = profit_ability.iloc[0].to_dict() if hasattr(profit_ability.iloc[0], 'to_dict') else {}
            
            # 添加随机延迟
            add_random_delay()
        except Exception as e:
            logger.warning(f"获取财务分析指标失败: {e}")
            pass
        
        # 方法3: 使用个股估值指标
        try:
            # 先添加较长延迟
            time.sleep(1.0)
            
            # 尝试使用备用接口获取估值数据
            try:
                # 方法A: 使用实时行情接口（akshare）
                stock_zh_a_spot = ak.stock_zh_a_spot()
                stock_data = stock_zh_a_spot[stock_zh_a_spot['代码'] == code]
                if not stock_data.empty:
                    valuation_data = {
                        '市盈率': str(stock_data.iloc[0].get('市盈率', '')),
                        '市净率': str(stock_data.iloc[0].get('市净率', '')),
                        '股息率': str(stock_data.iloc[0].get('股息率', ''))
                    }
            except Exception as inner_e:
                logger.warning(f"A股实时行情接口失败: {inner_e}")
                # 方法B: 降级方案：使用东方财富的个股行情接口
                try:
                    # 先添加较长延迟
                    time.sleep(1.5)
                    # 直接使用requests访问东方财富API，添加完整的反爬请求头
                    session = create_session()
                    headers = {
                        'User-Agent': random.choice(AntiCrawlConfig.USER_AGENTS),
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Referer': f'https://quote.eastmoney.com/{code}.html',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Connection': 'keep-alive',
                        'Cache-Control': 'no-cache'
                    }
                    secid = f"0.{code}" if str(code).startswith(('0', '3')) else f"1.{code}"
                    url = f"http://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f163,f164,f167,f168,f188"
                    response = session.get(url, headers=headers, timeout=10)
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            if 'data' in data:
                                stock_data = data['data']
                                valuation_data = {
                                    '市盈率': str(stock_data.get('f164', '')),  # 市盈率(TTM)
                                    '市净率': str(stock_data.get('f167', '')),
                                    '股息率': str(stock_data.get('f188', ''))
                                }
                        except json.JSONDecodeError as json_e:
                            logger.warning(f"东方财富API返回格式错误，可能是HTML: {json_e}")
                except Exception as inner_e3:
                    logger.warning(f"东方财富估值接口失败: {inner_e3}")
            
            # 添加随机延迟
            add_random_delay(1.0, 2.0)
        except Exception as e:
            logger.warning(f"获取个股估值指标失败: {e}")
            pass
        
        # 方法4: 使用东财的财务数据接口
        try:
            stock_financial = ak.stock_financial_analysis_indicator(symbol=code)
            if not stock_financial.empty:
                financial_dict = stock_financial.iloc[0].to_dict() if hasattr(stock_financial.iloc[0], 'to_dict') else {}
                # 合并数据
                for key, value in financial_dict.items():
                    if key not in latest_data:
                        latest_data[key] = value
        except Exception as e:
            logger.warning(f"获取东财财务数据失败: {e}")
            pass
        
        # 构建完整的基本面数据
        fundamental = {
            '股票代码': code,
            '股票名称': stock_name,
            '股票上市日期': ipo_date,
            '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
            '股票所属行业': industry,
            
            # 盈利能力
            '每股收益': str(latest_data.get('基本每股收益', '') or latest_data.get('每股收益', '')),
            '每股净资产': str(latest_data.get('每股净资产', '') or latest_data.get('净资产', '')),
            '净资产收益率': str(profit_data.get('净资产收益率', '') or latest_data.get('净资产收益率', '')),
            '总资产收益率': str(profit_data.get('总资产报酬率', '') or latest_data.get('总资产收益率', '')),
            '毛利率': str(profit_data.get('销售毛利率', '') or latest_data.get('毛利率', '')),
            '净利率': str(profit_data.get('销售净利率', '') or latest_data.get('净利率', '')),
            '营业利润率': str(profit_data.get('营业利润率', '') or latest_data.get('营业利润率', '')),
            
            # 估值指标
            '市盈率（静）': str(valuation_data.get('市盈率', '')),
            '市盈率（TTM）': str(valuation_data.get('市盈率TTM', '')),
            '市净率': str(valuation_data.get('市净率', '')),
            '市销率': str(valuation_data.get('市销率', '')),
            '股息率': str(valuation_data.get('股息率', '')),
            
            # 成长性
            '营业收入增长率': str(latest_data.get('营业收入增长率', '') or latest_data.get('营收增长率', '')),
            '净利润增长率': str(latest_data.get('净利润增长率', '') or latest_data.get('净利润增速', '')),
            '净资产增长率': str(latest_data.get('净资产增长率', '')),
            '净利润增速': str(latest_data.get('净利润增长率', '') or latest_data.get('净利润增速', '')),
            
            # 偿债能力
            '资产负债率': str(latest_data.get('资产负债率', '')),
            '流动比率': str(latest_data.get('流动比率', '')),
            '速动比率': str(latest_data.get('速动比率', '')),
            
            # 运营能力
            '总资产周转率': str(latest_data.get('总资产周转率', '')),
            '存货周转率': str(latest_data.get('存货周转率', '')),
            '应收账款周转率': str(latest_data.get('应收账款周转率', '')),
            
            # 现金流
            '每股经营现金流': str(latest_data.get('每股经营现金流', '') or latest_data.get('经营现金流', '')),
            '现金流量比率': str(latest_data.get('现金流量比率', ''))
        }
        
        return fundamental
        
    except Exception as e:
        logger.error(f"❌ akshare获取 {code} 数据失败: {str(e)}")
        return None

def get_fundamentals_real_data(code):
    """获取单只股票的真实基本面数据"""
    
    # 使用akshare获取数据
    result = get_fundamentals_from_akshare(code)
    if result and result.get('股票名称') and result.get('股票名称') != '':
        logger.info(f"   ✅ 使用akshare获取 {code} 数据成功")
        return result
    
    # 如果所有数据源都失败，返回基本信息
    logger.warning(f"   ❌ 获取 {code} 数据失败")
    return {
        '股票代码': code,
        '股票名称': '',
        '股票上市日期': '',
        '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
        '股票所属行业': ''
    }

def save_batch_to_csv(batch_data, mode='a'):
    """将批次数据保存到CSV文件"""
    try:
        df = pd.DataFrame(batch_data)
        
        # 确保数据格式正确
        numeric_columns = [
            '每股收益', '每股净资产', '净资产收益率', '总资产收益率', '毛利率', '净利率', '营业利润率',
            '市盈率（静）', '市盈率（TTM）', '市净率', '市销率', '股息率',
            '营业收入增长率', '净利润增长率', '净资产增长率', '净利润增速',
            '资产负债率', '流动比率', '速动比率',
            '总资产周转率', '存货周转率', '应收账款周转率',
            '每股经营现金流', '现金流量比率'
        ]
        
        for col in df.columns:
            if col in numeric_columns:
                df[col] = df[col].replace(['', 'None', 'nan'], np.nan)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        csv_path = 'cache/stockA_fundamentals_akshare.csv'
        if mode == 'w' or not os.path.exists(csv_path):
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        else:
            df.to_csv(csv_path, index=False, encoding='utf-8-sig', mode='a', header=False)
        
        return True
    except Exception as e:
        print(f"❌ 保存批次数据失败: {e}")
        return False

def update_log(stock_count, status='completed'):
    """更新日志文件"""
    try:
        log_data = {
            "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "股票数量": stock_count,
            "数据源": "akshare",
            "状态": status,
            "可用数据源": {
                "akshare": AKSHARE_AVAILABLE
            },
            "文件路径": "cache/stockA_fundamentals_akshare.csv",
            "包含字段": [
                "股票代码", "股票名称", "股票上市日期", "股票上市地点", "股票所属行业",
                # 盈利能力
                "每股收益", "每股净资产", "净资产收益率", "总资产收益率", "毛利率", "净利率", "营业利润率",
                # 估值指标
                "市盈率（静）", "市盈率（TTM）", "市净率", "市销率", "股息率",
                # 成长性
                "营业收入增长率", "净利润增长率", "净资产增长率", "净利润增速",
                # 偿债能力
                "资产负债率", "流动比率", "速动比率",
                # 运营能力
                "总资产周转率", "存货周转率", "应收账款周转率",
                # 现金流
                "每股经营现金流", "现金流量比率"
            ]
        }
        
        log_path = 'cache/fundamentals_akshare_update_log.json'
        # 如果日志文件存在，添加历史记录
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    existing_log = json.load(f)
                if "历史记录" in existing_log:
                    existing_log["历史记录"].insert(0, log_data)
                    # 保留最近100条历史记录
                    if len(existing_log["历史记录"]) > 100:
                        existing_log["历史记录"] = existing_log["历史记录"][:100]
                else:
                    existing_log["历史记录"] = [log_data]
                log_data = existing_log
            except Exception as e:
                logger.warning(f"读取历史日志失败: {e}")
        else:
            log_data = {"当前状态": log_data, "历史记录": [log_data]}
        
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        print(f"❌ 更新日志失败: {e}")
        return False

def main():
    """主函数：获取A股股票完整基本面数据，支持断点续传"""
    logger.info("🚀 开始获取A股股票完整基本面数据 (akshare模式)...")
    logger.info("📊 财务指标包括：盈利能力、估值指标、成长性、偿债能力、运营能力和现金流")
    
    # 显示可用数据源状态
    logger.info("📡 数据源状态:")
    logger.info(f"   akshare: {'✅ 可用' if AKSHARE_AVAILABLE else '❌ 未安装'}")
    
    if not AKSHARE_AVAILABLE:
        logger.error("❌ 没有可用的数据源，请安装akshare")
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
    
    logger.info(f"📊 共{total_stocks}只股票，从第{start_index+1}只开始获取...")
    logger.info(f"✅ 已完成{len(completed_codes)}只股票")
    
    if start_index >= total_stocks:
        logger.info("🎉 所有股票数据已获取完成！")
        update_log(len(completed_codes))
        return
    
    # 获取待处理的股票
    remaining_codes = [code for code in stock_codes[start_index:] if code not in completed_codes]
    
    if not remaining_codes:
        logger.info("🎉 没有需要获取的股票数据")
        update_log(len(completed_codes))
        return
    
    # 初始化或追加模式
    mode = 'w' if start_index == 0 else 'a'
    
    # 分批获取数据
    batch_size = 20  # 每批处理20只股票，避免请求过快
    success_count = len(completed_codes)
    
    try:
        for i in range(0, len(remaining_codes), batch_size):
            batch_codes = remaining_codes[i:i+batch_size]
            batch_fundamentals = []
            
            current_start = start_index + i + 1
            current_end = min(start_index + i + batch_size, total_stocks)
            logger.info(f"\n🔄 处理第{current_start}-{current_end}只股票...")
            
            # 检查当前批次的总体访问频率
            if i > 0:  # 不是第一批
                # 根据配置添加批次间延迟
                batch_delay = random.uniform(AntiCrawlConfig.BATCH_MIN_DELAY, AntiCrawlConfig.BATCH_MAX_DELAY)
                logger.info(f"   ⏱️  批次间延迟: {batch_delay:.2f}秒")
                time.sleep(batch_delay)
            
            for j, code in enumerate(batch_codes):
                # 设置随机User-Agent
                headers = {'User-Agent': random.choice(AntiCrawlConfig.USER_AGENTS)}
                
                fundamental = get_fundamentals_real_data(code)
                
                if fundamental['股票名称'] and fundamental['股票名称'] != '':
                    batch_fundamentals.append(fundamental)
                    success_count += 1
                    completed_codes.add(code)
                    
                    # 每只股票显示进度
                    logger.info(f"   ✅ {code} - {fundamental['股票名称']} 已获取")
                else:
                    logger.warning(f"   ⚠️  {code} 数据不完整，跳过")
                
                # 间隔时间避免请求过快
                time.sleep(AntiCrawlConfig.STOCK_MIN_INTERVAL)
            
            # 保存批次数据
            if batch_fundamentals:
                if save_batch_to_csv(batch_fundamentals, mode):
                    current_index = start_index + i + len(batch_codes)
                    save_progress(current_index, list(completed_codes))
                    logger.info(f"   💾 批次数据已保存 ({len(batch_fundamentals)}条记录)")
                    mode = 'a'  # 后续批次使用追加模式
                    # 保存当前进度到日志
                    update_log(success_count, status='in_progress')
            else:
                logger.warning(f"   ⚠️  本批次无有效数据")
        
        # 更新最终日志
        update_log(success_count)
        
        logger.info(f"\n🎉 数据获取完成！")
        logger.info(f"📊 成功获取 {success_count}/{total_stocks} 只股票的真实数据")
        logger.info(f"📁 数据已保存到 cache/stockA_fundamentals_akshare.csv")
        logger.info(f"📝 更新日志已保存到 cache/fundamentals_akshare_update_log.json")
        
        # 显示统计信息
        csv_path = 'cache/stockA_fundamentals_akshare.csv'
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            logger.info(f"\n📈 数据统计：")
            logger.info(f"   📊 总记录数: {len(df)}")
            
            # 显示有效数据统计
            valid_data = df[df['股票名称'].notna() & (df['股票名称'] != '')]
            logger.info(f"   ✅ 有效数据: {len(valid_data)}")
            
            if len(valid_data) > 0:
                # 行业分布
                industry_counts = valid_data['股票所属行业'].value_counts()
                if len(industry_counts) > 0:
                    logger.info(f"   🏢 主要行业: {industry_counts.head(3).to_dict()}")
                
                # 显示前几条数据
                logger.info(f"\n📋 数据预览:")
                preview_cols = ['股票代码', '股票名称', '股票所属行业', '每股收益', '净资产收益率', '市盈率（静）', '市净率']
                available_cols = [col for col in preview_cols if col in valid_data.columns]
                logger.info(valid_data[available_cols].head().to_string())

    except KeyboardInterrupt:
        logger.warning(f"\n⚠️  用户中断，进度已保存")
        logger.warning(f"   已完成 {success_count} 只股票")
        save_progress(start_index + len(remaining_codes[:i+batch_size]), list(completed_codes))
        update_log(success_count, status='interrupted')

    except Exception as e:
        logger.error(f"\n❌ 程序异常: {e}")
        logger.error(f"   已完成 {success_count} 只股票")
        save_progress(start_index + len(remaining_codes[:i+batch_size]), list(completed_codes))
        update_log(success_count, status='error')

if __name__ == "__main__":
    main()