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
                    handlers=[logging.FileHandler("stock_data_baostock.log"), logging.StreamHandler()])
logger = logging.getLogger('stock_data_baostock_fetcher')

# 检查baostock可用性
try:
    import baostock as bs
    BAOSTOCK_AVAILABLE = True
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
    progress_file = 'cache/fundamentals_baostock_progress.json'
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {"last_index": 0, "completed_codes": []}

def save_progress(index, completed_codes):
    """保存进度信息"""
    progress_file = 'cache/fundamentals_baostock_progress.json'
    try:
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump({"last_index": index, "completed_codes": completed_codes}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  保存进度失败: {e}")

def get_fundamentals_from_baostock(code):
    """使用baostock获取完整的基本面数据，包含访问控制策略"""
    if not BAOSTOCK_AVAILABLE:
        logger.warning("⚠️  baostock库未安装，无法获取数据")
        return None
        
    try:
        # 检查访问频率限制
        rate_limiter.check_rate_limit('baostock')
        
        # 登录baostock
        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f"❌ baostock登录失败: {lg.error_msg}")
            return None
        
        # 添加随机延迟，防止请求过快
        add_random_delay()
        
        # 转换股票代码格式（000001 -> sz.000001）
        market_code = f"sz.{code}" if str(code).startswith(('0', '3')) else f"sh.{code}"
        
        # 初始化基本面数据字典
        fundamental = {
            '股票代码': code,
            '股票名称': '',
            '股票上市日期': '',
            '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
            '股票所属行业': '',
            
            # 盈利能力
            '每股收益': '',
            '每股净资产': '',
            '净资产收益率': '',
            '总资产收益率': '',
            '毛利率': '',
            '净利率': '',
            '营业利润率': '',
            
            # 估值指标
            '市盈率（静）': '',
            '市盈率（TTM）': '',
            '市净率': '',
            '市销率': '',
            '股息率': '',
            
            # 成长性
            '营业收入增长率': '',
            '净利润增长率': '',
            '净资产增长率': '',
            '净利润增速': '',
            
            # 偿债能力
            '资产负债率': '',
            '流动比率': '',
            '速动比率': '',
            
            # 运营能力
            '总资产周转率': '',
            '存货周转率': '',
            '应收账款周转率': '',
            
            # 现金流
            '每股经营现金流': '',
            '现金流量比率': ''
        }
        
        # 获取最新年份和季度
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1
        
        # 尝试获取最近4个季度的数据，直到成功
        for year_offset in range(0, 2):
            for quarter_offset in range(0, 4):
                year = current_year - year_offset
                if year < 2007:  # baostock从2007年开始提供数据
                    break
                
                quarter = current_quarter - quarter_offset
                if quarter < 1:
                    quarter += 4
                    year -= 1
                
                # 获取季频盈利能力数据
                rs_profit = bs.query_profit_data(code=market_code, year=year, quarter=quarter)
                if rs_profit.error_code == '0':
                    profit_data = []
                    while rs_profit.error_code == '0' and rs_profit.next():
                        profit_data.append(rs_profit.get_row_data())
                    
                    if profit_data:
                        # 提取盈利能力数据
                        data = profit_data[0]
                        if len(data) > 4:
                            fundamental['净资产收益率'] = str(data[4])  # roe
                        if len(data) > 5:
                            fundamental['净利率'] = str(data[5])  # net_profit_ratio
                        if len(data) > 6:
                            fundamental['营业利润率'] = str(data[6])  # operating_profit_ratio
                        if len(data) > 7:
                            fundamental['毛利率'] = str(data[7])  # gross_profit_ratio
                        if len(data) > 11:
                            fundamental['每股收益'] = str(data[11])  # eps
                        if len(data) > 12:
                            fundamental['每股净资产'] = str(data[12])  # bps
                        
                        # 添加随机延迟
                        add_random_delay()
                        
                        # 获取资产负债表数据
                        rs_balance = bs.query_balance_data(code=market_code, year=year, quarter=quarter)
                        if rs_balance.error_code == '0':
                            balance_data = []
                            while rs_balance.error_code == '0' and rs_balance.next():
                                balance_data.append(rs_balance.get_row_data())
                            
                            if balance_data:
                                data = balance_data[0]
                                if len(data) > 13:
                                    fundamental['资产负债率'] = str(data[13])  # debt_to_assets_ratio
                                if len(data) > 14:
                                    current_ratio = float(data[14]) if data[14] else 0
                                    quick_ratio = float(data[15]) if data[15] else 0
                                    fundamental['流动比率'] = str(current_ratio)
                                    fundamental['速动比率'] = str(quick_ratio)
                        
                        # 添加随机延迟
                        add_random_delay()
                        
                        # 获取现金流量表数据
                        rs_cash = bs.query_cash_flow_data(code=market_code, year=year, quarter=quarter)
                        if rs_cash.error_code == '0':
                            cash_data = []
                            while rs_cash.error_code == '0' and rs_cash.next():
                                cash_data.append(rs_cash.get_row_data())
                            
                            if cash_data:
                                data = cash_data[0]
                                if len(data) > 24:
                                    fundamental['每股经营现金流'] = str(data[24])  # per_share_operation_cash_flow
                        
                        # 获取公司基本信息
                        rs_stock_basic = bs.query_stock_basic(code=market_code)
                        if rs_stock_basic.error_code == '0':
                            basic_data = []
                            while rs_stock_basic.error_code == '0' and rs_stock_basic.next():
                                basic_data.append(rs_stock_basic.get_row_data())
                            
                            if basic_data:
                                data = basic_data[0]
                                fundamental['股票名称'] = str(data[1]) if len(data) > 1 else ''
                                fundamental['股票上市日期'] = str(data[3]) if len(data) > 3 else ''
                                fundamental['股票所属行业'] = str(data[7]) if len(data) > 7 else ''
                        
                        # 获取估值数据 - 使用日K线数据中的市盈率等信息
                        rs_kdata = bs.query_history_k_data_plus(market_code, 
                                                              "date,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                                                              start_date=f"{year-1}-01-01", 
                                                              end_date=f"{year}-12-31", 
                                                              frequency="d", 
                                                              adjustflag="3")
                        
                        if rs_kdata.error_code == '0':
                            kdata_list = []
                            while rs_kdata.error_code == '0' and rs_kdata.next():
                                kdata_list.append(rs_kdata.get_row_data())
                            
                            if kdata_list:
                                # 取最新的估值数据
                                latest_kdata = kdata_list[-1]
                                fundamental['市盈率（TTM）'] = str(latest_kdata[1]) if len(latest_kdata) > 1 else ''
                                fundamental['市净率'] = str(latest_kdata[2]) if len(latest_kdata) > 2 else ''
                                fundamental['市销率'] = str(latest_kdata[3]) if len(latest_kdata) > 3 else ''
                                fundamental['现金流量比率'] = str(latest_kdata[4]) if len(latest_kdata) > 4 else ''
                        
                        break  # 成功获取数据后跳出循环
            if fundamental['股票名称']:  # 如果已获取到股票名称，表示数据获取成功
                break
        
        bs.logout()
        
        # 检查数据是否完整
        if fundamental['股票名称']:
            # 计算一些未直接获取的指标
            if fundamental['净利润增速'] == '' and fundamental['净利润增长率'] != '':
                fundamental['净利润增速'] = fundamental['净利润增长率']
            
            logger.info(f"✅ 使用baostock获取 {code} - {fundamental['股票名称']} 数据成功")
            return fundamental
        else:
            logger.warning(f"⚠️  {code} 数据不完整")
            return None
            
    except Exception as e:
        logger.error(f"❌ baostock获取 {code} 数据失败: {str(e)}")
        if BAOSTOCK_AVAILABLE:
            try:
                bs.logout()
            except:
                pass
        return None

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
        
        if mode == 'w' or not os.path.exists('cache/stockA_fundamentals_baostock.csv'):
            df.to_csv('cache/stockA_fundamentals_baostock.csv', index=False, encoding='utf-8-sig')
        else:
            df.to_csv('cache/stockA_fundamentals_baostock.csv', index=False, encoding='utf-8-sig', mode='a', header=False)
        
        return True
    except Exception as e:
        logger.error(f"❌ 保存批次数据失败: {e}")
        return False

def update_log(stock_count):
    """更新日志文件"""
    try:
        log_data = {
            "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "股票数量": stock_count,
            "数据源": "baostock",
            "可用数据源": {
                "baostock": BAOSTOCK_AVAILABLE
            },
            "文件路径": "cache/stockA_fundamentals_baostock.csv",
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
            ],
            "财务指标维度": {
                "盈利能力": ["每股收益", "每股净资产", "净资产收益率", "毛利率", "净利率", "营业利润率"],
                "估值指标": ["市盈率（TTM）", "市净率", "市销率"],
                "偿债能力": ["资产负债率", "流动比率", "速动比率"],
                "现金流": ["每股经营现金流"]
            }
        }
        
        with open('cache/fundamentals_baostock_update_log.json', 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        logger.error(f"❌ 更新日志失败: {e}")
        return False

def main():
    """主函数：获取A股股票完整基本面数据，支持断点续传"""
    logger.info("🚀 开始获取A股股票baostock基本面数据...")
    logger.info("📊 使用baostock数据源获取基本面数据")
    
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
    
    logger.info(f"📊 共{total_stocks}只股票，从第{start_index+1}只开始获取...")
    logger.info(f"✅ 已完成{len(completed_codes)}只股票")
    
    if start_index >= total_stocks:
        logger.info("🎉 所有股票数据已获取完成！")
        return
    
    # 获取待处理的股票
    remaining_codes = [code for code in stock_codes[start_index:] if code not in completed_codes]
    
    if not remaining_codes:
        logger.info("🎉 没有需要获取的股票数据")
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
                batch_delay = random.uniform(ANTI_CRAWL_CONFIG.BATCH_MIN_DELAY, ANTI_CRAWL_CONFIG.BATCH_MAX_DELAY)
                logger.info(f"   ⏱️  批次间延迟: {batch_delay:.2f}秒")
                time.sleep(batch_delay)
            
            for j, code in enumerate(batch_codes):
                # 重试机制
                retry_count = 0
                while retry_count < AntiCrawlConfig.MAX_RETRIES:
                    fundamental = get_fundamentals_from_baostock(code)
                    if fundamental:
                        break
                    retry_count += 1
                    logger.warning(f"   🔄 重试获取 {code} 数据 ({retry_count}/{AntiCrawlConfig.MAX_RETRIES})")
                    time.sleep(random.uniform(2, 5))  # 重试前增加较长延迟
                
                if fundamental and fundamental['股票名称']:
                    batch_fundamentals.append(fundamental)
                    success_count += 1
                    completed_codes.add(code)
                else:
                    logger.warning(f"   ⚠️  {code} 数据获取失败或不完整，跳过")
                
                # 间隔时间避免请求过快
                time.sleep(ANTI_CRAWL_CONFIG.STOCK_MIN_INTERVAL)
            
            # 保存批次数据
            if batch_fundamentals:
                if save_batch_to_csv(batch_fundamentals, mode):
                    current_index = start_index + i + len(batch_codes)
                    save_progress(current_index, list(completed_codes))
                    logger.info(f"   💾 批次数据已保存 ({len(batch_fundamentals)}条记录)")
                    mode = 'a'  # 后续批次使用追加模式
            else:
                logger.warning(f"   ⚠️  本批次无有效数据")
        
        # 更新最终日志
        update_log(success_count)
        
        logger.info(f"\n🎉 数据获取完成！")
        logger.info(f"📊 成功获取 {success_count}/{total_stocks} 只股票的真实数据")
        logger.info(f"📁 数据已保存到 cache/stockA_fundamentals_baostock.csv")
        logger.info(f"📡 数据源: baostock")
        logger.info(f"📈 数据包含完整财务指标：盈利能力、估值、偿债能力、现金流等维度")
        
        # 显示统计信息
        if os.path.exists('cache/stockA_fundamentals_baostock.csv'):
            df = pd.read_csv('cache/stockA_fundamentals_baostock.csv')
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
                preview_cols = ['股票代码', '股票名称', '股票所属行业', '每股收益', '净资产收益率', '市盈率（TTM）', '市净率']
                available_cols = [col for col in preview_cols if col in valid_data.columns]
                logger.info(valid_data[available_cols].head().to_string())
        
    except KeyboardInterrupt:
        logger.warning(f"\n⚠️  用户中断，进度已保存")
        logger.warning(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count)
    
    except Exception as e:
        logger.error(f"\n❌ 程序异常: {e}")
        logger.error(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count)

if __name__ == "__main__":
    main()