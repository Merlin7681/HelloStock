import pandas as pd
import numpy as np
import json
import os
import time
from datetime import datetime
import logging
import traceback

# 确保logs目录存在
if not os.path.exists('logs'):
    os.makedirs('logs')

# 配置日志
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/stock_fscore_baostock.log"), logging.StreamHandler()])
logger = logging.getLogger('stock_fscore_baostock_calculator')

# 检查baostock可用性
try:
    import baostock as bs
    BAOSTOCK_AVAILABLE = True
    logger.info("✅ 成功导入baostock库")
except ImportError:
    BAOSTOCK_AVAILABLE = False
    logger.error("⚠️  baostock库未安装，运行 `pip install baostock` 以启用baostock数据源")

# 确保cache目录存在
if not os.path.exists('cache'):
    os.makedirs('cache')

class FScoreCalculator:
    def __init__(self):
        self.current_year = datetime.now().year
        self.current_quarter = (datetime.now().month - 1) // 3 + 1
        self.is_logged_in = False
        
    def login_baostock(self):
        """登录baostock系统"""
        if not BAOSTOCK_AVAILABLE:
            return False
        
        if self.is_logged_in:
            return True
        
        lg = bs.login()
        if lg.error_code == '0':
            self.is_logged_in = True
            logger.info("✅ baostock登录成功")
            return True
        else:
            logger.error(f"❌ baostock登录失败: {lg.error_msg}")
            return False
    
    def logout_baostock(self):
        """登出baostock系统"""
        if BAOSTOCK_AVAILABLE and self.is_logged_in:
            try:
                bs.logout()
                self.is_logged_in = False
                logger.info("✅ baostock登出成功")
            except Exception as e:
                logger.error(f"❌ baostock登出异常: {str(e)}")
        
    def get_stock_basic_info(self, code):
        """获取股票基本信息，包括名称和行业"""
        if not self.login_baostock():
            return None
        
        market_code = f"sz.{code}" if code.startswith(('0', '3')) else f"sh.{code}"
        rs = bs.query_stock_basic(code=market_code)
        
        if rs.error_code != '0':
            logger.error(f"❌ 获取 {code} 基本信息失败: {rs.error_msg}")
            return None
        
        stock_info = {}
        while rs.error_code == '0' and rs.next():
            data = rs.get_row_data()
            stock_info['stock_name'] = data[1] if len(data) > 1 else ''
            stock_info['industry'] = data[7] if len(data) > 7 else ''
        
        return stock_info
    
    def get_financial_data(self, code):
        """获取股票的财务数据，包括近两年的财务指标"""
        if not self.login_baostock():
            return None
        
        market_code = f"sz.{code}" if code.startswith(('0', '3')) else f"sh.{code}"
        financial_data = {'stock_code': code}
        
        # 获取近两年的财务数据
        for year_offset in range(0, 2):
            year = self.current_year - year_offset
            # 使用最近的完整季度数据
            for quarter in range(4, 0, -1):
                # 获取盈利能力数据
                rs_profit = bs.query_profit_data(code=market_code, year=year, quarter=quarter)
                if rs_profit.error_code == '0':
                    profit_data = []
                    while rs_profit.error_code == '0' and rs_profit.next():
                        profit_data.append(rs_profit.get_row_data())
                    
                    if profit_data:
                        data = profit_data[0]
                        prefix = 'current_' if year_offset == 0 else 'previous_'
                        financial_data[f'{prefix}roa'] = float(data[4]) if len(data) > 4 and data[4] else None  # 净资产收益率
                        financial_data[f'{prefix}gross_margin'] = float(data[7]) if len(data) > 7 and data[7] else None  # 毛利率
                        break
            
            # 获取资产负债表数据
            for quarter in range(4, 0, -1):
                rs_balance = bs.query_balance_data(code=market_code, year=year, quarter=quarter)
                if rs_balance.error_code == '0':
                    balance_data = []
                    while rs_balance.error_code == '0' and rs_balance.next():
                        balance_data.append(rs_balance.get_row_data())
                    
                    if balance_data:
                        data = balance_data[0]
                        prefix = 'current_' if year_offset == 0 else 'previous_'
                        financial_data[f'{prefix}leverage'] = float(data[13]) if len(data) > 13 and data[13] else None  # 资产负债率
                        financial_data[f'{prefix}current_ratio'] = float(data[14]) if len(data) > 14 and data[14] else None  # 流动比率
                        break
            
            # 获取现金流量表数据
            for quarter in range(4, 0, -1):
                rs_cash = bs.query_cash_flow_data(code=market_code, year=year, quarter=quarter)
                if rs_cash.error_code == '0':
                    cash_data = []
                    while rs_cash.error_code == '0' and rs_cash.next():
                        cash_data.append(rs_cash.get_row_data())
                    
                    if cash_data:
                        data = cash_data[0]
                        prefix = 'current_' if year_offset == 0 else 'previous_'
                        financial_data[f'{prefix}operating_cash_flow'] = float(data[24]) if len(data) > 24 and data[24] else None  # 每股经营现金流
                        break
            
            # 获取总资产周转率
            rs_indicator = bs.query_operation_data(code=market_code, year=year, quarter=4)  # 使用年报数据
            if rs_indicator.error_code == '0':
                indicator_data = []
                while rs_indicator.error_code == '0' and rs_indicator.next():
                    indicator_data.append(rs_indicator.get_row_data())
                
                if indicator_data:
                    data = indicator_data[0]
                    prefix = 'current_' if year_offset == 0 else 'previous_'
                    financial_data[f'{prefix}asset_turnover'] = float(data[3]) if len(data) > 3 and data[3] else None  # 总资产周转率
        
        return financial_data
    
    def calculate_f_score(self, fundamental_data):
        """计算Piotroski F-Score"""
        f_score = 0
        score_details = {
            'positive_roa': 0,  # 资产收益率为正
            'positive_operating_cash_flow': 0,  # 经营现金流为正
            'roa_improved': 0,  # 资产收益率增长
            'accruals': 0,  # 经营现金流大于净利润
            'leverage_improved': 0,  # 杠杆率降低
            'current_ratio_improved': 0,  # 流动比率提高
            'no_new_equity': 1,  # 简化处理：假设未发行新股
            'gross_margin_improved': 0,  # 毛利率提高
            'asset_turnover_improved': 0  # 资产周转率提高
        }
        
        # 1. 资产收益率为正
        current_roa = fundamental_data.get('current_roa', 0)
        if current_roa is not None and current_roa > 0:
            f_score += 1
            score_details['positive_roa'] = 1
        
        # 2. 经营现金流为正
        current_operating_cash_flow = fundamental_data.get('current_operating_cash_flow', 0)
        if current_operating_cash_flow is not None and current_operating_cash_flow > 0:
            f_score += 1
            score_details['positive_operating_cash_flow'] = 1
        
        # 3. 资产收益率增长
        previous_roa = fundamental_data.get('previous_roa')
        if (current_roa is not None and previous_roa is not None and current_roa > previous_roa):
            f_score += 1
            score_details['roa_improved'] = 1
        
        # 4. 经营现金流大于净利润（简化处理）
        if current_operating_cash_flow is not None and current_roa is not None and current_operating_cash_flow > 0 and current_roa > 0:
            f_score += 1
            score_details['accruals'] = 1
        
        # 5. 杠杆率降低
        current_leverage = fundamental_data.get('current_leverage')
        previous_leverage = fundamental_data.get('previous_leverage')
        if (current_leverage is not None and previous_leverage is not None and current_leverage < previous_leverage):
            f_score += 1
            score_details['leverage_improved'] = 1
        
        # 6. 流动比率提高
        current_current_ratio = fundamental_data.get('current_current_ratio')
        previous_current_ratio = fundamental_data.get('previous_current_ratio')
        if (current_current_ratio is not None and previous_current_ratio is not None and current_current_ratio > previous_current_ratio):
            f_score += 1
            score_details['current_ratio_improved'] = 1
        
        # 7. 未发行新股（简化处理，默认为1）
        # 这里简化处理，假设未发行新股
        
        # 8. 毛利率提高
        current_gross_margin = fundamental_data.get('current_gross_margin')
        previous_gross_margin = fundamental_data.get('previous_gross_margin')
        if (current_gross_margin is not None and previous_gross_margin is not None and current_gross_margin > previous_gross_margin):
            f_score += 1
            score_details['gross_margin_improved'] = 1
        
        # 9. 资产周转率提高
        current_asset_turnover = fundamental_data.get('current_asset_turnover')
        previous_asset_turnover = fundamental_data.get('previous_asset_turnover')
        if (current_asset_turnover is not None and previous_asset_turnover is not None and current_asset_turnover > previous_asset_turnover):
            f_score += 1
            score_details['asset_turnover_improved'] = 1
        
        result = {
            'stock_code': fundamental_data.get('stock_code', ''),
            'f_score': f_score,
            'score_details': score_details
        }
        
        return result
    
    def analyze_stock(self, code):
        """分析指定股票的F-Score"""
        logger.info(f"📊 开始分析股票 {code} 的Piotroski F-Score")
        
        # 获取基本面数据
        financial_data = self.get_financial_data(code)
        if not financial_data:
            logger.error(f"❌ 无法获取 {code} 的财务数据")
            return None
        
        # 获取股票基本信息
        basic_info = self.get_stock_basic_info(code)
        if basic_info:
            financial_data.update(basic_info)
        
        # 计算F-Score
        f_score_result = self.calculate_f_score(financial_data)
        
        if f_score_result:
            # 集成股票名称和行业信息
            f_score_result['stock_name'] = financial_data.get('stock_name', '')
            f_score_result['industry'] = financial_data.get('industry', '')
            
            # 包含9项财务指标的原始数据
            f_score_result.update({
                'current_roa': financial_data.get('current_roa', None),
                'current_operating_cash_flow': financial_data.get('current_operating_cash_flow', None),
                'current_leverage': financial_data.get('current_leverage', None),
                'current_current_ratio': financial_data.get('current_current_ratio', None),
                'current_gross_margin': financial_data.get('current_gross_margin', None),
                'current_asset_turnover': financial_data.get('current_asset_turnover', None),
                'previous_roa': financial_data.get('previous_roa', None),
                'previous_leverage': financial_data.get('previous_leverage', None),
                'previous_current_ratio': financial_data.get('previous_current_ratio', None),
                'previous_gross_margin': financial_data.get('previous_gross_margin', None),
                'previous_asset_turnover': financial_data.get('previous_asset_turnover', None)
            })
        
        return f_score_result

def get_stock_list():
    """从本地文件读取股票列表"""
    try:
        stock_list = pd.read_csv('cache/stockA_list.csv', header=None, names=['code', 'name', 'status', 'market', 'type', 'remark'])
        logger.info(f"✅ 成功读取股票列表，共{len(stock_list)}只股票")
        return stock_list
    except Exception as e:
        logger.error(f"❌ 读取股票列表失败: {e}")
        return None

def save_f_score_results(results, append=False):
    """保存F-Score计算结果到CSV文件
    
    Args:
        results (list): F-Score计算结果列表
        append (bool): 是否追加到现有文件，默认为False（覆盖）
    
    Returns:
        bool: 保存是否成功
    """
    try:
        if not results:
            logger.warning("⚠️  没有有效的结果可以保存")
            return False
        
        # 准备要保存的数据
        data_to_save = []
        for result in results:
            # 提取所需字段
            row = {
                '股票名称': result.get('stock_name', ''),
                '股票代码': result.get('stock_code', ''),
                '股票所属行业': result.get('industry', ''),
                'F-Score值': result.get('f_score', 0)
            }
            
            # 添加9项指标的详细信息
            score_details = result.get('score_details', {})
            row.update({
                '资产收益率为正': score_details.get('positive_roa', 0),
                '经营现金流为正': score_details.get('positive_operating_cash_flow', 0),
                '资产收益率增长': score_details.get('roa_improved', 0),
                '经营现金流大于净利润': score_details.get('accruals', 0),
                '杠杆率降低': score_details.get('leverage_improved', 0),
                '流动比率提高': score_details.get('current_ratio_improved', 0),
                '未发行新股': score_details.get('no_new_equity', 0),
                '毛利率提高': score_details.get('gross_margin_improved', 0),
                '资产周转率提高': score_details.get('asset_turnover_improved', 0)
            })
            
            data_to_save.append(row)
        
        # 创建DataFrame
        df = pd.DataFrame(data_to_save)
        
        # 按F-Score值排序
        df_sorted = df.sort_values(by='F-Score值', ascending=False)
        
        # 保存到CSV文件
        csv_path = os.path.join('cache', 'stockA_fscore_baostock.csv')
        
        if append and os.path.exists(csv_path):
            # 如果是追加且文件存在，读取现有数据并合并
            existing_df = pd.read_csv(csv_path, encoding='utf-8-sig')
            combined_df = pd.concat([existing_df, df_sorted]).sort_values(by='F-Score值', ascending=False)
            # 去重（防止重复保存相同的股票数据）
            combined_df = combined_df.drop_duplicates(subset=['股票代码'], keep='last')
            combined_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"💾 F-Score计算结果已追加到 {csv_path}")
        else:
            # 覆盖保存或创建新文件
            df_sorted.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"💾 F-Score计算结果已保存到 {csv_path}")
        
        return True
    except Exception as e:
        logger.error(f"❌ 保存F-Score结果失败: {e}")
        logger.error(traceback.format_exc())
        return False

def main():
    """主函数：批量计算A股股票的Piotroski F-Score"""
    logger.info("🚀 开始批量计算A股股票的Piotroski F-Score...")
    
    # 检查baostock是否可用
    if not BAOSTOCK_AVAILABLE:
        logger.error("❌ baostock库未安装，无法获取数据，请运行 pip install baostock 后重试")
        return
    
    # 获取股票列表
    stock_list = get_stock_list()
    if stock_list is None:
        return
    
    stock_codes = stock_list['code'].astype(str).str.zfill(6).tolist()
    total_stocks = len(stock_codes)
    logger.info(f"📊 共{total_stocks}只股票需要计算F-Score")
    
    # 创建计算器实例
    calculator = FScoreCalculator()
    all_results = []
    success_count = 0
    last_save_count = 0  # 记录上次保存时的成功数量
    save_interval = 10   # 每10条数据保存一次
    
    # 确保开始时删除旧文件（如果存在），以便从头开始
    csv_path = os.path.join('cache', 'stockA_fscore_baostock.csv')
    if os.path.exists(csv_path):
        try:
            os.remove(csv_path)
            logger.info(f"🧹 已删除旧的结果文件: {csv_path}")
        except Exception as e:
            logger.warning(f"⚠️ 删除旧文件失败: {e}")
    
    try:
        # 批量处理股票（一次login获取多只股票数据）
        if calculator.login_baostock():
            batch_size = 50  # 每批处理50只股票
            for i in range(0, total_stocks, batch_size):
                batch_codes = stock_codes[i:i+batch_size]
                batch_start = i + 1
                batch_end = min(i + batch_size, total_stocks)
                logger.info(f"🔄 处理第{batch_start}-{batch_end}只股票...")
                
                batch_results = []
                for code in batch_codes:
                    try:
                        # 分析单只股票
                        result = calculator.analyze_stock(code)
                        if result:
                            batch_results.append(result)
                            success_count += 1
                            logger.info(f"   ✅ {code} - {result.get('stock_name', '')} 的F-Score: {result.get('f_score')}")
                            
                            # 每获取10条数据就保存一次
                            if success_count - last_save_count >= save_interval:
                                logger.info(f"🔄 已累积{success_count - last_save_count}条新数据，准备保存...")
                                recent_results = batch_results[-save_interval:]
                                save_f_score_results(recent_results, append=True)
                                last_save_count = success_count
                        else:
                            logger.warning(f"   ⚠️  {code} 数据获取失败或不完整，无法计算F-Score")
                    except Exception as e:
                        logger.error(f"   ❌ {code} 处理异常: {str(e)}")
                
                # 将批次结果添加到总结果中
                all_results.extend(batch_results)
                
                # 显示批次进度
                logger.info(f"✅ 已完成 {success_count}/{total_stocks} 只股票的F-Score计算")
                
        # 保存剩余的结果（如果有的话）
        if all_results and last_save_count < len(all_results):
            remaining_results = all_results[last_save_count:]
            if remaining_results:
                logger.info(f"🔄 保存剩余的{len(remaining_results)}条数据...")
                save_f_score_results(remaining_results, append=True)
            
            # 显示F-Score分布统计
            f_score_counts = pd.Series([r['f_score'] for r in all_results]).value_counts().sort_index(ascending=False)
            logger.info("\n📊 F-Score分布统计:")
            for score, count in f_score_counts.items():
                logger.info(f"   F-Score {score}: {count}只股票 ({count/len(all_results)*100:.1f}%)")
            
            # 显示前5名高分股票
            top_5_results = sorted(all_results, key=lambda x: x['f_score'], reverse=True)[:5]
            logger.info("\n🏆 前5名高分股票:")
            for i, result in enumerate(top_5_results):
                logger.info(f"   {i+1}. {result['stock_code']} - {result['stock_name']} (行业: {result['industry']}) - F-Score: {result['f_score']}")
        else:
            logger.warning("⚠️  没有计算到任何股票的F-Score")
            
    except KeyboardInterrupt:
        logger.warning("⚠️ 用户中断，已计算部分结果")
        if all_results and last_save_count < len(all_results):
            remaining_results = all_results[last_save_count:]
            if remaining_results:
                logger.info(f"🔄 保存剩余的{len(remaining_results)}条数据...")
                save_f_score_results(remaining_results, append=True)
    except Exception as e:
        logger.error(f"❌ 程序异常: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # 确保登出baostock
        calculator.logout_baostock()
        
    logger.info(f"🎉 F-Score计算完成！成功计算 {success_count}/{total_stocks} 只股票")

if __name__ == "__main__":
    main()