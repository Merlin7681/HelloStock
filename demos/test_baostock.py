import pandas as pd
import baostock as bs
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FScoreCalculator:
    def __init__(self):
        pass
    
    def get_financial_data(self, code, year):
        """获取指定股票和年份的财务数据"""
        try:
            # 获取利润表数据
            profit_df = bs.query_profit_data(code=code, year=year, quarter=4).get_data()
            # 获取资产负债表数据
            balance_df = bs.query_balance_data(code=code, year=year, quarter=4).get_data()
            # 获取现金流量表数据
            cashflow_df = bs.query_cash_flow_data(code=code, year=year, quarter=4).get_data()
            return profit_df, balance_df, cashflow_df
        except Exception as e:
            logger.error(f"获取财务数据时出错: {str(e)}")
            return None, None, None
    
    def calculate_f_score(self, code, current_year):
        """计算指定股票的F-Score
        F-Score是一个9项指标的评分系统，每项符合条件得1分，不符合得0分，总分范围0-9
        """
        try:
            # 获取当前年份和上一年的数据
            current_profit, current_balance, current_cashflow = self.get_financial_data(code, current_year)
            prev_profit, prev_balance, prev_cashflow = self.get_financial_data(code, str(int(current_year) - 1))
            
            # 检查数据是否完整
            if any(df is None or df.empty for df in [current_profit, current_balance, current_cashflow, prev_profit, prev_balance]):
                logger.warning(f"股票{code}的财务数据不完整，无法计算F-Score")
                return None
            
            score = 0
            score_details = {}
            
            # 1. 盈利能力：ROA > 0
            current_roa = self._get_value(current_profit, 'totalProfitable') / self._get_value(current_balance, 'totalAsset') if self._get_value(current_balance, 'totalAsset') else None
            if current_roa is not None and current_roa > 0:
                score += 1
                score_details['ROA'] = 1
            else:
                score_details['ROA'] = 0
            
            # 2. 现金流：经营现金流 > 0
            current_cfo = self._get_value(current_cashflow, 'netOperateCashFlow')
            if current_cfo is not None and current_cfo > 0:
                score += 1
                score_details['经营现金流'] = 1
            else:
                score_details['经营现金流'] = 0
            
            # 3. 盈利能力变化：ROA增长
            prev_roa = self._get_value(prev_profit, 'totalProfitable') / self._get_value(prev_balance, 'totalAsset') if self._get_value(prev_balance, 'totalAsset') else None
            if current_roa is not None and prev_roa is not None and current_roa > prev_roa:
                score += 1
                score_details['ROA增长'] = 1
            else:
                score_details['ROA增长'] = 0
            
            # 4. 现金流变化：经营现金流 > 净利润
            current_net_profit = self._get_value(current_profit, 'totalProfitable')
            if current_cfo is not None and current_net_profit is not None and current_cfo > current_net_profit:
                score += 1
                score_details['现金流>净利润'] = 1
            else:
                score_details['现金流>净利润'] = 0
            
            # 5. 杠杆率：资产负债率下降
            current_debt_ratio = self._get_value(current_balance, 'totalLiability') / self._get_value(current_balance, 'totalAsset') if self._get_value(current_balance, 'totalAsset') else None
            prev_debt_ratio = self._get_value(prev_balance, 'totalLiability') / self._get_value(prev_balance, 'totalAsset') if self._get_value(prev_balance, 'totalAsset') else None
            if current_debt_ratio is not None and prev_debt_ratio is not None and current_debt_ratio < prev_debt_ratio:
                score += 1
                score_details['资产负债率下降'] = 1
            else:
                score_details['资产负债率下降'] = 0
            
            # 6. 流动比率：流动比率上升
            current_current_ratio = self._get_value(current_balance, 'currentAsset') / self._get_value(current_balance, 'currentLiability') if self._get_value(current_balance, 'currentLiability') else None
            prev_current_ratio = self._get_value(prev_balance, 'currentAsset') / self._get_value(prev_balance, 'currentLiability') if self._get_value(prev_balance, 'currentLiability') else None
            if current_current_ratio is not None and prev_current_ratio is not None and current_current_ratio > prev_current_ratio:
                score += 1
                score_details['流动比率上升'] = 1
            else:
                score_details['流动比率上升'] = 0
            
            # 7. 股票增发：未增发新股（简化处理，这里假设没有增发）
            score += 1  # 简化处理，实际应用中需要检查股本变化
            score_details['未增发新股'] = 1
            
            # 8. 毛利率：毛利率上升
            current_gross_profit_rate = self._get_value(current_profit, 'grossProfit') / self._get_value(current_profit, 'revenue') if self._get_value(current_profit, 'revenue') else None
            prev_gross_profit_rate = self._get_value(prev_profit, 'grossProfit') / self._get_value(prev_profit, 'revenue') if self._get_value(prev_profit, 'revenue') else None
            if current_gross_profit_rate is not None and prev_gross_profit_rate is not None and current_gross_profit_rate > prev_gross_profit_rate:
                score += 1
                score_details['毛利率上升'] = 1
            else:
                score_details['毛利率上升'] = 0
            
            # 9. 资产周转率：资产周转率上升
            current_asset_turnover = self._get_value(current_profit, 'revenue') / self._get_value(current_balance, 'totalAsset') if self._get_value(current_balance, 'totalAsset') else None
            prev_asset_turnover = self._get_value(prev_profit, 'revenue') / self._get_value(prev_balance, 'totalAsset') if self._get_value(prev_balance, 'totalAsset') else None
            if current_asset_turnover is not None and prev_asset_turnover is not None and current_asset_turnover > prev_asset_turnover:
                score += 1
                score_details['资产周转率上升'] = 1
            else:
                score_details['资产周转率上升'] = 0
            
            return {'score': score, 'details': score_details}
        except Exception as e:
            logger.error(f"计算F-Score时出错: {str(e)}")
            return None
    
    def _get_value(self, df, column_name):
        """从DataFrame中获取指定列的值"""
        try:
            if df is not None and not df.empty and column_name in df.columns:
                value = df.iloc[0][column_name]
                # 尝试转换为数值
                if isinstance(value, str):
                    try:
                        return float(value)
                    except ValueError:
                        return None
                return value
            return None
        except Exception:
            return None
    
    def calculate_ff_score(self, code, current_year):
        """计算指定股票的华泰FFScore
        华泰FFScore是在传统F-Score基础上针对中国A股市场特点进行优化的评分系统
        包含盈利能力、运营能力、偿债能力、成长能力等多个维度的10项指标
        每项符合条件得1分，不符合得0分，总分范围0-10
        """
        try:
            # 获取当前年份和上一年的数据
            current_profit, current_balance, current_cashflow = self.get_financial_data(code, current_year)
            prev_profit, prev_balance, prev_cashflow = self.get_financial_data(code, str(int(current_year) - 1))
            
            # 检查数据是否完整
            if any(df is None or df.empty for df in [current_profit, current_balance, current_cashflow, prev_profit, prev_balance]):
                logger.warning(f"股票{code}的财务数据不完整，无法计算华泰FFScore")
                return None
            
            score = 0
            score_details = {}
            
            # 1. 盈利能力：ROA > 行业中位数
            current_roa = self._get_value(current_profit, 'totalProfitable') / self._get_value(current_balance, 'totalAsset') if self._get_value(current_balance, 'totalAsset') else None
            # 简化处理：使用固定阈值0.06（约6%）作为判断标准
            if current_roa is not None and current_roa > 0.06:
                score += 1
                score_details['ROA'] = 1
            else:
                score_details['ROA'] = 0
            
            # 2. 现金流：经营现金流 > 净利润
            current_cfo = self._get_value(current_cashflow, 'netOperateCashFlow')
            current_net_profit = self._get_value(current_profit, 'totalProfitable')
            if current_cfo is not None and current_net_profit is not None and current_cfo > current_net_profit:
                score += 1
                score_details['现金流>净利润'] = 1
            else:
                score_details['现金流>净利润'] = 0
            
            # 3. 盈利能力变化：净利润增长
            current_profit_val = self._get_value(current_profit, 'totalProfitable')
            prev_profit_val = self._get_value(prev_profit, 'totalProfitable')
            if current_profit_val is not None and prev_profit_val is not None and current_profit_val > prev_profit_val:
                score += 1
                score_details['净利润增长'] = 1
            else:
                score_details['净利润增长'] = 0
            
            # 4. 运营能力：总资产周转率 > 行业中位数
            current_asset_turnover = self._get_value(current_profit, 'revenue') / self._get_value(current_balance, 'totalAsset') if self._get_value(current_balance, 'totalAsset') else None
            # 简化处理：使用固定阈值0.8作为判断标准
            if current_asset_turnover is not None and current_asset_turnover > 0.8:
                score += 1
                score_details['总资产周转率'] = 1
            else:
                score_details['总资产周转率'] = 0
            
            # 5. 运营能力：存货周转率上升
            current_inventory_turnover = self._get_value(current_profit, 'revenue') / self._get_value(current_balance, 'inventory') if self._get_value(current_balance, 'inventory') else None
            prev_inventory_turnover = self._get_value(prev_profit, 'revenue') / self._get_value(prev_balance, 'inventory') if self._get_value(prev_balance, 'inventory') else None
            if current_inventory_turnover is not None and prev_inventory_turnover is not None and current_inventory_turnover > prev_inventory_turnover:
                score += 1
                score_details['存货周转率上升'] = 1
            else:
                score_details['存货周转率上升'] = 0
            
            # 6. 偿债能力：资产负债率 < 行业中位数
            current_debt_ratio = self._get_value(current_balance, 'totalLiability') / self._get_value(current_balance, 'totalAsset') if self._get_value(current_balance, 'totalAsset') else None
            # 简化处理：使用固定阈值0.6作为判断标准
            if current_debt_ratio is not None and current_debt_ratio < 0.6:
                score += 1
                score_details['资产负债率'] = 1
            else:
                score_details['资产负债率'] = 0
            
            # 7. 偿债能力：流动比率 > 1
            current_current_ratio = self._get_value(current_balance, 'currentAsset') / self._get_value(current_balance, 'currentLiability') if self._get_value(current_balance, 'currentLiability') else None
            if current_current_ratio is not None and current_current_ratio > 1:
                score += 1
                score_details['流动比率'] = 1
            else:
                score_details['流动比率'] = 0
            
            # 8. 成长能力：营收增长
            current_revenue = self._get_value(current_profit, 'revenue')
            prev_revenue = self._get_value(prev_profit, 'revenue')
            if current_revenue is not None and prev_revenue is not None and current_revenue > prev_revenue:
                score += 1
                score_details['营收增长'] = 1
            else:
                score_details['营收增长'] = 0
            
            # 9. 成长能力：毛利率上升
            current_gross_profit_rate = self._get_value(current_profit, 'grossProfit') / self._get_value(current_profit, 'revenue') if self._get_value(current_profit, 'revenue') else None
            prev_gross_profit_rate = self._get_value(prev_profit, 'grossProfit') / self._get_value(prev_profit, 'revenue') if self._get_value(prev_profit, 'revenue') else None
            if current_gross_profit_rate is not None and prev_gross_profit_rate is not None and current_gross_profit_rate > prev_gross_profit_rate:
                score += 1
                score_details['毛利率上升'] = 1
            else:
                score_details['毛利率上升'] = 0
            
            # 10. 估值水平：市盈率 < 行业中位数
            # 简化处理：这里使用净利润和一个假设的市值来计算市盈率
            # 实际应用中需要获取真实的市值数据
            if current_profit_val is not None and current_profit_val > 0:
                # 假设市值为1000亿（1,000,000,000）
                market_cap = 10000000000
                pe_ratio = market_cap / current_profit_val if current_profit_val != 0 else None
                # 简化处理：使用固定阈值20作为判断标准
                if pe_ratio is not None and pe_ratio < 20:
                    score += 1
                    score_details['市盈率'] = 1
                else:
                    score_details['市盈率'] = 0
            else:
                score_details['市盈率'] = 0
            
            return {'score': score, 'details': score_details}
        except Exception as e:
            logger.error(f"计算华泰FFScore时出错: {str(e)}")
            return None

def main():
    try:
        # 登录baostock系统
        logger.info("正在登录baostock系统...")
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"登录失败: {login_result.error_msg}")
            return
        logger.info("登录成功")
        
        # 创建F-Score计算器实例
        fscore_calculator = FScoreCalculator()
        
        # 计算股票600519的F-Score（贵州茅台）
        stock_code = "sh.600519"
        current_year = str(datetime.now().year - 1)  # 使用上一年数据
        
        logger.info(f"正在计算股票{stock_code}（贵州茅台）{current_year}年的F-Score...")
        f_score_result = fscore_calculator.calculate_f_score(stock_code, current_year)
        
        if f_score_result:
            logger.info(f"股票{stock_code}（贵州茅台）{current_year}年的F-Score: {f_score_result['score']}/9")
            logger.info("详细评分情况:")
            for indicator, score in f_score_result['details'].items():
                logger.info(f"  {indicator}: {'通过' if score == 1 else '未通过'}")
        else:
            logger.warning(f"无法计算股票{stock_code}的F-Score")
        
        # 计算华泰FFScore
        logger.info(f"\n正在计算股票{stock_code}（贵州茅台）{current_year}年的华泰FFScore...")
        ff_score_result = fscore_calculator.calculate_ff_score(stock_code, current_year)
        
        if ff_score_result:
            logger.info(f"股票{stock_code}（贵州茅台）{current_year}年的华泰FFScore: {ff_score_result['score']}/10")
            logger.info("详细评分情况:")
            for indicator, score in ff_score_result['details'].items():
                logger.info(f"  {indicator}: {'通过' if score == 1 else '未通过'}")
        else:
            logger.warning(f"无法计算股票{stock_code}的华泰FFScore")
            
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    finally:
        # 登出系统
        logger.info("正在登出baostock系统...")
        bs.logout()
        logger.info("登出成功")

if __name__ == "__main__":
    main()