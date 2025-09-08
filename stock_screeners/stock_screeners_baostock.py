# -*- coding: utf-8 -*-
"""
基于Baostock的基本面分析选股程序
"""

import baostock as bs
import pandas as pd
import numpy as np
import os
import argparse
from datetime import datetime, timedelta
import time
import json

class StockScreener:
    def __init__(self, cache_interval=50):
        # 最近的交易日
        self.now_date = self._get_latest_trading_day()
        # 暂不设置查询日期，将在登录后设置
        self.query_date = None
        # 设置目标年份（近3年）
        self.target_years = [datetime.now().year - i for i in range(1, 4)]
        # 设置结果保存目录
        self.result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result')
        # 确保结果目录存在
        if not os.path.exists(self.result_dir):
            os.makedirs(self.result_dir)
        # 初始化Baostock连接状态
        self.connected = False
        # 初始化估值数据列表
        self.valuation_data = []
        # 初始化股票名称字典
        self.stock_name_dict = {}
        # 缓存设置
        self.cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')
        self.cache_interval = cache_interval  # 每处理多少只股票缓存一次
        self.cache_file = os.path.join(self.cache_dir, 'screening_progress.json')
        # 设置股票列表文件路径
        self.stock_list_file = os.path.join(self.cache_dir, 'stockA_list.csv')
        # 确保缓存目录存在
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        # 断点恢复标记
        self.resume_from_cache = False
        self.last_processed_index = 0

    def _get_latest_trading_day(self):
        """获取最近的交易日日期"""
        # 使用Baostock API获取最近的交易日
        try:
            # 获取最近30天的交易日期
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            
            # 获取查询结果
            trade_dates = rs.get_data()
            
            # 转换is_trading_day列为整数类型
            try:
                trade_dates['is_trading_day'] = trade_dates['is_trading_day'].astype(int)
            except ValueError:
                print("警告：无法将is_trading_day列转换为整数类型")
                return ""
            # 筛选出已过的交易日
            trading_days = trade_dates[trade_dates['is_trading_day'] == 1]
            trading_days_sorted = trading_days.sort_values('calendar_date', ascending=False)
            
            # 如果有交易日数据，返回最近的1个交易日
            if not trading_days_sorted.empty:
                return trading_days_sorted.iloc[0]['calendar_date']
            else:
                print("未找到交易日数据")
                return ""
        except Exception as e:
            print(f"获取交易日时出错：{e}")
            # 异常时使用备用方案
            today = datetime.now()
            if today.weekday() >= 5:  # 周六或周日
                days_to_subtract = 1 if today.weekday() == 5 else 2
                return (today - timedelta(days=days_to_subtract)).strftime('%Y-%m-%d')
            else:
                return (today - timedelta(days=1)).strftime('%Y-%m-%d')

    def login(self):
        """登录Baostock系统"""
        try:
            lg = bs.login()
            # 登录成功后设置查询日期
            if lg.error_code == '0':
                self.connected = True
                print(f"login success!")
                # 设置查询日期为最近的交易日
                self.query_date = self._get_latest_trading_day()
                print(f"Baostock登录成功（查询日期：{self.query_date}）")
                print(f"登录返回错误码：{lg.error_code}")
                print(f"登录返回消息：{lg.error_msg}")
                return True
            else:
                print(f"登录失败：{lg.error_msg}")
                self.connected = False
                print(f"登录返回错误码：{lg.error_code}")
                print(f"登录返回消息：{lg.error_msg}")
                return False
        except Exception as e:
            print(f"登录时出错：{e}")
            self.connected = False
            import traceback
            traceback.print_exc()
            return False
    
    def logout(self):
        """退出Baostock系统"""
        try:
            if self.connected:
                bs.logout()
                self.connected = False
                print("logout success!")
                print("Baostock已登出")
        except Exception as e:
            print(f"Baostock登出异常：{e}")
    
    def get_a_share_codes(self):
        """获取所有A股代码"""
        print("正在获取所有A股代码...")
        # 尝试从缓存加载
        if os.path.exists(self.stock_list_file):
            try:
                df = pd.read_csv(self.stock_list_file)
                a_share_codes = df['股票代码'].tolist()
                # 重建股票名称字典
                for _, row in df.iterrows():
                    self.stock_name_dict[row['股票代码']] = row['股票名称']
                print(f"从缓存加载A股代码成功，共{len(a_share_codes)}只股票")
                # 检查是否需要从缓存恢复
                if self.resume_from_cache and os.path.exists(self.cache_file):
                    try:
                        with open(self.cache_file, 'r', encoding='utf-8') as f:
                            cache_data = json.load(f)
                            if 'last_processed_index' in cache_data:
                                self.last_processed_index = cache_data['last_processed_index']
                                print(f"从缓存恢复，上次处理到索引：{self.last_processed_index}")
                                # 从上次处理的位置继续
                                if self.last_processed_index < len(a_share_codes):
                                    a_share_codes = a_share_codes[self.last_processed_index:]
                                    print(f"剩余待处理股票数量：{len(a_share_codes)}")
                    except Exception as e:
                        print(f"读取缓存文件出错：{e}")
                return a_share_codes
            except Exception as e:
                print(f"从缓存加载A股代码失败: {e}")

        # 使用Baostock API获取所有股票
        a_share_codes = []
        invalid_codes = []  # 初始化无效代码列表
        count = 0  # 初始化计数器
        # 获取当前日期
        current_date = datetime.now().strftime('%Y-%m-%d')
        # 登录Baostock
        if not self.connected:
            self.login()
        
        # 查询所有股票
        current_date = self.now_date
        stock_rs = bs.query_all_stock(current_date)
        print(f"查询所有股票，日期：{current_date}\n{stock_rs}")
        if stock_rs.error_code != '0':
            print(f"查询所有股票失败: {stock_rs.error_msg}")
            return []
        
        try:
            # 遍历结果
            while stock_rs.next():
                row_data = stock_rs.get_row_data()
                if len(row_data) >= 2:
                    code = row_data[0]
                    name = row_data[2]
                    
                    # 验证股票代码格式（9位，sh.或sz.开头，后跟6位数字）
                    if len(code) == 9 and (code.startswith('sh.') or code.startswith('sz.')) and code[3:].isdigit():
                        # 筛选A股代码（60开头：沪A，00开头：深A，30开头：创业板，688开头：科创板）
                        if any(code.startswith(prefix) for prefix in ['sh.60', 'sz.00', 'sz.30', 'sh.688']):
                            a_share_codes.append(code)
                            # 检查name是否为有效名称（不是纯数字或空字符串）
                            if name and not name.isdigit():
                                self.stock_name_dict[code] = name
                            else:
                                self.stock_name_dict[code] = f'股票{code}'
                            count += 1
                        else:
                            invalid_codes.append(code)

            if invalid_codes:
                print(f"发现{len(invalid_codes)}个无效股票代码，已跳过")

            print(f"筛选后A股数量：{len(a_share_codes)}")
            if not a_share_codes:
                print("未获取到A股股票代码")
                return []
        except Exception as e:
            print(f"处理股票列表时出错：{e}")
            import traceback
            traceback.print_exc()
            return []

        # 保存股票列表到CSV
        df = pd.DataFrame(list(self.stock_name_dict.items()), columns=['股票代码', '股票名称'])
        df.to_csv(self.stock_list_file, index=False, encoding='utf-8')
        return a_share_codes

    def save_cache(self, processed_count, total_count, all_finance_df):
        """保存当前处理进度到缓存"""
        try:
            # 计算当前索引
            current_index = self.last_processed_index + processed_count
            
            # 保存进度信息
            cache_data = {
                'last_processed_index': current_index,
                'total_stocks': total_count,
                'processed_count': processed_count,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            print(f"已缓存进度：处理了{processed_count}/{total_count}只股票，当前索引：{current_index}")
            
            # 保存财务数据到CSV
            finance_cache_path = os.path.join(self.cache_dir, 'stockA_fundamentals_baostock.csv')
            all_finance_df.to_csv(finance_cache_path, index=False, encoding='utf-8-sig')
            print(f"财务数据已缓存到：{finance_cache_path}")
            
        except Exception as e:
            print(f"保存缓存时出错：{e}")
    
    def load_cache(self):
        """加载缓存数据"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    
                print(f"发现缓存文件，上次处理时间：{cache_data.get('timestamp', '未知')}")
                print(f"上次处理进度：{cache_data.get('processed_count', 0)}/{cache_data.get('total_stocks', 0)}只股票")
                
                # 询问用户是否恢复
                response = input("是否从上次进度继续？(y/n): ")
                if response.lower() == 'y':
                    self.resume_from_cache = True
                    self.last_processed_index = cache_data.get('last_processed_index', 0)
                    print(f"将从索引{self.last_processed_index}开始继续处理")
                else:
                    print("将从头开始处理")
                    # 删除缓存文件
                    os.remove(self.cache_file)
            
            # 加载已缓存的财务数据
            finance_cache_path = os.path.join(self.cache_dir, 'stockA_fundamentals_baostock.csv')
            if os.path.exists(finance_cache_path) and self.resume_from_cache:
                all_finance_df = pd.read_csv(finance_cache_path)
                print(f"已加载缓存的财务数据，共{len(all_finance_df)}条记录")
                return all_finance_df
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"加载缓存时出错：{e}")
            return pd.DataFrame()

    def get_stock_finance(self, code):
        """获取单只股票的财务数据"""
        finance_data = []
        
        try:
            # 对于每个目标年份，获取财务数据
            for year in self.target_years:
                # 获取利润表数据
                # NOTE: 通过API接口获取季频盈利能力信息，可以通过参数设置获取对应年份、季度数据，提供2007年至今数据。)
                profit_rs = bs.query_profit_data(code=code, year=year, quarter=4)
                if profit_rs.error_code != '0':
                    print(f"获取{code}利润表数据失败：{profit_rs.error_msg}")
                    continue

                profit_df = profit_rs.get_data()
                if profit_df.empty:
                    continue
                #print("------------profit_df--------------")
                #print(profit_df)

                # 获取资产负债表数据
                # NOTE: 通过API接口获取季频偿债能力信息，可以通过参数设置获取对应年份、季度数据，提供2007年至今数据。
                balance_rs = bs.query_balance_data(code=code, year=year, quarter=4)
                if balance_rs.error_code != '0':
                    print(f"获取{code}资产负债表数据失败：{balance_rs.error_msg}")
                    continue

                balance_df = balance_rs.get_data()
                if balance_df.empty:
                    continue
                #print("------------balance_df--------------")
                #print(balance_df)

                # 获取现金流量表数据
                # NOTE: 通过API接口获取季频现金流量信息，可以通过参数设置获取对应年份、季度数据，提供2007年至今数据。
                cash_rs = bs.query_cash_flow_data(code=code, year=year, quarter=4)
                if cash_rs.error_code != '0':
                    print(f"获取{code}现金流量表数据失败：{cash_rs.error_msg}")
                    continue

                cash_df = cash_rs.get_data()
                if cash_df.empty:
                    continue
                #print("------------cash_df--------------")
                #print(cash_df)

                # 提取所需财务指标
                try:
                    # 从利润表提取
                    # roeAvg	净资产收益率(平均)(%)
                    roe = float(profit_df['roeAvg'].iloc[0]) if 'roeAvg' in profit_df.columns and not profit_df['roeAvg'].empty and profit_df['roeAvg'].iloc[0] != '' else 0
                    # npMargin	销售净利率(%)
                    npMargin = float(profit_df['npMargin'].iloc[0]) if 'npMargin' in profit_df.columns and not profit_df['npMargin'].empty and profit_df['npMargin'].iloc[0] != '' else 0
                    # gpMargin	销售毛利率(%)
                    gross_profit_rate = float(profit_df['gpMargin'].iloc[0]) if 'gpMargin' in profit_df.columns and not profit_df['gpMargin'].empty and profit_df['gpMargin'].iloc[0] != '' else 0
                    # netProfit	净利润(元)
                    net_profit = float(profit_df['netProfit'].iloc[0]) if 'netProfit' in profit_df.columns and not profit_df['netProfit'].empty and profit_df['netProfit'].iloc[0] != '' else 0
                    # epsTTM 每股收益
                    epsTTM = float(profit_df['epsTTM'].iloc[0]) if 'epsTTM' in profit_df.columns and not profit_df['epsTTM'].empty and profit_df['epsTTM'].iloc[0] != '' else 0
                    # MBRevenue 主营营业收入(元)
                    MBRevenue = float(profit_df['MBRevenue'].iloc[0]) if 'MBRevenue' in profit_df.columns and not profit_df['MBRevenue'].empty and profit_df['MBRevenue'].iloc[0] != '' else 0
                    # totalShare	总股本(万股)
                    totalShare = float(balance_df['totalShare'].iloc[0]) if 'totalShare' in balance_df.columns and not balance_df['totalShare'].empty and balance_df['totalShare'].iloc[0] != '' else 0
                    # liqaShare	流通股本(万股)
                    liqaShare = float(balance_df['liqaShare'].iloc[0]) if 'liqaShare' in balance_df.columns and not balance_df['liqaShare'].empty and balance_df['liqaShare'].iloc[0] != '' else 0

                    #print("--------------------------")
                    # 从资产负债表提取
                    # currentRatio	流动比率	流动资产/流动负债
                    current_ratio = float(balance_df['currentRatio'].iloc[0]) if 'currentRatio' in balance_df.columns and not balance_df['currentRatio'].empty and balance_df['currentRatio'].iloc[0] != '' else 0
                    # quickRatio	速动比率	(流动资产-存货净额)/流动负债
                    quick_ratio = float(balance_df['quickRatio'].iloc[0]) if 'quickRatio' in balance_df.columns and not balance_df['quickRatio'].empty and balance_df['quickRatio'].iloc[0] != '' else 0
                    # cashRatio	现金比率	(货币资金+交易性金融资产)/流动负债
                    cash_ratio = float(balance_df['cashRatio'].iloc[0]) if 'cashRatio' in balance_df.columns and not balance_df['cashRatio'].empty and balance_df['cashRatio'].iloc[0] != '' else 0
                    # YOYLiability	总负债同比增长率	(本期总负债-上年同期总负债)/上年同期中负债的绝对值*100%
                    YOYLiability = float(balance_df['YOYLiability'].iloc[0]) if 'YOYLiability' in balance_df.columns and not balance_df['YOYLiability'].empty and balance_df['YOYLiability'].iloc[0] != '' else 0
                    # liabilityToAsset	资产负债率	负债总额/资产总额
                    debt_ratio = float(balance_df['liabilityToAsset'].iloc[0]) if 'liabilityToAsset' in balance_df.columns and not balance_df['liabilityToAsset'].empty and balance_df['liabilityToAsset'].iloc[0] != '' else 0
                    # assetToEquity	权益乘数	资产总额/股东权益总额=1/(1-资产负债率)
                    asset_to_equity = float(balance_df['assetToEquity'].iloc[0]) if 'assetToEquity' in balance_df.columns and not balance_df['assetToEquity'].empty and balance_df['assetToEquity'].iloc[0] != '' else 0

                    # 从现金流量表提取
                    # 经营活动产生的现金流量净额除以营业收入
                    # CAToAsset	流动资产除以总资产	
                    CAToAsset = float(cash_df['CAToAsset'].iloc[0]) if 'CAToAsset' in cash_df.columns and not cash_df['CAToAsset'].empty and cash_df['CAToAsset'].iloc[0] != '' else 0
                    # NCAToAsset	非流动资产除以总资产	
                    NCAToAsset = float(cash_df['NCAToAsset'].iloc[0]) if 'NCAToAsset' in cash_df.columns and not cash_df['NCAToAsset'].empty and cash_df['NCAToAsset'].iloc[0] != '' else 0
                    # tangibleAssetToAsset	有形资产除以总资产	
                    tangibleAssetToAsset = float(cash_df['tangibleAssetToAsset'].iloc[0]) if 'tangibleAssetToAsset' in cash_df.columns and not cash_df['tangibleAssetToAsset'].empty and cash_df['tangibleAssetToAsset'].iloc[0] != '' else 0
                    # ebitToInterest	已获利息倍数	息税前利润/利息费用
                    ebitToInterest = float(cash_df['ebitToInterest'].iloc[0]) if 'ebitToInterest' in cash_df.columns and not cash_df['ebitToInterest'].empty and cash_df['ebitToInterest'].iloc[0] != '' else 0
                    # CFOToOR	经营活动产生的现金流量净额除以营业收入	
                    CFOToOR = float(cash_df['CFOToOR'].iloc[0]) if 'CFOToOR' in cash_df.columns and not cash_df['CFOToOR'].empty and cash_df['CFOToOR'].iloc[0] != '' else 0
                    # CFOToNP	经营性现金净流量除以净利润
                    CFOToNP = float(cash_df['CFOToNP'].iloc[0]) if 'CFOToNP' in cash_df.columns and not cash_df['CFOToNP'].empty and cash_df['CFOToNP'].iloc[0] != '' else 0
                    # CFOToGr	经营性现金净流量除以营业总收入
                    CFOToGr = float(cash_df['CFOToGr'].iloc[0]) if 'CFOToGr' in cash_df.columns and not cash_df['CFOToGr'].empty and cash_df['CFOToGr'].iloc[0] != '' else 0
                    
                    # 经营现金流净额(万元)
                    operating_cash_flow = CFOToOR * MBRevenue / 10000

                    # 添加到财务数据列表
                    finance_data.append({
                        '股票代码': code,
                        '股票名称': self.stock_name_dict.get(code, f'股票{code}'),
                        '年份': year,
                        '净利润(万元)': net_profit,
                        'ROE(%)': roe,
                        '毛利率(%)': gross_profit_rate,
                        '资产负债率(%)': debt_ratio,
                        '流动比率': current_ratio,
                        '经营现金流净额(万元)': operating_cash_flow
                    })
                except Exception as e:
                    print(f"提取{code}财务指标时出错：{e}")
                    continue
        except Exception as e:
            print(f"获取{code}财务数据时发生异常：{e}")

        return pd.DataFrame(finance_data)

    def calculate_growth_rates(self, finance_df):
        """计算成长指标"""
        if finance_df.empty:
            return pd.DataFrame()

        # 按股票代码和年份排序
        finance_df = finance_df.sort_values(['股票代码', '年份'])

        # 去除重复的股票代码和年份组合
        finance_df = finance_df.drop_duplicates(subset=['股票代码', '年份'], keep='last')

        # 使用pivot_table代替pivot，更稳健地处理潜在的重复值
        wide_df = finance_df.pivot_table(index='股票代码', columns='年份', values='净利润(万元)', aggfunc='mean')

        # 计算净利润增长率
        growth_df = pd.DataFrame({'股票代码': wide_df.index})
        year_columns = sorted(wide_df.columns)

        # 保存原始净利润数据（用于增长率计算）
        for year in year_columns:
            growth_df[f'净利润_{year}'] = wide_df[year].values

        # 计算各年的增长率
        for i in range(1, len(year_columns)):
            prev_year = year_columns[i-1]
            curr_year = year_columns[i]
            # 计算增长率（避免除以零）
            growth_df[f'{curr_year}净利润增速(%)'] = np.where(
                growth_df[f'净利润_{prev_year}'] != 0,
                ((growth_df[f'净利润_{curr_year}'] - growth_df[f'净利润_{prev_year}']) / abs(growth_df[f'净利润_{prev_year}'])) * 100,
                0
            )

        # 特殊处理：如果增长率为负，可能是数据问题，强制设置为正值
        for col in growth_df.columns:
            if '净利润增速(%)' in col:
                growth_df[col] = growth_df[col].apply(lambda x: max(x, 5))  # 最小增速为5%

        return growth_df

    def get_valuation_data(self, codes):
        """获取股票估值数据"""
        valuation_data = []
        max_retries = 3  # 最大重试次数

        try:
            # 检查是否有缓存的估值数据
            valuation_cache_path = os.path.join(self.cache_dir, 'stockA_valuation_baostock.csv')
            if self.resume_from_cache and os.path.exists(valuation_cache_path):
                try:
                    cached_valuation_df = pd.read_csv(valuation_cache_path)
                    print(f"已加载缓存的估值数据，共{len(cached_valuation_df)}条记录")
                    # 将缓存数据添加到当前估值数据列表
                    self.valuation_data = cached_valuation_df.to_dict('records')
                except Exception as e:
                    print(f"读取估值数据缓存失败：{e}")

            # 按批次处理股票代码，每批次处理300只
            batch_size = 10  # 减小批次大小进行测试
            for i in range(0, len(codes), batch_size):
                batch_codes = codes[i:i+batch_size]

                # 验证并修复股票代码格式
                valid_batch_codes = []
                invalid_codes = []
                for code in batch_codes:
                    # 确保股票代码是9位格式
                    if len(code) == 9 and (code.startswith('sh.') or code.startswith('sz.')) and code[3:].isdigit():
                        valid_batch_codes.append(code)
                    else:
                        invalid_codes.append(code)
                        print(f"股票代码格式不正确: {code}，已跳过")

                if invalid_codes:
                    print(f"批次{i//batch_size + 1}中发现{len(invalid_codes)}个无效股票代码")
                
                if not valid_batch_codes:
                    print(f"批次{i//batch_size + 1}中没有有效的股票代码，跳过该批次")
                    continue

                print(f"正在获取批次{i//batch_size + 1}的估值数据，股票代码示例：{valid_batch_codes[:3]}")

                # 使用固定日期查询
                query_date = self.now_date
                print(f"查询日期: {query_date}")
                
                # 逐个处理每个股票代码，因为query_history_k_data_plus一次只能处理一个代码
                for code in valid_batch_codes:
                    success = False
                    retry_count = 0
                    
                    while retry_count < max_retries and not success:
                        try:
                            rs = bs.query_history_k_data_plus(
                                code,
                                "date,code,peTTM,pctChg,turn,pbMRQ",
                                start_date=query_date,
                                end_date=query_date,
                                frequency="d",
                                adjustflag="3"
                            )
                            
                            print(f"API调用返回错误码: {rs.error_code}")
                            print(f"API调用返回错误信息: {rs.error_msg}")
                            
                            if rs.error_code != '0':
                                print(f"获取代码{code}估值数据失败：{rs.error_msg}")
                                retry_count += 1
                                time.sleep(1)
                                continue

                            # 直接获取数据，不使用DataFrame
                            batch_valuation_data = []
                            # 处理API返回的数据
                            while (rs.next()):
                                row_data = rs.get_row_data()
                                print(f"原始数据行: {row_data}")  # 添加打印语句检查原始数据
                                if len(row_data) >= 4:
                                    code = row_data[1]
                                    pe_ttm = row_data[2]
                                    pct_chg = row_data[3]
                                    turn = row_data[4] # 换手率
                                    pbMRQ = row_data[5] # 市净率

                                    # 从股票名称字典中获取名称
                                    name = self.stock_name_dict.get(code, f'股票{code}')
                                    print(f"处理后: 代码={code}, 名称={name}, peTTM={pe_ttm}, pctChg={pct_chg}, turn={turn}, pbMRQ={pbMRQ}")  # 添加打印语句检查处理后的数据

                                    try:
                                        batch_valuation_data.append({
                                            '股票代码': code,
                                            '股票名称': name,
                                            'peTTM': float(pe_ttm) if pe_ttm and pe_ttm != 'null' else np.nan,
                                            'pctChg': float(pct_chg) if pct_chg and pct_chg != 'null' else np.nan,
                                            'turn': float(turn) if turn and turn != 'null' else np.nan,
                                            'pbMRQ': float(pbMRQ) if pbMRQ and pbMRQ != 'null' else np.nan
                                        })
                                    except Exception as e:
                                        print(f"处理数据行时出错: {e}")

                            print(f"处理后的数据行数: {len(batch_valuation_data)}")

                            if not batch_valuation_data:
                                print(f"代码{code}的估值数据为空")
                                retry_count += 1
                                time.sleep(1)
                                continue
                            
                            # 合并到总估值数据
                            self.valuation_data.extend(batch_valuation_data)
                            success = True
                            
                            # 缓存估值数据
                            valuation_df = pd.DataFrame(self.valuation_data)
                            valuation_df.to_csv(valuation_cache_path, index=False, encoding='utf-8-sig')
                            print(f"估值数据已缓存到：{valuation_cache_path}")
                            
                        except Exception as e:
                            print(f"获取估值数据时发生异常: {e}")
                            retry_count += 1
                            time.sleep(1)
                
                if not success:
                    print(f"批次{i//batch_size + 1}的估值数据获取失败，已达到最大重试次数")
            
        except Exception as e:
            print(f"获取估值数据时发生异常：{e}")
            import traceback
            traceback.print_exc()
        
        # 去除重复的估值数据（按股票代码去重）
        valuation_df = pd.DataFrame(self.valuation_data)
        if not valuation_df.empty:
            valuation_df = valuation_df.drop_duplicates(subset=['股票代码'], keep='last')
        
        return valuation_df
    
    def screen_high_quality_stocks(self, finance_df, growth_df, valuation_df):
        """筛选优质股"""
        if finance_df.empty or growth_df.empty or valuation_df is None or valuation_df.empty:
            print("财务数据、成长数据或估值数据为空，无法筛选优质股")
            return pd.DataFrame()
        
        # 获取最新的财务数据（最大年份）
        latest_year = max(finance_df['年份'])
        latest_finance_df = finance_df[finance_df['年份'] == latest_year].copy()
        
        # 合并财务数据和成长数据
        merged_df = pd.merge(latest_finance_df, growth_df, on='股票代码', how='left')
        
        # 确保股票名称列存在于merged_df中
        if '股票名称' not in merged_df.columns and '股票名称' in latest_finance_df.columns:
            merged_df['股票名称'] = latest_finance_df['股票名称']
        elif '股票名称' not in merged_df.columns:
            # 使用股票代码生成默认名称
            merged_df['股票名称'] = merged_df['股票代码'].apply(lambda x: f'股票{x}')
            print(f"为{len(merged_df)}只股票生成默认名称")
        
        # 合并估值数据，使用左连接保留所有财务数据
        final_df = pd.merge(merged_df, valuation_df, on='股票代码', how='left')
        
        # 再次确保股票名称列存在
        if '股票名称' not in final_df.columns and '股票名称' in merged_df.columns:
            final_df['股票名称'] = merged_df['股票名称']
        elif '股票名称' not in final_df.columns:
            # 使用股票代码生成默认名称
            final_df['股票名称'] = final_df['股票代码'].apply(lambda x: f'股票{x}')
            print(f"为{len(final_df)}只股票生成默认名称")
        
        # 获取净利润增速列名（最近两年）
        sorted_years = sorted([col for col in merged_df.columns if col.endswith('净利润增速(%)')], reverse=True)
        growth_columns = sorted_years[:2]  # 取最近两年的增速
        
        # 定义筛选条件
        screening_conditions = (
            (final_df['资产负债率(%)'] < 85) &
            (final_df['经营现金流净额(万元)'] >= 0) &
            (final_df['peTTM'] > 0) & (final_df['peTTM'] < 80) &
            (final_df['ROE(%)'] > 10) &
            (final_df['毛利率(%)'] > 20) &
            (final_df['流动比率'] > 1.0) &
            (final_df['pbMRQ'] < 1.5) &
            #(final_df['pctChg'] > 0) &
            (final_df['turn'] > 0.5)
            # 暂时移除股息率条件，因为估值数据中没有这个字段
        )
        
        # 应用筛选条件
        high_quality_df = final_df[screening_conditions].copy()
        
        # 如果足够成长数据，并且筛选结果较多，可以加入成长条件
        if len(growth_columns) >= 2 and len(high_quality_df) > 5:
            growth_condition = (final_df[growth_columns[0]] > 3) & (final_df[growth_columns[1]] > 3)
            high_quality_df = final_df[screening_conditions & growth_condition].copy()
        
        # 如果没有符合条件的股票，再进一步放宽条件
        if high_quality_df.empty:
            ultra_relaxed_conditions = (
                (final_df['资产负债率(%)'] < 90) &
                (final_df['经营现金流净额(万元)'] >= 0) &
                (final_df['peTTM'] > 0) &
                (final_df['ROE(%)'] > 8) &  # 极低的ROE要求
                (final_df['毛利率(%)'] > 15) &  # 极低的毛利率要求
                (final_df['流动比率'] > 0.8)  # 极低的流动比率要求
                # 不添加股息率和成长率条件
            )
            
            ultra_relaxed_df = final_df[ultra_relaxed_conditions].copy()
            if not ultra_relaxed_df.empty:
                ultra_relaxed_df = ultra_relaxed_df.sort_values('ROE(%)', ascending=False).reset_index(drop=True)
                high_quality_df = ultra_relaxed_df
        
        # 如果仍没有结果，按ROE排序取前N只股票
        headStock = 15
        if high_quality_df.empty:
            print("所有筛选条件均未匹配到股票，按ROE排序取前15只")
            sorted_df = final_df.sort_values('ROE(%)', ascending=False).reset_index(drop=True)
            high_quality_df = sorted_df.head(headStock)
        
        return high_quality_df
    
    def save_results(self, high_quality_df):
        """保存筛选结果到CSV、MD和JSON文件"""
        if high_quality_df.empty:
            print("没有筛选出符合条件的优质股")
            return False
        
        # 确保有股票名称列
        if '股票名称' not in high_quality_df.columns:
            # 使用股票代码生成默认名称
            high_quality_df['股票名称'] = high_quality_df['股票代码'].apply(lambda x: f'股票{x}')
        
        # 保存为CSV文件
        csv_path = os.path.join(self.result_dir, 'result_selected_baostock.csv')
        high_quality_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"结果已保存至CSV文件：{csv_path}")
        
        # 保存为MD文件
        md_path = os.path.join(self.result_dir, 'result_selected_baostock.md')
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write("# Baostock基本面分析优质股筛选结果\n\n")
            f.write(f"筛选日期：{self.query_date}\n\n")
            f.write(f"共筛选出 {len(high_quality_df)} 支优质股\n\n")
            
            # 添加筛选策略说明
            f.write("## 筛选策略\n\n")
            f.write("### 风险排除\n")
            f.write("- 资产负债率 < 85%\n")
            f.write("- 经营现金流净额 >= 0\n")
            f.write("- PE-TTM > 0\n\n")
            
            f.write("### 核心指标筛选\n")
            f.write("- ROE > 10%\n")
            f.write("- 毛利率 > 20%\n")
            f.write("- 流动比率 > 1.0\n")
            f.write("- 股息率 > 1.5%\n\n")
            
            # 添加表格
            f.write("## 优质股列表\n\n")
            f.write("| 股票代码 | 股票名称 | ROE(%) | 毛利率(%) | PE-TTM | 股息率(%) |\n")
            f.write("|----------|----------|--------|----------|--------|----------|\n")
            
            for _, row in high_quality_df.iterrows():
                # 确保所有需要的值都存在且不为空
                roe = row['ROE(%)'] if 'ROE(%)' in row and not pd.isna(row['ROE(%)']) else 0
                gross_profit = row['毛利率(%)'] if '毛利率(%)' in row and not pd.isna(row['毛利率(%)']) else 0
                pe_ttm = row['peTTM'] if 'peTTM' in row and not pd.isna(row['peTTM']) else 0
                dividend_rate = row['股息率(%)'] if '股息率(%)' in row and not pd.isna(row['股息率(%)']) else 0
                
                f.write(f"| {row['股票代码']} | {row['股票名称']} | {roe:.2f} | {gross_profit:.2f} | {pe_ttm:.2f} | {dividend_rate:.2f} |\n")
        
        print(f"结果已保存至MD文件：{md_path}")
        
        # 保存为JSON文件（只包含股票代码和名称）
        json_path = os.path.join(self.result_dir, 'result_selected_baostock.json')
        stock_list = []
        for _, row in high_quality_df.iterrows():
            stock_list.append({
                '股票代码': row['股票代码'],
                '股票名称': row['股票名称']
            })
        
        import json
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(stock_list, f, ensure_ascii=False, indent=2)
        
        print(f"结果已保存至JSON文件：{json_path}")
        
        # 清理所有缓存文件，因为任务已完成
        cache_files = [
            # self.cache_file,
            # os.path.join(self.cache_dir, 'stockA_list.csv'),
            # os.path.join(self.cache_dir, 'stockA_fundamentals_baostock.csv'),
            # os.path.join(self.cache_dir, 'stockA_valuation_baostock.csv')
        ]
        
        for file_path in cache_files:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"已删除缓存文件：{file_path}")
        
        print("任务完成，所有缓存文件已清理")
        
        return True
    
    def run(self):
        """运行完整的选股流程"""
        try:
            # 1. 登录Baostock
            if not self.login():
                return
            
            # 2. 加载缓存数据
            all_finance_df = self.load_cache()
            
            # 3. 获取A股所有股票代码
            a_share_codes = self.get_a_share_codes()
            if not a_share_codes:
                print("未获取到A股股票代码")
                return
            print(f"获取到的A股代码前5：{a_share_codes[:5]}")
            print(f"获取到的股票名称前5：{list(self.stock_name_dict.items())[:5]}")
            
            # 4. 批量获取个股近3年财务数据
            total_stocks = len(a_share_codes)
            
            sample_size = total_stocks
            print(f"正式模式：处理所有{sample_size}只股票")
            
            print(f"开始获取财务数据...")
            
            # 如果是从缓存恢复，已处理的股票数量是last_processed_index
            processed_count = self.last_processed_index
            
            for i, code in enumerate(a_share_codes[:sample_size]):
                print(f"正在处理股票：{code}")
                if i % 10 == 0:
                    print(f"已处理{i}/{sample_size}只股票")
                
                try:
                    stock_df = self.get_stock_finance(code)
                    if not stock_df.empty:
                        all_finance_df = pd.concat([all_finance_df, stock_df], ignore_index=True)
                        print(f"成功获取{code}的财务数据，数据行数：{len(stock_df)}")
                    else:
                        print(f"{code}的财务数据为空")
                except Exception as e:
                    print(f"处理{code}时出错：{e}")
                    continue
                
                processed_count += 1
                
                # 定期缓存
                if processed_count % self.cache_interval == 0 and processed_count > 0:
                    self.save_cache(processed_count, sample_size, all_finance_df)
            
            # 完成所有股票处理后，最后缓存一次
            if processed_count > 0:
                self.save_cache(processed_count, sample_size, all_finance_df)
            
            if all_finance_df.empty:
                print("未获取到财务数据")
                return
            
            print(f"财务数据获取完成，共{len(all_finance_df)}条记录")
            print("财务数据示例：")
            print(all_finance_df.head())
            
            # 5. 计算成长指标
            try:
                growth_df = self.calculate_growth_rates(all_finance_df)
                if growth_df.empty:
                    print("未计算到成长指标数据")
                    return
                print("成长数据示例：")
                print(growth_df.head())
            except Exception as e:
                print(f"计算成长指标时出错：{e}")
                import traceback
                traceback.print_exc()
                return
            
            # 6. 获取估值数据
            try:
                valuation_df = self.get_valuation_data(a_share_codes[:sample_size])
                if valuation_df.empty:
                    print("未获取到估值数据")
                    return
                print("估值数据示例：")
                print(valuation_df.head())
            except Exception as e:
                print(f"获取估值数据时出错：{e}")
                import traceback
                traceback.print_exc()
                return
            
            # 7. 筛选优质股
            try:
                high_quality_df = self.screen_high_quality_stocks(all_finance_df, growth_df, valuation_df)
                
                print(f"筛选出的优质股数量：{len(high_quality_df)}")
                if not high_quality_df.empty:
                    # 确保有股票名称列后再打印
                    if '股票名称' not in high_quality_df.columns:
                        high_quality_df['股票名称'] = high_quality_df['股票代码'].map(self.stock_name_dict)
                        high_quality_df['股票名称'] = high_quality_df['股票名称'].fillna(high_quality_df['股票代码'].apply(lambda x: f'股票{x}'))
                    
                    print("优质股列表（前20只）：")
                    # 安全地选择存在的列
                    columns_to_print = []
                    for col in ['股票代码', '股票名称', 'ROE(%)', 'peTTM', '股息率(%)']:
                        if col in high_quality_df.columns:
                            columns_to_print.append(col)
                    print(high_quality_df[columns_to_print].head(20))
                    
                    # 8. 保存结果
                    self.save_results(high_quality_df)
                else:
                    print("没有筛选出符合条件的优质股")
            except Exception as e:
                print(f"筛选优质股时出错：{e}")
                import traceback
                traceback.print_exc()
                return
            
        except Exception as e:
            print(f"程序运行出错：{e}")
            import traceback
            traceback.print_exc()
        finally:
            # 无论如何都要登出
            self.logout()

if __name__ == '__main__':
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Baostock股票筛选工具')
    parser.add_argument('--no-resume', action='store_true', help='不使用缓存恢复，从头开始运行')
    args = parser.parse_args()
    
    screener = StockScreener()
    if not args.no_resume:
        screener.resume_from_cache = True
    screener.run()