#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股股票基本面数据缓存系统
功能：
1. 缓存所有A股股票的完整基本面数据
2. 支持定期更新机制（默认一个月）
3. 包含README.md要求的所有数据字段
4. 支持断点续传和分批处理
"""

import os
import json
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

class StockFundamentalsCache:
    """A股股票基本面数据缓存系统"""
    
    def __init__(self):
        self.cache_dir = 'cache'
        self.fundamentals_cache = os.path.join(self.cache_dir, 'stockA_fundamentals.csv')
        self.stock_list_cache = os.path.join(self.cache_dir, 'stockA_list.csv')
        self.update_log = os.path.join(self.cache_dir, 'update_log.json')
        
        # 创建缓存目录
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        # 定义完整的数据字段
        self.required_fields = {
            # 基本信息
            'code': '股票代码',
            'name': '股票名称', 
            'list_date': '上市日期',
            'exchange': '交易所',
            'industry': '所属行业',
            'sector': '所属地区',
            'company_name': '公司名称',
            'company_industry': '公司行业',
            'company_region': '公司地区',
            'ceo': '总经理',
            'chairman': '董事长',
            'secretary': '董秘',
            'employees': '员工人数',
            
            # 财务数据
            'total_assets': '总资产(万)',
            'total_liabilities': '总负债(万)',
            'total_equity': '股东权益(万)',
            'revenue': '营业收入(万)',
            'net_profit': '净利润(万)',
            'operating_cash_flow': '经营现金流(万)',
            'eps': '每股收益(元)',
            'pe_static': '市盈率(静)',
            'pe_ttm': '市盈率(TTM)',
            'gross_margin': '毛利率(%)',
            'net_margin': '净利率(%)',
            'roe': '净资产收益率(%)',
            'debt_ratio': '资产负债率(%)',
            'revenue_growth': '营收增长率(%)',
            'profit_growth': '净利润增长率(%)',
            
            # 市场数据
            'current_price': '当前价格',
            'market_cap': '总市值(亿)',
            'pb': '市净率',
            'dividend_yield': '股息率(%)',
            
            # 更新时间
            'update_time': '更新时间'
        }

    def should_update_cache(self, days=30):
        """检查是否需要更新缓存"""
        if not os.path.exists(self.fundamentals_cache):
            return True
            
        # 检查文件修改时间
        file_time = datetime.fromtimestamp(os.path.getmtime(self.fundamentals_cache))
        if datetime.now() - file_time >= timedelta(days=days):
            return True
            
        # 检查更新日志
        if os.path.exists(self.update_log):
            try:
                with open(self.update_log, 'r', encoding='utf-8') as f:
                    log_data = json.load(f)
                    last_update = datetime.fromisoformat(log_data['last_update'])
                    if datetime.now() - last_update >= timedelta(days=days):
                        return True
            except:
                pass
                
        return False

    def get_stock_list(self):
        """获取A股股票列表"""
        if os.path.exists(self.stock_list_cache):
            print("📂 从缓存加载A股股票列表...")
            try:
                return pd.read_csv(self.stock_list_cache)
            except:
                pass
                
        print("🔄 从网络获取A股股票列表...")
        try:
            # 获取A股股票列表
            stock_list = ak.stock_zh_a_spot_em()
            stock_data = stock_list[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
            
            # 保存到缓存
            stock_data.to_csv(self.stock_list_cache, index=False, encoding='utf-8')
            print(f"✅ A股股票列表已缓存，共{len(stock_data)}只股票")
            return stock_data
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return pd.DataFrame()

    def get_company_info(self, code):
        """获取公司基本信息"""
        try:
            # 获取公司基本信息
            company_info = ak.stock_individual_info_em(symbol=code)
            
            # 获取公司高管信息
            try:
                manager_info = ak.stock_individual_executive_em(symbol=code)
                ceo = manager_info[manager_info['职位'] == '总经理']['姓名'].iloc[0] if len(manager_info) > 0 else ''
                chairman = manager_info[manager_info['职位'] == '董事长']['姓名'].iloc[0] if len(manager_info) > 0 else ''
                secretary = manager_info[manager_info['职位'] == '董秘']['姓名'].iloc[0] if len(manager_info) > 0 else ''
            except:
                ceo = chairman = secretary = ''
            
            return {
                'list_date': company_info[company_info['item'] == '上市时间']['value'].iloc[0] if len(company_info) > 0 else '',
                'exchange': company_info[company_info['item'] == '市场']['value'].iloc[0] if len(company_info) > 0 else '',
                'industry': company_info[company_info['item'] == '行业']['value'].iloc[0] if len(company_info) > 0 else '',
                'employees': company_info[company_info['item'] == '员工人数']['value'].iloc[0] if len(company_info) > 0 else '',
                'ceo': ceo,
                'chairman': chairman,
                'secretary': secretary
            }
        except:
            return {}

    def get_financial_data(self, code):
        """获取财务数据"""
        try:
            # 获取最新财务数据
            financial_data = {}
            
            # 获取财务摘要
            try:
                abstract = ak.stock_financial_abstract_ths(symbol=code)
                if not abstract.empty:
                    latest = abstract.iloc[0]
                    financial_data.update({
                        'total_assets': latest.get('资产总计', 0),
                        'total_liabilities': latest.get('负债合计', 0),
                        'total_equity': latest.get('股东权益合计', 0),
                        'revenue': latest.get('营业收入', 0),
                        'net_profit': latest.get('净利润', 0),
                        'operating_cash_flow': latest.get('经营活动产生的现金流量净额', 0)
                    })
            except:
                pass

            # 获取财务指标
            try:
                indicators = ak.stock_financial_analysis_indicator(symbol=code, indicator="年度")
                if not indicators.empty:
                    latest = indicators.iloc[0]
                    financial_data.update({
                        'eps': latest.get('每股收益', 0),
                        'roe': latest.get('净资产收益率', 0),
                        'debt_ratio': latest.get('资产负债比率', 0),
                        'gross_margin': latest.get('销售毛利率', 0),
                        'net_margin': latest.get('销售净利率', 0)
                    })
            except:
                pass

            return financial_data
        except:
            return {}

    def get_market_data(self, code):
        """获取市场数据"""
        try:
            # 获取实时行情
            spot_data = ak.stock_zh_a_spot_em()
            stock_data = spot_data[spot_data['代码'] == code]
            
            if not stock_data.empty:
                return {
                    'current_price': stock_data.iloc[0]['最新价'],
                    'market_cap': stock_data.iloc[0]['总市值'] / 10000,  # 转换为亿
                    'pe_ttm': stock_data.iloc[0]['市盈率-动态'],
                    'pb': stock_data.iloc[0]['市净率']
                }
        except:
            pass
            
        return {}

    def update_fundamentals_cache(self, batch_size=50, max_stocks=0):
        """更新基本面数据缓存"""
        
        # 获取股票列表
        stock_list = self.get_stock_list()
        if stock_list.empty:
            print("❌ 无法获取股票列表")
            return

        # 设置处理数量
        total_stocks = len(stock_list)
        if max_stocks > 0:
            total_stocks = min(max_stocks, total_stocks)
            stock_list = stock_list.head(total_stocks)

        print(f"📊 开始更新基本面数据，共{total_stocks}只股票...")

        # 断点续传
        checkpoint_file = os.path.join(self.cache_dir, 'fundamentals_update_checkpoint.json')
        processed_codes = set()
        
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    processed_codes = set(checkpoint_data.get('processed_codes', []))
                    print(f"📂 从断点续传，已处理{len(processed_codes)}只股票")
            except:
                pass

        # 分批处理
        fundamentals_data = []
        total_batches = (total_stocks + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, total_stocks)
            batch_stocks = stock_list.iloc[start_idx:end_idx]
            
            print(f"🔄 处理第{batch_idx+1}/{total_batches}批 ({start_idx+1}-{end_idx}只股票)")
            
            for _, stock in batch_stocks.iterrows():
                code = str(stock['code']).zfill(6)
                name = str(stock['name'])
                
                if code in processed_codes:
                    continue
                
                try:
                    # 收集所有数据
                    stock_data = {'code': code, 'name': name}
                    
                    # 公司信息
                    company_info = self.get_company_info(code)
                    stock_data.update(company_info)
                    
                    # 财务数据
                    financial_data = self.get_financial_data(code)
                    stock_data.update(financial_data)
                    
                    # 市场数据
                    market_data = self.get_market_data(code)
                    stock_data.update(market_data)
                    
                    # 更新时间
                    stock_data['update_time'] = datetime.now().isoformat()
                    
                    fundamentals_data.append(stock_data)
                    processed_codes.add(code)
                    
                    # 实时保存进度
                    if len(processed_codes) % batch_size == 0:
                        self._save_checkpoint(processed_codes, checkpoint_file)
                        print(f"💾 已处理{len(processed_codes)}只股票，进度已保存")
                        
                except Exception as e:
                    print(f"⚠️ 处理股票{code}失败: {e}")
                    continue
            
            # 每批完成后保存进度
            self._save_checkpoint(processed_codes, checkpoint_file)
            
            # 避免API限制
            if batch_idx < total_batches - 1:
                time.sleep(1)

        # 清理断点文件
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

        # 保存完整数据
        if fundamentals_data:
            df = pd.DataFrame(fundamentals_data)
            
            # 确保所有必需字段都存在
            for field in self.required_fields.keys():
                if field not in df.columns:
                    df[field] = None
            
            # 按字段顺序保存
            df = df[list(self.required_fields.keys())]
            df.to_csv(self.fundamentals_cache, index=False, encoding='utf-8')
            
            # 更新日志
            update_info = {
                'last_update': datetime.now().isoformat(),
                'total_stocks': len(fundamentals_data),
                'cache_file': self.fundamentals_cache
            }
            
            with open(self.update_log, 'w', encoding='utf-8') as f:
                json.dump(update_info, f, ensure_ascii=False, indent=2)
            
            print(f"✅ 基本面数据缓存更新完成，共{len(fundamentals_data)}只股票")
            print(f"📊 数据已保存到: {self.fundamentals_cache}")
            
            return df
        else:
            print("❌ 未能获取有效数据")
            return pd.DataFrame()

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

    def load_fundamentals_cache(self):
        """加载基本面数据缓存"""
        if not os.path.exists(self.fundamentals_cache):
            print("❌ 基本面数据缓存不存在")
            return pd.DataFrame()
            
        try:
            df = pd.read_csv(self.fundamentals_cache)
            print(f"📂 已加载基本面数据缓存，共{len(df)}只股票")
            return df
        except Exception as e:
            print(f"❌ 加载缓存失败: {e}")
            return pd.DataFrame()

    def get_cache_info(self):
        """获取缓存信息"""
        info = {
            'fundamentals_exists': os.path.exists(self.fundamentals_cache),
            'stock_list_exists': os.path.exists(self.stock_list_cache),
            'update_log_exists': os.path.exists(self.update_log),
            'total_fields': len(self.required_fields)
        }
        
        if os.path.exists(self.fundamentals_cache):
            info['fundamentals_size'] = os.path.getsize(self.fundamentals_cache)
            info['fundamentals_modified'] = datetime.fromtimestamp(
                os.path.getmtime(self.fundamentals_cache)).isoformat()
                
        if os.path.exists(self.update_log):
            try:
                with open(self.update_log, 'r', encoding='utf-8') as f:
                    log_data = json.load(f)
                    info.update(log_data)
            except:
                pass
                
        return info

def main():
    """主函数"""
    cache = StockFundamentalsCache()
    
    # 检查是否需要更新
    if cache.should_update_cache(days=30):
        print("🔄 需要更新基本面数据缓存")
        
        # 获取缓存信息
        info = cache.get_cache_info()
        print(f"📊 当前缓存信息: {json.dumps(info, indent=2, ensure_ascii=False)}")
        
        # 更新缓存
        df = cache.update_fundamentals_cache(
            batch_size=int(os.getenv('BATCH_SIZE', 50)),
            max_stocks=int(os.getenv('MAX_STOCKS', 0))
        )
        
        if not df.empty:
            print(f"✅ 更新完成，共{len(df)}只股票")
            print("📊 数据预览:")
            print(df.head())
    else:
        print("✅ 缓存数据在有效期内，无需更新")
        
        # 加载现有缓存
        df = cache.load_fundamentals_cache()
        if not df.empty:
            print("📊 缓存数据概览:")
            print(f"- 总股票数: {len(df)}")
            print(f"- 数据字段: {len(df.columns)}")
            print(f"- 最后更新: {df['update_time'].max() if 'update_time' in df.columns else '未知'}")

if __name__ == "__main__":
    main()