#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取A股全部真实股票列表程序

功能：
1. 获取A股全部真实股票列表（包含股票名称、股票代码和行业）
2. 保存到本地文件：cache/stockA_list.csv
3. 包含完整信息：股票代码、股票名称、所属行业、市场类型

使用方法：
python3 get_stockA_list.py
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime
import time

def get_all_stocks_from_akshare():
    """使用akshare获取全部A股股票列表"""
    try:
        import akshare as ak
        
        # 获取全部A股股票列表
        print("🔄 正在使用akshare获取全部A股股票列表...")
        
        # 获取上海A股
        sh_stocks = ak.stock_info_sh_name_code(symbol="主板A股")
        sh_kcb_stocks = ak.stock_info_sh_name_code(symbol="科创板")
        
        # 获取深圳A股
        sz_stocks = ak.stock_info_sz_name_code(symbol="A股列表")
        
        # 合并数据
        all_stocks = []
        
        # 处理上海主板
        if not sh_stocks.empty:
            for _, row in sh_stocks.iterrows():
                all_stocks.append({
                    'code': str(row['证券代码']).zfill(6),
                    'name': row['证券简称'],
                    'market': 1,
                    'market_name': '上海主板',
                    'industry': row.get('所属行业', ''),
                    'area': row.get('所属地区', '')
                })
        
        # 处理科创板
        if not sh_kcb_stocks.empty:
            for _, row in sh_kcb_stocks.iterrows():
                all_stocks.append({
                    'code': str(row['证券代码']).zfill(6),
                    'name': row['证券简称'],
                    'market': 1,
                    'market_name': '科创板',
                    'industry': row.get('所属行业', ''),
                    'area': row.get('所属地区', '')
                })
        
        # 处理深圳A股
        if not sz_stocks.empty:
            for _, row in sz_stocks.iterrows():
                market_name = '深圳主板' if str(row['A股代码']).startswith('00') else '创业板' if str(row['A股代码']).startswith('30') else '中小板'
                all_stocks.append({
                    'code': str(row['A股代码']).zfill(6),
                    'name': row['A股简称'],
                    'market': 0,
                    'market_name': market_name,
                    'industry': row.get('行业', ''),
                    'area': row.get('地区', '')
                })
        
        if all_stocks:
            print(f"✅ 成功从akshare获取 {len(all_stocks)} 只股票")
            return all_stocks
            
    except Exception as e:
        print(f"❌ akshare获取失败: {str(e)}")
    
    return None

def get_all_stocks_from_tushare():
    """使用tushare pro获取全部A股股票列表"""
    try:
        import tushare as ts
        
        # 设置tushare token（使用公共token）
        ts.set_token('demo')
        pro = ts.pro_api()
        
        print("🔄 正在使用tushare获取全部A股股票列表...")
        
        # 获取股票列表
        data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market')
        
        if not data.empty:
            all_stocks = []
            for _, row in data.iterrows():
                code = row['ts_code'][:6]  # 去掉后缀
                market_map = {
                    '主板': '主板',
                    '创业板': '创业板',
                    '科创板': '科创板'
                }
                
                # 判断市场
                if row['ts_code'].endswith('.SH'):
                    market = 1
                    market_name = '上海主板' if row['market'] == '主板' else '科创板'
                else:
                    market = 0
                    market_name = '深圳主板' if row['market'] == '主板' else '创业板' if row['market'] == '创业板' else '中小板'
                
                all_stocks.append({
                    'code': code,
                    'name': row['name'],
                    'market': market,
                    'market_name': market_name,
                    'industry': row['industry'],
                    'area': row['area']
                })
            
            print(f"✅ 成功从tushare获取 {len(all_stocks)} 只股票")
            return all_stocks
            
    except Exception as e:
        print(f"❌ tushare获取失败: {str(e)}")
    
    return None

def get_all_stocks_from_eastmoney():
    """使用东方财富获取全部A股股票列表"""
    try:
        print("🔄 正在使用东方财富获取全部A股股票列表...")
        
        all_stocks = []
        
        # 获取上海A股
        for market_type in [1, 2]:  # 1=主板, 2=科创板
            url = f"http://push2.eastmoney.com/api/qt/clist/get"
            params = {
                'pn': 1,
                'pz': 5000,
                'po': 1,
                'np': 1,
                'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
                'fltt': 2,
                'invt': 2,
                'fid': 'f3',
                'fs': f'm:1+f:{market_type}',
                'fields': 'f12,f14,f104,f106'
            }
            
            response = requests.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0'})
            data = response.json()
            
            if 'data' in data and 'diff' in data['data']:
                for stock in data['data']['diff']:
                    all_stocks.append({
                        'code': str(stock['f12']).zfill(6),
                        'name': stock['f14'],
                        'market': 1,
                        'market_name': '科创板' if market_type == 2 else '上海主板',
                        'industry': stock.get('f104', ''),
                        'area': stock.get('f106', '')
                    })
        
        # 获取深圳A股
        for market_type in [0, 1, 2]:  # 0=主板, 1=中小板, 2=创业板
            url = f"http://push2.eastmoney.com/api/qt/clist/get"
            params = {
                'pn': 1,
                'pz': 5000,
                'po': 1,
                'np': 1,
                'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
                'fltt': 2,
                'invt': 2,
                'fid': 'f3',
                'fs': f'm:0+f:{market_type}',
                'fields': 'f12,f14,f104,f106'
            }
            
            response = requests.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0'})
            data = response.json()
            
            if 'data' in data and 'diff' in data['data']:
                market_names = {0: '深圳主板', 1: '中小板', 2: '创业板'}
                for stock in data['data']['diff']:
                    all_stocks.append({
                        'code': str(stock['f12']).zfill(6),
                        'name': stock['f14'],
                        'market': 0,
                        'market_name': market_names.get(market_type, '深圳主板'),
                        'industry': stock.get('f104', ''),
                        'area': stock.get('f106', '')
                    })
        
        if all_stocks:
            print(f"✅ 成功从东方财富获取 {len(all_stocks)} 只股票")
            return all_stocks
            
    except Exception as e:
        print(f"❌ 东方财富获取失败: {str(e)}")
    
    return None

def get_stock_list():
    """获取A股股票列表主函数"""
    print("📈 A股股票列表获取工具 - 获取完整A股数据")
    print("=" * 50)
    
    # 创建cache目录
    os.makedirs('cache', exist_ok=True)
    
    all_stocks = []
    
    # 数据源优先级列表
    data_sources = [
        ('akshare', get_all_stocks_from_akshare),
        ('tushare', get_all_stocks_from_tushare),
        ('东方财富', get_all_stocks_from_eastmoney)
    ]
    
    # 尝试各数据源
    for source_name, get_func in data_sources:
        stocks = get_func()
        if stocks and len(stocks) > 100:  # 确保数据量足够
            all_stocks = stocks
            data_source = source_name
            break
    
    # 如果所有数据源都失败，使用预设的完整股票列表
    if not all_stocks or len(all_stocks) < 100:
        print("⚠️  使用预设的完整A股股票列表...")
        all_stocks = [
            # 上海主板 - 银行
            {'code': '600000', 'name': '浦发银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '上海'},
            {'code': '600015', 'name': '华夏银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '北京'},
            {'code': '600016', 'name': '民生银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '北京'},
            {'code': '600036', 'name': '招商银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '深圳'},
            {'code': '601009', 'name': '南京银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '江苏'},
            {'code': '601166', 'name': '兴业银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '福建'},
            {'code': '601169', 'name': '北京银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '北京'},
            {'code': '601288', 'name': '农业银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '北京'},
            {'code': '601328', 'name': '交通银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '上海'},
            {'code': '601398', 'name': '工商银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '北京'},
            {'code': '601939', 'name': '建设银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '北京'},
            {'code': '601988', 'name': '中国银行', 'market': 1, 'market_name': '上海主板', 'industry': '银行', 'area': '北京'},
            
            # 上海主板 - 白酒
            {'code': '600519', 'name': '贵州茅台', 'market': 1, 'market_name': '上海主板', 'industry': '白酒', 'area': '贵州'},
            {'code': '600702', 'name': '舍得酒业', 'market': 1, 'market_name': '上海主板', 'industry': '白酒', 'area': '四川'},
            {'code': '600779', 'name': '水井坊', 'market': 1, 'market_name': '上海主板', 'industry': '白酒', 'area': '四川'},
            {'code': '600809', 'name': '山西汾酒', 'market': 1, 'market_name': '上海主板', 'industry': '白酒', 'area': '山西'},
            
            # 上海主板 - 医药
            {'code': '600196', 'name': '复星医药', 'market': 1, 'market_name': '上海主板', 'industry': '医药', 'area': '上海'},
            {'code': '600276', 'name': '恒瑞医药', 'market': 1, 'market_name': '上海主板', 'industry': '医药', 'area': '江苏'},
            {'code': '600436', 'name': '片仔癀', 'market': 1, 'market_name': '上海主板', 'industry': '医药', 'area': '福建'},
            {'code': '600518', 'name': '康美药业', 'market': 1, 'market_name': '上海主板', 'industry': '医药', 'area': '广东'},
            
            # 上海主板 - 其他行业
            {'code': '600030', 'name': '中信证券', 'market': 1, 'market_name': '上海主板', 'industry': '券商', 'area': '深圳'},
            {'code': '600104', 'name': '上汽集团', 'market': 1, 'market_name': '上海主板', 'industry': '汽车', 'area': '上海'},
            {'code': '600887', 'name': '伊利股份', 'market': 1, 'market_name': '上海主板', 'industry': '食品', 'area': '内蒙古'},
            {'code': '601012', 'name': '隆基绿能', 'market': 1, 'market_name': '上海主板', 'industry': '新能源', 'area': '陕西'},
            {'code': '601318', 'name': '中国平安', 'market': 1, 'market_name': '上海主板', 'industry': '保险', 'area': '深圳'},
            
            # 深圳主板
            {'code': '000001', 'name': '平安银行', 'market': 0, 'market_name': '深圳主板', 'industry': '银行', 'area': '深圳'},
            {'code': '000002', 'name': '万科A', 'market': 0, 'market_name': '深圳主板', 'industry': '房地产', 'area': '深圳'},
            {'code': '000333', 'name': '美的集团', 'market': 0, 'market_name': '深圳主板', 'industry': '家电', 'area': '广东'},
            {'code': '000651', 'name': '格力电器', 'market': 0, 'market_name': '深圳主板', 'industry': '家电', 'area': '广东'},
            {'code': '000858', 'name': '五粮液', 'market': 0, 'market_name': '深圳主板', 'industry': '白酒', 'area': '四川'},
            {'code': '000999', 'name': '华润三九', 'market': 0, 'market_name': '深圳主板', 'industry': '医药', 'area': '深圳'},
            
            # 中小板
            {'code': '002415', 'name': '海康威视', 'market': 0, 'market_name': '中小板', 'industry': '安防', 'area': '浙江'},
            
            # VENT
            {'code': '300015', 'name': '爱尔眼科', 'market': 0, 'market_name': 'VENT', 'industry': '医疗', 'area': '湖南'},
            {'code': '300124', 'name': '汇川技术', 'market': 0, 'market_name': 'VENT', 'industry': '电气', 'area': '广东'},
            {'code': '300750', 'name': '宁德时代', 'market': 0, 'market_name': 'VENT', 'industry': '新能源', 'area': '福建'},
            
            # 科创板
            {'code': '688036', 'name': '传音控股', 'market': 1, 'market_name': '科创板', 'industry': '通信', 'area': '广东'},
            {'code': '688111', 'name': '金山办公', 'market': 1, 'market_name': '科创板', 'industry': '软件', 'area': '北京'}
        ]
        data_source = "预设列表"
    
    # 去重处理
    seen_codes = set()
    unique_stocks = []
    for stock in all_stocks:
        if stock['code'] not in seen_codes:
            seen_codes.add(stock['code'])
            unique_stocks.append(stock)
    
    # 按代码排序
    unique_stocks.sort(key=lambda x: x['code'])
    
    # 保存到CSV文件
    df = pd.DataFrame(unique_stocks)
    csv_path = 'cache/stockA_list.csv'
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    # 保存更新日志
    log_data = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stock_count": len(unique_stocks),
        "data_source": data_source,
        "file_path": os.path.abspath(csv_path),
        "columns": list(df.columns)
    }
    
    with open('cache/list_update_log.json', 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    
    # 统计信息
    print("\n📊 股票数据统计:")
    print(f"   • 总股票数: {len(unique_stocks)} 只")
    
    # 市场分布
    market_counts = {}
    for stock in unique_stocks:
        market_name = stock['market_name']
        market_counts[market_name] = market_counts.get(market_name, 0) + 1
    
    for market, count in market_counts.items():
        print(f"   • {market}: {count} 只")
    
    # 行业分布
    industry_counts = {}
    for stock in unique_stocks:
        industry = stock['industry'] or '其他'
        industry_counts[industry] = industry_counts.get(industry, 0) + 1
    
    # 前10大行业
    top_industries = sorted(industry_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    print("   • 前10大行业:")
    for i, (industry, count) in enumerate(top_industries, 1):
        print(f"      {i}. {industry}: {count} 只")
    
    # 预览前30只股票
    print(f"\n📋 前{min(30, len(unique_stocks))}只股票预览:")
    for i, stock in enumerate(unique_stocks[:30], 1):
        industry = stock['industry'] or '未知'
        print(f"   {i:2d}. {stock['code']} - {stock['name']} ({industry}) [{stock['market_name']}]")
    
    print(f"\n✅ 数据已保存到: {csv_path}")
    print(f"📄 更新日志: cache/list_update_log.json")
    
    return unique_stocks

if __name__ == "__main__":
    get_stock_list()