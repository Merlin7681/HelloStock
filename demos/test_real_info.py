#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取A股股票实时行情信息
"""

import akshare as ak
import pandas as pd
import warnings
import time
warnings.filterwarnings('ignore')
from datetime import datetime


def get_stock_real_time_info(stock_code):
    """
    获取指定股票代码的实时行情信息
    
    参数:
    stock_code: str, 6位股票代码，如'000001'
    
    返回:
    dict: 包含股票名称、当前价格等实时行情信息的字典
          如果获取失败，返回None
    """
    # 确保股票代码格式正确
    if not isinstance(stock_code, str) or len(stock_code) != 6:
        print(f"❌ 股票代码格式错误: {stock_code}，请输入6位数字代码")
        return None
    
    print(f"📡 开始获取股票 {stock_code} 实时行情信息...")
    
    # 尝试多种数据源获取实时行情
    # 方法1: 东方财富实时行情
    try:
        print("🔍 尝试从东方财富获取数据...")
        stock_info = ak.stock_zh_a_spot_em()
        stock_data = stock_info[stock_info['代码'] == stock_code]
        
        if not stock_data.empty:
            result = {
                '股票代码': stock_data.iloc[0]['代码'],
                '股票名称': stock_data.iloc[0]['名称'],
                '最新价': stock_data.iloc[0]['最新价'],
                '涨跌额': stock_data.iloc[0]['涨跌额'],
                '涨跌幅': stock_data.iloc[0]['涨跌幅'],
                '开盘价': stock_data.iloc[0]['开盘价'],
                '最高价': stock_data.iloc[0]['最高价'],
                '最低价': stock_data.iloc[0]['最低价'],
                '成交量': stock_data.iloc[0]['成交量'],
                '成交额': stock_data.iloc[0]['成交额'],
                '换手率': stock_data.iloc[0]['换手率'],
                '市盈率': stock_data.iloc[0]['市盈率-动态'],
                '获取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '数据源': '东方财富'
            }
            print(f"✅ 成功从{result['数据源']}获取{result['股票名称']}({result['股票代码']})实时行情")
            return result
    except Exception as e:
        print(f"❌ 东方财富数据源出错: {str(e)}")
    
    # 方法2: 尝试通用实时行情接口
    try:
        print("🔍 尝试获取沪深A股实时行情...")
        # 获取股票代码全称 (沪深A股)
        market = 'sh' if stock_code.startswith('6') else 'sz'
        full_code = f"{market}{stock_code}"
        
        try:
            # 尝试使用通用A股实时行情接口
            real_time_quote = ak.stock_zh_a_spot()
            stock_data = real_time_quote[real_time_quote['代码'] == stock_code]
            
            if not stock_data.empty:
                # 适配不同接口的数据结构
                result = {
                    '股票代码': stock_code,
                    '股票名称': stock_data.iloc[0]['名称'] if '名称' in stock_data.columns else f"股票{stock_code}",
                    '最新价': stock_data.iloc[0]['现价'] if '现价' in stock_data.columns else \
                             (stock_data.iloc[0]['最新价'] if '最新价' in stock_data.columns else 'N/A'),
                    '涨跌额': stock_data.iloc[0]['涨跌额'] if '涨跌额' in stock_data.columns else 'N/A',
                    '涨跌幅': stock_data.iloc[0]['涨跌幅'] if '涨跌幅' in stock_data.columns else 'N/A',
                    '开盘价': stock_data.iloc[0]['今开'] if '今开' in stock_data.columns else 'N/A',
                    '最高价': stock_data.iloc[0]['最高'] if '最高' in stock_data.columns else 'N/A',
                    '最低价': stock_data.iloc[0]['最低'] if '最低' in stock_data.columns else 'N/A',
                    '成交量': stock_data.iloc[0]['成交量'] if '成交量' in stock_data.columns else 'N/A',
                    '成交额': stock_data.iloc[0]['成交额'] if '成交额' in stock_data.columns else 'N/A',
                    '获取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '数据源': '沪深A股实时行情'
                }
                print(f"✅ 成功获取{result['股票名称']}({result['股票代码']})实时行情")
                return result
            else:
                print(f"  └─ 未找到股票代码 {stock_code} 的实时行情数据")
        except Exception as inner_e:
            print(f"  └─ 沪深A股实时行情接口失败: {str(inner_e)}")
    except Exception as e:
        print(f"❌ 实时行情获取出错: {str(e)}")
    
    # 方法3: 尝试股票基本面数据接口
    try:
        print("🔍 尝试获取股票基本面数据...")
        time.sleep(0.5)  # 避免请求过于频繁
        
        try:
            # 尝试获取股票实时行情（单只股票）
            stock_profile = ak.stock_individual_info_em(symbol=stock_code)
            if stock_profile is not None and len(stock_profile) > 0:
                # 获取公司名称
                stock_name = f"股票{stock_code}"
                # 构建基础信息结果
                result = {
                    '股票代码': stock_code,
                    '股票名称': stock_name,
                    '最新价': 'N/A',
                    '涨跌额': 'N/A',
                    '涨跌幅': 'N/A',
                    '开盘价': 'N/A',
                    '最高价': 'N/A',
                    '最低价': 'N/A',
                    '成交量': 'N/A',
                    '成交额': 'N/A',
                    '获取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '数据源': '股票基本面数据'
                }
                print(f"✅ 成功获取股票{stock_code}基本面数据")
                return result
        except Exception as inner_e:
            print(f"  └─ 基本面数据接口失败: {str(inner_e)}")
    except Exception as e:
        print(f"❌ 股票基本面数据获取出错: {str(e)}")
    
    # 方法4: 尝试从新浪接口获取数据
    try:
        print("🔍 尝试从新浪数据源获取数据...")
        time.sleep(0.5)  # 避免请求过于频繁
        
        try:
            # 尝试获取新浪股票实时行情
            sina_df = ak.stock_zh_a_spot_sina(symbol=stock_code)
            if not sina_df.empty:
                result = {
                    '股票代码': stock_code,
                    '股票名称': sina_df['名称'].iloc[0] if '名称' in sina_df.columns else f"股票{stock_code}",
                    '最新价': sina_df['最新价'].iloc[0] if '最新价' in sina_df.columns else 'N/A',
                    '涨跌额': sina_df['涨跌额'].iloc[0] if '涨跌额' in sina_df.columns else 'N/A',
                    '涨跌幅': sina_df['涨跌幅'].iloc[0] if '涨跌幅' in sina_df.columns else 'N/A',
                    '开盘价': sina_df['今开'].iloc[0] if '今开' in sina_df.columns else 'N/A',
                    '最高价': sina_df['最高'].iloc[0] if '最高' in sina_df.columns else 'N/A',
                    '最低价': sina_df['最低'].iloc[0] if '最低' in sina_df.columns else 'N/A',
                    '成交量': sina_df['成交量'].iloc[0] if '成交量' in sina_df.columns else 'N/A',
                    '成交额': sina_df['成交额'].iloc[0] if '成交额' in sina_df.columns else 'N/A',
                    '获取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '数据源': '新浪行情'
                }
                print(f"✅ 成功从新浪获取{result['股票名称']}({result['股票代码']})实时行情")
                return result
        except Exception as inner_e:
            print(f"  └─ 新浪数据源接口失败: {str(inner_e)}")
    except Exception as e:
        print(f"❌ 新浪数据源获取出错: {str(e)}")
    
    # 方法4: 获取历史数据作为备选
    try:
        print("🔍 尝试获取历史数据作为备选...")
        hist_data = ak.stock_zh_a_hist(
            symbol=stock_code, 
            period="daily", 
            start_date=(datetime.now() - pd.Timedelta(days=30)).strftime('%Y%m%d'),
            end_date=datetime.now().strftime('%Y%m%d'),
            adjust=""
        )
        
        if not hist_data.empty:
            latest_data = hist_data.iloc[-1]
            result = {
                '股票代码': stock_code,
                '股票名称': f"股票{stock_code}",  # 历史数据不包含名称
                '最新价': latest_data['收盘'],
                '涨跌额': latest_data['涨跌额'] if '涨跌额' in latest_data else 'N/A',
                '涨跌幅': latest_data['涨跌幅'] if '涨跌幅' in latest_data else 'N/A',
                '开盘价': latest_data['开盘'],
                '最高价': latest_data['最高'],
                '最低价': latest_data['最低'],
                '成交量': latest_data['成交量'],
                '成交额': latest_data['成交额'],
                '获取时间': latest_data['日期'],
                '数据源': '历史数据'
            }
            print(f"✅ 成功获取{result['股票名称']}({result['股票代码']})历史数据")
            return result
    except Exception as e:
        print(f"❌ 历史数据获取出错: {str(e)}")
    
    print(f"❌ 所有数据源均获取失败")
    return None


def display_stock_info(stock_info):
    """
    格式化显示股票行情信息
    
    参数:
    stock_info: dict, 包含股票行情信息的字典
    """
    if not stock_info:
        print("❌ 无股票信息可显示")
        return
    
    print("\n📊 股票实时行情信息")
    print("=" * 60)
    print(f"股票名称: {stock_info['股票名称']} ({stock_info['股票代码']})")
    
    # 安全格式化价格信息
    def safe_format_price(value, prefix="¥"):
        if value == 'N/A':
            return f"{prefix}N/A"
        try:
            float_value = float(value)
            return f"{prefix}{float_value:.2f}"
        except:
            return f"{prefix}{value}"
    
    # 格式化最新价格
    print(f"最新价格: {safe_format_price(stock_info['最新价'])}")
    
    # 格式化涨跌信息
    change_info = stock_info.get('涨跌额', 'N/A')
    change_percent = stock_info.get('涨跌幅', 'N/A')
    if change_info != 'N/A' and change_percent != 'N/A':
        try:
            change_val = float(change_info)
            if change_val > 0:
                print(f"涨跌情况: +{change_info} ({change_percent})")
            else:
                print(f"涨跌情况: {change_info} ({change_percent})")
        except:
            print(f"涨跌情况: {change_info} ({change_percent})")
    else:
        print(f"涨跌情况: {change_info} ({change_percent})")
    
    # 格式化其他行情信息
    print(f"今日开: {safe_format_price(stock_info.get('开盘价', 'N/A'))}")
    print(f"最高: {safe_format_price(stock_info.get('最高价', 'N/A'))}")
    print(f"最低: {safe_format_price(stock_info.get('最低价', 'N/A'))}")
    
    # 格式化成交量（千位分隔符）
    volume = stock_info.get('成交量', 'N/A')
    if volume != 'N/A':
        try:
            print(f"成交量: {int(volume):,}")
        except:
            print(f"成交量: {volume}")
    else:
        print(f"成交量: {volume}")
    
    # 格式化成交额（转换为亿元或万元）
    amount = stock_info.get('成交额', 'N/A')
    if amount != 'N/A':
        try:
            amount_value = float(amount)
            if amount_value >= 100000000:
                print(f"成交额: {amount_value/100000000:.2f}亿")
            else:
                print(f"成交额: {amount_value/10000:.2f}万")
        except:
            print(f"成交额: {amount}")
    else:
        print(f"成交额: {amount}")
    
    # 显示换手率和市盈率
    if '换手率' in stock_info and stock_info['换手率'] != 'N/A':
        print(f"换手率: {stock_info['换手率']}%")
    if '市盈率' in stock_info and stock_info['市盈率'] != 'N/A':
        print(f"市盈率: {stock_info['市盈率']}")
    
    print(f"数据时间: {stock_info['获取时间']}")
    print(f"数据源: {stock_info['数据源']}")
    print("=" * 60)


if __name__ == "__main__":
    """
    主函数，支持命令行参数传入股票代码
    """
    import sys
    
    # 默认测试股票代码
    test_stock_codes = ['000001', '600519', '000858']
    
    if len(sys.argv) > 1:
        # 从命令行获取股票代码
        target_stock_code = sys.argv[1]
        info = get_stock_real_time_info(target_stock_code)
        if info:
            display_stock_info(info)
    else:
        # 使用默认测试股票代码
        print("📋 没有指定股票代码，将使用默认测试股票代码")
        for stock_code in test_stock_codes:
            info = get_stock_real_time_info(stock_code)
            if info:
                display_stock_info(info)
                print()  # 空行分隔不同股票

        print("💡 使用提示：")
        print("  python test_real_info.py <股票代码>")
        print("  示例: python test_real_info.py 000001")