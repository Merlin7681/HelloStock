import pandas as pd
import numpy as np
import time
import json
import os
import requests
from datetime import datetime
import warnings
import akshare as ak

try:
    import baostock as bs
    BAOSTOCK_AVAILABLE = True
except ImportError:
    BAOSTOCK_AVAILABLE = False
    print("⚠️  baostock库未安装，运行 `pip install baostock` 以启用baostock数据源")

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
    progress_file = 'cache/fundamentals_progress.json'
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {"last_index": 0, "completed_codes": []}

def save_progress(index, completed_codes):
    """保存进度信息"""
    progress_file = 'cache/fundamentals_progress.json'
    try:
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump({"last_index": index, "completed_codes": completed_codes}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  保存进度失败: {e}")

def get_fundamentals_from_eastmoney(code):
    """使用东方财富API获取完整基本面数据"""
    try:
        # 东方财富API基础URL
        base_url = "http://push2.eastmoney.com/api"
        
        # 获取股票基本信息
        url = f"{base_url}/qt/stock/get"
        params = {
            'secid': f"0.{code}" if str(code).startswith(('0', '3')) else f"1.{code}",
            'fields': 'f43,f44,f45,f46,f48,f49,f50,f51,f52,f57,f58,f60,f62,f84,f85,f116,f117,f162,f163,f164,f167,f168,f169,f170,f171,f172,f173,f174,f175,f176,f177,f178,f184,f185,f186,f187,f188,f189,f190,f191,f277'
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if 'data' not in data:
            return None
            
        stock_data = data['data']
        
        # 获取财务数据 - 使用更完整的字段映射
        try:
            # 获取详细的财务指标
            financial_data = {
                # 盈利能力指标
                '每股收益': stock_data.get('f162', ''),
                '每股净资产': stock_data.get('f173', ''),
                '净资产收益率': stock_data.get('f177', ''),  # ROE
                '总资产收益率': stock_data.get('f178', ''),  # ROA
                '毛利率': stock_data.get('f184', ''),
                '净利率': stock_data.get('f185', ''),
                '营业利润率': stock_data.get('f186', ''),
                
                # 估值指标
                '市盈率（静）': stock_data.get('f163', ''),
                '市盈率（TTM）': stock_data.get('f164', ''),
                '市净率': stock_data.get('f167', ''),
                '市销率': stock_data.get('f168', ''),
                '股息率': stock_data.get('f188', ''),
                
                # 成长性指标
                '营业收入增长率': stock_data.get('f190', ''),
                '净利润增长率': stock_data.get('f191', ''),
                '净资产增长率': stock_data.get('f189', ''),
                
                # 偿债能力指标
                '资产负债率': stock_data.get('f116', ''),
                '流动比率': stock_data.get('f277', ''),
                
                # 运营能力指标
                '总资产周转率': stock_data.get('f187', ''),
                '存货周转率': stock_data.get('f174', ''),
                '应收账款周转率': stock_data.get('f175', ''),
                
                # 现金流指标
                '经营现金流净额': '',
                '每股经营现金流': '',
                '现金流量比率': ''
            }
            
        except Exception as e:
            print(f"⚠️  获取财务数据时出错: {e}")
            financial_data = {}
        
        # 获取行业和上市信息
        try:
            # 获取股票详细信息
            detail_url = "http://emweb.securities.eastmoney.com/PC_HSF10/CoreConception/Index"
            detail_params = {
                'type': 'web',
                'code': code,
                'rt': str(int(time.time() * 1000))
            }
            
            # 这里简化处理，实际需要从页面解析
            industry = ''
            ipo_date = ''
            
        except:
            industry = ''
            ipo_date = ''
        
        fundamental = {
            '股票代码': code,
            '股票名称': str(stock_data.get('f58', '')).strip(),
            '股票上市日期': ipo_date,
            '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
            '股票所属行业': industry,
            
            # 盈利能力
            '每股收益': str(financial_data.get('每股收益', '')).strip(),
            '每股净资产': str(financial_data.get('每股净资产', '')).strip(),
            '净资产收益率': str(financial_data.get('净资产收益率', '')).strip(),
            '总资产收益率': str(financial_data.get('总资产收益率', '')).strip(),
            '毛利率': str(financial_data.get('毛利率', '')).strip(),
            '净利率': str(financial_data.get('净利率', '')).strip(),
            '营业利润率': str(financial_data.get('营业利润率', '')).strip(),
            
            # 估值指标
            '市盈率（静）': str(financial_data.get('市盈率（静）', '')).strip(),
            '市盈率（TTM）': str(financial_data.get('市盈率（TTM）', '')).strip(),
            '市净率': str(financial_data.get('市净率', '')).strip(),
            '市销率': str(financial_data.get('市销率', '')).strip(),
            '股息率': str(financial_data.get('股息率', '')).strip(),
            
            # 成长性
            '营业收入增长率': str(financial_data.get('营业收入增长率', '')).strip(),
            '净利润增长率': str(financial_data.get('净利润增长率', '')).strip(),
            '净资产增长率': str(financial_data.get('净资产增长率', '')).strip(),
            '净利润增速': str(financial_data.get('净利润增长率', '')).strip(),  # 兼容旧字段
            
            # 偿债能力
            '资产负债率': str(financial_data.get('资产负债率', '')).strip(),
            '流动比率': str(financial_data.get('流动比率', '')).strip(),
            
            # 运营能力
            '总资产周转率': str(financial_data.get('总资产周转率', '')).strip(),
            '存货周转率': str(financial_data.get('存货周转率', '')).strip(),
            '应收账款周转率': str(financial_data.get('应收账款周转率', '')).strip(),
            
            # 现金流
            '每股经营现金流': '',
            '现金流量比率': ''
        }
        
        return fundamental
        
    except Exception as e:
        print(f"❌ 东方财富API获取 {code} 数据失败: {str(e)}")
        return None

def get_fundamentals_from_baostock(code):
    """使用baostock库获取基本面数据"""
    if not BAOSTOCK_AVAILABLE:
        return None
        
    try:
        # 登录baostock
        lg = bs.login()
        if lg.error_code != '0':
            print(f"❌ baostock登录失败: {lg.error_msg}")
            return None
        
        # 转换股票代码格式（000001 -> sz.000001）
        market_code = f"sz.{code}" if str(code).startswith(('0', '3')) else f"sh.{code}"
        
        # 获取财务数据
        rs = bs.query_profit_data(code=market_code, year=2023, quarter=4)
        if rs.error_code != '0':
            print(f"❌ baostock获取 {code} 财务数据失败: {rs.error_msg}")
            bs.logout()
            return None
        
        # 获取基本信息
        rs_list = []
        while rs.error_code == '0' and rs.next():
            rs_list.append(rs.get_row_data())
        
        if rs_list:
            data = rs_list[0]
            fundamental = {
                '股票代码': code,
                '股票名称': '',  # 需要单独获取
                '股票上市日期': '',
                '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
                '股票所属行业': '',
                '每股收益': str(data[4]) if len(data) > 4 else '',  # roe
                '市盈率（静）': '',
                '市盈率（TTM）': '',
                '毛利率': '',
                '净利率': str(data[5]) if len(data) > 5 else '',  # net_profit_ratio
                '资产收益率': str(data[4]) if len(data) > 4 else '',
                '资产负债率': '',
                '净利润增速': ''
            }
        else:
            fundamental = None
        
        bs.logout()
        return fundamental
        
    except Exception as e:
        print(f"❌ baostock获取 {code} 数据失败: {str(e)}")
        if BAOSTOCK_AVAILABLE:
            bs.logout()
        return None

def get_fundamentals_from_akshare_full(code):
    """使用akshare获取完整的财务数据"""
    try:
        # 获取股票基本信息
        try:
            stock_info = ak.stock_individual_info_em(symbol=code)
            stock_name = str(stock_info.loc[stock_info['item'] == '股票简称', 'value'].iloc[0]) if not stock_info.empty else ''
            ipo_date = str(stock_info.loc[stock_info['item'] == '上市时间', 'value'].iloc[0]) if not stock_info.empty else ''
            industry = str(stock_info.loc[stock_info['item'] == '行业', 'value'].iloc[0]) if not stock_info.empty else ''
        except:
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
        except:
            pass
        
        # 方法2: 使用财务分析指标
        try:
            profit_ability = ak.stock_financial_analysis_indicator(symbol=code)
            if not profit_ability.empty:
                profit_data = profit_ability.iloc[0].to_dict() if hasattr(profit_ability.iloc[0], 'to_dict') else {}
        except:
            pass
        
        # 方法3: 使用个股估值指标
        try:
            # 使用akshare的股票估值接口
            stock_zh_a_spot = ak.stock_zh_a_spot()
            stock_data = stock_zh_a_spot[stock_zh_a_spot['代码'] == code]
            if not stock_data.empty:
                valuation_data = {
                    '市盈率': str(stock_data.iloc[0].get('市盈率', '')),
                    '市净率': str(stock_data.iloc[0].get('市净率', '')),
                    '股息率': str(stock_data.iloc[0].get('股息率', ''))
                }
        except:
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
        except:
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
        print(f"❌ akshare获取 {code} 数据失败: {str(e)}")
        return None

def get_fundamentals_real_data(code, data_source='akshare'):
    """获取单只股票的真实基本面数据，支持多数据源"""
    
    # 数据源优先级：akshare > 东方财富 > baostock
    data_sources = ['akshare', 'eastmoney', 'baostock']
    
    if data_source != 'auto':
        data_sources = [data_source] + [ds for ds in data_sources if ds != data_source]
    
    for source in data_sources:
        try:
            if source == 'akshare':
                result = get_fundamentals_from_akshare_full(code)
                if result and result.get('股票名称'):
                    print(f"   ✅ 使用akshare获取 {code} 数据成功")
                    return result
                    
            elif source == 'eastmoney':
                result = get_fundamentals_from_eastmoney(code)
                if result and result.get('股票名称'):
                    print(f"   ✅ 使用东方财富API获取 {code} 数据成功")
                    return result
                    
            elif source == 'baostock' and BAOSTOCK_AVAILABLE:
                result = get_fundamentals_from_baostock(code)
                if result and result.get('股票名称'):
                    print(f"   ✅ 使用baostock获取 {code} 数据成功")
                    return result
        
        except Exception as e:
            print(f"   ❌ 所有数据源获取 {code} 数据失败: {str(e)}")
            
    # 如果所有数据源都失败，返回空数据
    return {
        '股票代码': code,
        '股票名称': '',
        '股票上市日期': '',
        '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
        '股票所属行业': '',
        '每股收益': '',
        '市盈率（静）': '',
        '市盈率（TTM）': '',
        '毛利率': '',
        '净利率': '',
        '资产收益率': '',
        '资产负债率': '',
        '净利润增速': ''
    }

def save_batch_to_csv(batch_data, mode='a'):
    """将批次数据保存到CSV文件"""
    try:
        df = pd.DataFrame(batch_data)
        
        # 确保数据格式正确 - 包含所有新的财务指标
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
        
        if mode == 'w' or not os.path.exists('cache/stockA_fundamentals.csv'):
            df.to_csv('cache/stockA_fundamentals.csv', index=False, encoding='utf-8-sig')
        else:
            df.to_csv('cache/stockA_fundamentals.csv', index=False, encoding='utf-8-sig', mode='a', header=False)
        
        return True
    except Exception as e:
        print(f"❌ 保存批次数据失败: {e}")
        return False

def update_log(stock_count, data_source):
    """更新日志文件"""
    try:
        log_data = {
            "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "股票数量": stock_count,
            "数据源": data_source,
            "可用数据源": {
                "baostock": BAOSTOCK_AVAILABLE,
                "东方财富API": "可用",
                "akshare": "可用"
            },
            "文件路径": "cache/stockA_fundamentals.csv",
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
                "盈利能力": ["每股收益", "每股净资产", "净资产收益率", "总资产收益率", "毛利率", "净利率", "营业利润率"],
                "估值指标": ["市盈率（静）", "市盈率（TTM）", "市净率", "市销率", "股息率"],
                "成长性": ["营业收入增长率", "净利润增长率", "净资产增长率", "净利润增速"],
                "偿债能力": ["资产负债率", "流动比率", "速动比率"],
                "运营能力": ["总资产周转率", "存货周转率", "应收账款周转率"],
                "现金流": ["每股经营现金流", "现金流量比率"]
            }
        }
        
        with open('cache/fundamentals_update_log.json', 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        print(f"❌ 更新日志失败: {e}")
        return False

def main(data_source='auto'):
    """主函数：获取A股股票完整基本面数据，支持断点续传和多数据源选择
    
    Args:
        data_source: 数据源选择
            'auto' - 自动选择最佳数据源
            'baostock' - 仅使用baostock
            'eastmoney' - 仅使用东方财富API
            'akshare' - 仅使用akshare
    """
    print("🚀 开始获取A股股票完整基本面数据...")
    print("📊 新增财务指标：ROE、ROA、每股净资产、市净率、股息率、营收增长率等")
    print(f"📊 数据源: {data_source}")
    
    # 显示可用数据源状态
    print("📡 数据源状态:")
    print(f"   baostock: {'✅ 可用' if BAOSTOCK_AVAILABLE else '❌ 未安装'}")
    print(f"   东方财富API: ✅ 可用")
    print(f"   akshare: ✅ 可用")
    
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
    
    print(f"📊 共{total_stocks}只股票，从第{start_index+1}只开始获取...")
    print(f"✅ 已完成{len(completed_codes)}只股票")
    
    if start_index >= total_stocks:
        print("🎉 所有股票数据已获取完成！")
        return
    
    # 获取待处理的股票
    remaining_codes = [code for code in stock_codes[start_index:] if code not in completed_codes]
    
    if not remaining_codes:
        print("🎉 没有需要获取的股票数据")
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
            print(f"\n🔄 处理第{current_start}-{current_end}只股票...")
            
            for j, code in enumerate(batch_codes):
                fundamental = get_fundamentals_real_data(code, data_source)
                
                if fundamental['股票名称'] and fundamental['股票名称'] != '':
                    batch_fundamentals.append(fundamental)
                    success_count += 1
                    completed_codes.add(code)
                    
                    # 每只股票显示进度
                    print(f"   ✅ {code} - {fundamental['股票名称']} 已获取")
                else:
                    print(f"   ⚠️  {code} 数据不完整，跳过")
                
                # 间隔时间避免请求过快
                time.sleep(0.5)
            
            # 保存批次数据
            if batch_fundamentals:
                if save_batch_to_csv(batch_fundamentals, mode):
                    current_index = start_index + i + len(batch_codes)
                    save_progress(current_index, list(completed_codes))
                    print(f"   💾 批次数据已保存 ({len(batch_fundamentals)}条记录)")
                    mode = 'a'  # 后续批次使用追加模式
                
                # 批次间隔
                time.sleep(3)
            else:
                print(f"   ⚠️  本批次无有效数据")
        
        # 更新最终日志
        update_log(success_count, f"multi_source_{data_source}")
        
        print(f"\n🎉 数据获取完成！")
        print(f"📊 成功获取 {success_count}/{total_stocks} 只股票的真实数据")
        print(f"📁 数据已保存到 cache/stockA_fundamentals.csv")
        print(f"📡 数据源: {data_source}")
        print(f"📈 数据包含完整财务指标：盈利能力、估值、成长性、偿债能力、运营能力、现金流六大维度")
        
        # 显示统计信息
        if os.path.exists('cache/stockA_fundamentals.csv'):
            df = pd.read_csv('cache/stockA_fundamentals.csv')
            print(f"\n📈 数据统计：")
            print(f"   📊 总记录数: {len(df)}")
            
            # 显示有效数据统计
            valid_data = df[df['股票名称'].notna() & (df['股票名称'] != '')]
            print(f"   ✅ 有效数据: {len(valid_data)}")
            
            if len(valid_data) > 0:
                # 行业分布
                industry_counts = valid_data['股票所属行业'].value_counts()
                if len(industry_counts) > 0:
                    print(f"   🏢 主要行业: {industry_counts.head(3).to_dict()}")
                
                # 显示前几条数据
                print(f"\n📋 数据预览:")
                preview_cols = ['股票代码', '股票名称', '股票所属行业', '每股收益', '净资产收益率', '市盈率（静）', '市净率']
                available_cols = [col for col in preview_cols if col in valid_data.columns]
                print(valid_data[available_cols].head())
        
    except KeyboardInterrupt:
        print(f"\n⚠️  用户中断，进度已保存")
        print(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count, f"multi_source_{data_source}")
    
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        print(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count, f"multi_source_{data_source}")

if __name__ == "__main__":
    import sys
    
    # 支持命令行参数选择数据源
    data_source = 'auto'
    if len(sys.argv) > 1:
        source_arg = sys.argv[1].lower()
        if source_arg in ['baostock', 'eastmoney', 'akshare', 'auto']:
            data_source = source_arg
        else:
            print("⚠️  无效的数据源参数，使用自动模式")
            print("   可用参数: auto, baostock, eastmoney, akshare")
    
    main(data_source=data_source)