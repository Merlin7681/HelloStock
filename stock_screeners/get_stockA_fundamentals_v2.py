import pandas as pd
import numpy as np
import time
import json
import os
import requests
from datetime import datetime
import warnings
import akshare as ak
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

def get_fundamentals_simple(code):
    """使用更稳定的方法获取基本面数据"""
    try:
        # 方法1: 使用akshare的stock_zh_a_spot获取实时数据
        try:
            spot_data = ak.stock_zh_a_spot()
            stock_data = spot_data[spot_data['代码'] == code]
            if not stock_data.empty:
                stock_info = stock_data.iloc[0]
                name = str(stock_info.get('名称', '')).strip()
                pe = str(stock_info.get('市盈率TTM', '')).strip()
                industry = str(stock_info.get('行业', '')).strip()
            else:
                name = ''
                pe = ''
                industry = ''
        except:
            name = ''
            pe = ''
            industry = ''
        
        # 方法2: 使用stock_financial_hk_report_em获取财务数据
        try:
            # 获取财务报表摘要
            financial_data = ak.stock_financial_abstract_ths(symbol=code)
            if not financial_data.empty:
                # 获取最新数据
                latest = financial_data.iloc[0]
                eps = str(latest.get('每股收益', '')).strip()
                gross_margin = str(latest.get('毛利率', '')).strip()
                net_margin = str(latest.get('净利率', '')).strip()
                roe = str(latest.get('净资产收益率', '')).strip()
                debt_ratio = str(latest.get('资产负债率', '')).strip()
                profit_growth = str(latest.get('净利润增长率', '')).strip()
            else:
                eps = gross_margin = net_margin = roe = debt_ratio = profit_growth = ''
        except:
            eps = gross_margin = net_margin = roe = debt_ratio = profit_growth = ''
        
        # 如果name为空，尝试获取股票简称
        if not name:
            try:
                info = ak.stock_individual_info_em(symbol=code)
                name = str(info.get('股票简称', '')).strip()
            except:
                name = ''
        
        # 构建数据
        fundamental = {
            '股票代码': code,
            '股票名称': name,
            '股票上市日期': '',
            '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
            '股票所属行业': industry,
            '每股收益': eps,
            '市盈率（静）': pe,
            '市盈率（TTM）': pe,
            '毛利率': gross_margin,
            '净利率': net_margin,
            '资产收益率': roe,
            '资产负债率': debt_ratio,
            '净利润增速': profit_growth
        }
        
        return fundamental
        
    except Exception as e:
        print(f"❌ 获取 {code} 数据失败: {str(e)}")
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
        
        # 确保数据格式正确
        numeric_cols = ['每股收益', '市盈率（静）', '市盈率（TTM）', '毛利率', '净利率', '资产收益率', '资产负债率', '净利润增速']
        for col in numeric_cols:
            if col in df.columns:
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
            "文件路径": "cache/stockA_fundamentals.csv",
            "包含字段": [
                "股票代码", "股票名称", "股票上市日期", "股票上市地点", 
                "股票所属行业", "每股收益", "市盈率（静）", "市盈率（TTM）",
                "毛利率", "净利率", "资产收益率", "资产负债率", "净利润增速"
            ]
        }
        
        with open('cache/fundamentals_update_log.json', 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        print(f"❌ 更新日志失败: {e}")
        return False

def main():
    """主函数：支持断点续传的分批获取"""
    print("🚀 开始获取A股股票真实基本面数据...")
    
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
    batch_size = 10  # 每批处理10只股票
    success_count = len(completed_codes)
    
    try:
        for i in range(0, len(remaining_codes), batch_size):
            batch_codes = remaining_codes[i:i+batch_size]
            batch_fundamentals = []
            
            current_start = start_index + i + 1
            current_end = min(start_index + i + batch_size, total_stocks)
            print(f"\n🔄 处理第{current_start}-{current_end}只股票...")
            
            for j, code in enumerate(batch_codes):
                fundamental = get_fundamentals_simple(code)
                
                if fundamental['股票名称'] and fundamental['股票名称'] != '':
                    batch_fundamentals.append(fundamental)
                    success_count += 1
                    completed_codes.add(code)
                    print(f"   ✅ {code} - {fundamental['股票名称']} 已获取")
                else:
                    print(f"   ⚠️  {code} 数据不完整，跳过")
                
                # 间隔时间避免请求过快
                time.sleep(1)
            
            # 保存批次数据
            if batch_fundamentals:
                if save_batch_to_csv(batch_fundamentals, mode):
                    current_index = start_index + i + len(batch_codes)
                    save_progress(current_index, list(completed_codes))
                    print(f"   💾 批次数据已保存 ({len(batch_fundamentals)}条记录)")
                    mode = 'a'  # 后续批次使用追加模式
                
                # 批次间隔
                time.sleep(2)
            else:
                print(f"   ⚠️  本批次无有效数据")
        
        # 更新最终日志
        update_log(success_count, "akshare_simple")
        
        print(f"\n🎉 数据获取完成！")
        print(f"📊 成功获取 {success_count}/{total_stocks} 只股票的真实数据")
        print(f"📁 数据已保存到 cache/stockA_fundamentals.csv")
        
        # 显示统计信息
        if os.path.exists('cache/stockA_fundamentals.csv'):
            df = pd.read_csv('cache/stockA_fundamentals.csv')
            print(f"\n📈 数据统计：")
            print(f"   📊 总记录数: {len(df)}")
            
            valid_data = df[df['股票名称'].notna() & (df['股票名称'] != '')]
            print(f"   ✅ 有效数据: {len(valid_data)}")
            
            if len(valid_data) > 0:
                industry_counts = valid_data['股票所属行业'].value_counts()
                if len(industry_counts) > 0:
                    print(f"   🏢 主要行业: {industry_counts.head(3).to_dict()}")
                
                print(f"\n📋 数据预览:")
                print(valid_data[['股票代码', '股票名称', '股票所属行业', '每股收益', '市盈率（静）']].head())
        
    except KeyboardInterrupt:
        print(f"\n⚠️  用户中断，进度已保存")
        print(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count, "akshare_simple")
    
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        print(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count, "akshare_simple")

if __name__ == "__main__":
    main()