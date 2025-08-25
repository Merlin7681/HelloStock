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

def get_fundamentals_real_data(code):
    """获取单只股票的真实基本面数据"""
    try:
        # 获取股票基本信息
        stock_info = ak.stock_individual_info_em(symbol=code)
        
        # 获取最新财务数据（使用年度数据）
        try:
            # 获取财务报表摘要
            financial = ak.stock_financial_abstract_ths(symbol=code)
            if not financial.empty:
                # 获取最新年度数据
                annual_data = financial[financial['报告期'].str.contains('12-31')].iloc[0] if len(financial[financial['报告期'].str.contains('12-31')]) > 0 else financial.iloc[0]
            else:
                annual_data = pd.Series()
        except:
            annual_data = pd.Series()
        
        # 获取实时行情数据
        try:
            # 使用新浪实时行情
            quote = ak.stock_zh_a_spot()
            quote_data = quote[quote['代码'] == code]
            if not quote_data.empty:
                current_price = quote_data.iloc[0]['最新价']
                pe_ttm = quote_data.iloc[0]['市盈率TTM']
                pe_static = quote_data.iloc[0]['市盈率']
            else:
                current_price = None
                pe_ttm = None
                pe_static = None
        except:
            current_price = None
            pe_ttm = None
            pe_static = None
        
        # 获取股票概念和行业信息
        try:
            concept = ak.stock_board_concept_cons_ths(symbol=code)
            industry = concept.iloc[0]['行业'] if not concept.empty else ''
        except:
            industry = stock_info.get('行业', '')
        
        # 构建基本面数据
        fundamental = {
            '股票代码': code,
            '股票名称': str(stock_info.get('股票简称', '')).strip(),
            '股票上市日期': '',  # 需要单独获取上市日期
            '股票上市地点': '上海' if str(code).startswith(('6', '5')) else '深圳',
            '股票所属行业': str(industry).strip(),
            '每股收益': str(annual_data.get('每股收益', '')).strip() if pd.notna(annual_data.get('每股收益', '')) else '',
            '市盈率（静）': str(pe_static).strip() if pe_static and str(pe_static) != 'nan' else '',
            '市盈率（TTM）': str(pe_ttm).strip() if pe_ttm and str(pe_ttm) != 'nan' else '',
            '毛利率': str(annual_data.get('毛利率', '')).strip() if pd.notna(annual_data.get('毛利率', '')) else '',
            '净利率': str(annual_data.get('净利率', '')).strip() if pd.notna(annual_data.get('净利率', '')) else '',
            '资产收益率': str(annual_data.get('净资产收益率', '')).strip() if pd.notna(annual_data.get('净资产收益率', '')) else '',
            '资产负债率': str(annual_data.get('资产负债率', '')).strip() if pd.notna(annual_data.get('资产负债率', '')) else '',
            '净利润增速': str(annual_data.get('净利润增长率', '')).strip() if pd.notna(annual_data.get('净利润增长率', '')) else ''
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
        for col in df.columns:
            if col in ['每股收益', '市盈率（静）', '市盈率（TTM）', '毛利率', '净利率', '资产收益率', '资产负债率', '净利润增速']:
                df[col] = df[col].replace('', np.nan)
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
                fundamental = get_fundamentals_real_data(code)
                
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
        update_log(success_count, "akshare_real_data")
        
        print(f"\n🎉 数据获取完成！")
        print(f"📊 成功获取 {success_count}/{total_stocks} 只股票的真实数据")
        print(f"📁 数据已保存到 cache/stockA_fundamentals.csv")
        
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
                print(valid_data[['股票代码', '股票名称', '股票所属行业', '每股收益', '市盈率（静）']].head())
        
    except KeyboardInterrupt:
        print(f"\n⚠️  用户中断，进度已保存")
        print(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count, "akshare_real_data")
    
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        print(f"   已完成 {success_count} 只股票")
        save_progress(success_count, list(completed_codes))
        update_log(success_count, "akshare_real_data")

if __name__ == "__main__":
    main()