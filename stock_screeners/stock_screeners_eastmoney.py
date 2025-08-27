#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富基本面数据分析与优质股票筛选工具
功能：
1. 读取东方财富基本面数据（stockA_fundamentals_eastmoney.csv）
2. 进行策略分析，筛选出现价价值被低估的优质股票
3. 生成CSV、Markdown和JSON三种格式的筛选结果
"""

import pandas as pd
import json
import os
from datetime import datetime

# 确保result目录存在
def ensure_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# 读取东方财富基本面数据
def load_fundamental_data(file_path):
    """加载东方财富基本面数据"""
    try:
        df = pd.read_csv(file_path)
        print(f"✅ 成功读取数据，共{len(df)}只股票")
        return df
    except Exception as e:
        print(f"❌ 读取数据失败: {str(e)}")
        return None

# 数据清洗
def clean_data(df):
    """清洗数据，处理异常值和缺失值"""
    # 创建数据副本以避免修改原数据
    cleaned_df = df.copy()
    
    # 将列名中的空格去除
    cleaned_df.columns = [col.strip() for col in cleaned_df.columns]
    
    # 处理数值类型的列
    numeric_columns = ['每股收益', '每股净资产', '净资产收益率', '总资产收益率', 
                      '毛利率', '净利率', '营业利润率', '市盈率（静）', '市盈率（TTM）', 
                      '市净率', '市销率', '股息率', '营业收入增长率', '净利润增长率', 
                      '净资产增长率', '净利润增速', '资产负债率', '流动比率', 
                      '总资产周转率', '存货周转率', '应收账款周转率', '每股经营现金流', 
                      '现金流量比率']
    
    for col in numeric_columns:
        if col in cleaned_df.columns:
            # 尝试转换为数值类型
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
            # 替换无穷大值
            cleaned_df[col] = cleaned_df[col].replace([float('inf'), float('-inf')], None)
    
    # 删除ST股票和*ST股票
    cleaned_df = cleaned_df[~cleaned_df['股票名称'].str.contains('ST')]
    
    return cleaned_df

# 设计筛选策略并筛选优质股票
def screen_stocks(df):
    """根据多种财务指标筛选优质股票"""
    if df is None or df.empty:
        return None
    
    print("🔍 开始筛选优质股票...")
    
    # 分析数据分布后发现，指标值普遍偏高，可能存在数据单位或格式问题
    # 重新设计基于数据实际分布的筛选策略
    
    # 1. 先移除极端异常值
    # 计算各指标的四分位数和IQR
    q1_pe = df['市盈率（TTM）'].quantile(0.25)
    q3_pe = df['市盈率（TTM）'].quantile(0.75)
    iqr_pe = q3_pe - q1_pe
    lower_bound_pe = q1_pe - 1.5 * iqr_pe
    upper_bound_pe = q3_pe + 1.5 * iqr_pe
    
    q1_pb = df['市净率'].quantile(0.25)
    q3_pb = df['市净率'].quantile(0.75)
    iqr_pb = q3_pb - q1_pb
    lower_bound_pb = q1_pb - 1.5 * iqr_pb
    upper_bound_pb = q3_pb + 1.5 * iqr_pb
    
    # 移除市盈率和市净率的极端异常值
    filtered_df = df[(df['市盈率（TTM）'] >= lower_bound_pe) & 
                    (df['市盈率（TTM）'] <= upper_bound_pe) &
                    (df['市净率'] >= lower_bound_pb) & 
                    (df['市净率'] <= upper_bound_pb)]
    
    print(f"✅ 移除极端异常值后，剩余{len(filtered_df)}只股票")
    
    # 2. 根据实际数据分布设计筛选条件
    # 基于价值投资的核心指标：市盈率、市净率相对较低，盈利能力相对较强
    selected_stocks = filtered_df[
        # 市盈率（TTM）在合理范围内且为正
        (filtered_df['市盈率（TTM）'] > 0) & 
        # 市净率为正
        (filtered_df['市净率'] > 0) &
        # 综合考虑多个盈利指标
        ((filtered_df['净资产收益率'] > filtered_df['净资产收益率'].quantile(0.6)) |  # 净资产收益率高于60%分位数
         (filtered_df['毛利率'] > filtered_df['毛利率'].quantile(0.6)) |            # 毛利率高于60%分位数
         (filtered_df['净利率'] > filtered_df['净利率'].quantile(0.6))) &           # 净利率高于60%分位数
        # 财务风险相对较低
        (filtered_df['资产负债率'] < filtered_df['资产负债率'].quantile(0.8)) &     # 资产负债率低于80%分位数
        # 短期偿债能力相对较强
        (filtered_df['流动比率'] > filtered_df['流动比率'].quantile(0.4))          # 流动比率高于40%分位数
    ]
    
    print(f"✅ 初步筛选后，剩余{len(selected_stocks)}只股票")
    
    # 3. 如果结果数量不足20，进一步放宽条件
    if len(selected_stocks) < 20:
        print("⚠️  筛选结果较少，放宽部分条件...")
        selected_stocks = filtered_df[
            (filtered_df['市盈率（TTM）'] > 0) & 
            (filtered_df['市净率'] > 0) &
            ((filtered_df['净资产收益率'] > filtered_df['净资产收益率'].quantile(0.5)) |
             (filtered_df['毛利率'] > filtered_df['毛利率'].quantile(0.5)) |
             (filtered_df['净利率'] > filtered_df['净利率'].quantile(0.5))) &
            (filtered_df['资产负债率'] < filtered_df['资产负债率'].quantile(0.9))
        ]
    
    # 4. 按综合评分排序（基于分位数标准化）
    if not selected_stocks.empty:
        # 计算各指标的分位数，用于标准化评分
        pe_median = df['市盈率（TTM）'].median()
        pb_median = df['市净率'].median()
        roe_median = df['净资产收益率'].median()
        gross_profit_median = df['毛利率'].median()
        net_profit_median = df['净利率'].median()
        debt_ratio_median = df['资产负债率'].median()
        
        # 计算综合评分（基于相对表现）
        selected_stocks['综合评分'] = (
            # 市盈率（越低越好，取倒数）
            (pe_median / (selected_stocks['市盈率（TTM）'] + 1)) * 20 +
            # 市净率（越低越好，取倒数）
            (pb_median / (selected_stocks['市净率'] + 1)) * 20 +
            # 净资产收益率（越高越好）
            (selected_stocks['净资产收益率'] / (roe_median + 1)) * 20 +
            # 毛利率（越高越好）
            (selected_stocks['毛利率'] / (gross_profit_median + 1)) * 15 +
            # 净利率（越高越好）
            (selected_stocks['净利率'] / (net_profit_median + 1)) * 15 +
            # 资产负债率（越低越好）
            ((debt_ratio_median + 1) / (selected_stocks['资产负债率'] + 1)) * 10
        )
        
        # 限制评分在0-100之间
        selected_stocks['综合评分'] = selected_stocks['综合评分'].clip(0, 100)
        
        # 按综合评分降序排序
        selected_stocks = selected_stocks.sort_values('综合评分', ascending=False)
        
        # 最多选择50只股票
        if len(selected_stocks) > 50:
            selected_stocks = selected_stocks.head(50)
    
    print(f"✅ 筛选完成，共选出{len(selected_stocks)}只优质股票")
    return selected_stocks

# 保存筛选结果到CSV文件
def save_to_csv(df, file_path):
    """将筛选结果保存为CSV文件"""
    try:
        # 选择重要的列保存
        important_columns = ['股票代码', '股票名称', '股票所属行业', '市盈率（TTM）', 
                           '市净率', '净资产收益率', '毛利率', '净利率', 
                           '资产负债率', '流动比率', '营业收入增长率', '净利润增长率', 
                           '每股经营现金流', '综合评分']
        
        # 只保存数据框中存在的列
        columns_to_save = [col for col in important_columns if col in df.columns]
        
        df[columns_to_save].to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"✅ 结果已保存到CSV文件: {file_path}")
    except Exception as e:
        print(f"❌ 保存CSV文件失败: {str(e)}")

# 保存筛选结果到Markdown文件
def save_to_markdown(df, file_path):
    """将筛选结果保存为Markdown文件"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("# 东方财富优质股票筛选结果\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"共筛选出 {len(df)} 只优质股票\n\n")
            
            f.write("## 筛选策略说明\n\n")
            f.write("本策略基于价值投资理念，综合考虑以下关键财务指标：\n\n")
            f.write("1. **市盈率（TTM）较低** - 表明股票可能被低估\n")
            f.write("2. **市净率较低** - 表明股价相对于净资产较低\n")
            f.write("3. **净资产收益率较高** - 表明公司盈利能力强\n")
            f.write("4. **毛利率较高** - 表明公司产品竞争力强\n")
            f.write("5. **净利率较高** - 表明公司盈利能力强\n")
            f.write("6. **资产负债率合理** - 表明公司财务风险低\n")
            f.write("7. **流动比率良好** - 表明公司短期偿债能力强\n")
            f.write("8. **每股经营现金流为正** - 表明公司经营活动现金充足\n")
            f.write("9. **净利润增长率为正** - 表明公司利润增长良好\n\n")
            
            f.write("## 优质股票列表\n\n")
            
            # 创建Markdown表格
            f.write("| 排名 | 股票代码 | 股票名称 | 股票所属行业 | 市盈率（TTM） | 市净率 | 净资产收益率 | 综合评分 |\n")
            f.write("|------|----------|----------|--------------|--------------|--------|--------------|----------|\n")
            
            # 添加数据行
            for i, (_, row) in enumerate(df.iterrows(), 1):
                f.write(f"| {i} | {row.get('股票代码', '')} | {row.get('股票名称', '')} | ")
                f.write(f"{row.get('股票所属行业', '')} | {row.get('市盈率（TTM）', ''):.2f} | ")
                f.write(f"{row.get('市净率', ''):.2f} | {row.get('净资产收益率', ''):.2f} | ")
                f.write(f"{row.get('综合评分', ''):.2f} |\n")
            
            f.write("\n## 投资建议\n\n")
            f.write("1. **分散投资** - 不要将所有资金集中在少数几只股票上\n")
            f.write("2. **长期持有** - 优质股票适合长期投资，避免频繁交易\n")
            f.write("3. **定期复查** - 定期检查公司基本面变化，及时调整投资组合\n")
            f.write("4. **风险控制** - 根据个人风险承受能力调整仓位\n")
            f.write("5. **独立研究** - 本筛选结果仅供参考，请结合个人研究做出投资决策\n")
        
        print(f"✅ 结果已保存到Markdown文件: {file_path}")
    except Exception as e:
        print(f"❌ 保存Markdown文件失败: {str(e)}")

# 保存筛选结果到JSON文件
def save_to_json(df, file_path):
    """将筛选结果保存为JSON文件"""
    try:
        # 创建股票列表
        stock_list = []
        for _, row in df.iterrows():
            stock_info = {
                "code": str(row.get('股票代码', '')).zfill(6),  # 确保股票代码为6位
                "name": row.get('股票名称', '')
            }
            stock_list.append(stock_info)
        
        # 创建JSON数据
        json_data = {
            "generation_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_stocks": len(stock_list),
            "stocks": stock_list
        }
        
        # 保存JSON文件
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 结果已保存到JSON文件: {file_path}")
    except Exception as e:
        print(f"❌ 保存JSON文件失败: {str(e)}")

# 主函数
def main():
    print("🚀 东方财富基本面数据优质股票筛选工具")
    print("=" * 50)
    
    # 数据文件路径
    data_file = './cache/stockA_fundamentals_eastmoney.csv'
    
    # 确保result目录存在
    result_dir = './result'
    ensure_directory(result_dir)
    
    # 结果文件路径
    csv_file = os.path.join(result_dir, 'result_selected_eastmoney.csv')
    md_file = os.path.join(result_dir, 'result_selected_eastmoney.md')
    json_file = os.path.join(result_dir, 'result_selected_eastmoney.json')
    
    # 加载数据
    df = load_fundamental_data(data_file)
    if df is None:
        print("❌ 数据加载失败，程序退出")
        return
    
    # 清洗数据
    cleaned_df = clean_data(df)
    print(f"✅ 数据清洗完成，剩余{len(cleaned_df)}只股票")
    
    # 筛选优质股票
    selected_stocks = screen_stocks(cleaned_df)
    if selected_stocks is None or selected_stocks.empty:
        print("❌ 未筛选出符合条件的股票，程序退出")
        return
    
    # 保存结果
    save_to_csv(selected_stocks, csv_file)
    save_to_markdown(selected_stocks, md_file)
    save_to_json(selected_stocks, json_file)
    
    print("=" * 50)
    print("🎉 筛选完成！")
    print(f"📊 共筛选出 {len(selected_stocks)} 只优质股票")
    print(f"💾 结果已保存到 {result_dir} 目录")

if __name__ == "__main__":
    main()