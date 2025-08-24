#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股股票推荐程序测试脚本
从selected_stocks.json动态获取股票代码进行测试
"""

import subprocess
import sys
import os
import json

def load_stocks_from_json():
    """从selected_stocks.json加载股票代码列表"""
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'quantitative_trading', 'selected_stocks.json')
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            stock_codes = json.load(f)
        
        if isinstance(stock_codes, list) and stock_codes:
            print(f"✅ 成功从JSON文件加载 {len(stock_codes)} 只股票")
            return stock_codes
        else:
            print("❌ JSON文件格式错误或为空")
            return []
            
    except FileNotFoundError:
        print("❌ selected_stocks.json 文件未找到")
        return []
    except json.JSONDecodeError:
        print("❌ JSON文件解析失败")
        return []

def test_stock_recommendation(stock_code):
    """测试特定股票的推荐功能"""
    print(f"\n🚀 正在分析股票 {stock_code}...")
    
    try:
        # 运行推荐程序
        result = subprocess.run([
            sys.executable, 
            'stock_recommendation.py', 
            stock_code
        ], capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
        
        if result.returncode == 0:
            print(f"✅ {stock_code} 分析报告已生成")
            # 显示文件内容预览
            md_file = f"stock_recommendation_{stock_code}.md"
            if os.path.exists(md_file):
                with open(md_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()[:15]  # 显示前15行
                    print("\n📊 报告预览:")
                    for line in lines:
                        print(line.rstrip())
        else:
            print(f"❌ {stock_code} 分析失败: {result.stderr}")
            
    except Exception as e:
        print(f"❌ 运行错误: {e}")

def main():
    """主函数"""
    print("🎯 A股股票推荐程序测试")
    print("=" * 50)
    
    # 从JSON文件加载股票代码
    test_stocks = load_stocks_from_json()
    
    if not test_stocks:
        print("❌ 没有可用的股票代码进行分析")
        return
    
    print(f"\n📊 开始分析 {len(test_stocks)} 只股票...")
    print("=" * 50)
    
    for stock in test_stocks:
        test_stock_recommendation(stock)
        print("-" * 50)
    
    print("\n📁 所有报告已生成完成！")
    print("查看详细报告请打开对应的 .md 文件")
    print("\n📋 已分析股票列表:")
    for i, stock in enumerate(test_stocks, 1):
        print(f"   {i}. {stock}")

if __name__ == "__main__":
    main()