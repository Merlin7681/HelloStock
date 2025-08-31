# -*- coding: utf-8 -*-
"""
测试Piotroski F-Score计算程序
"""

import os
import sys
import time
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('test_fscore')

def test_fscore_akshare():
    """测试F-Score计算程序"""
    logger.info("🚀 开始测试Piotroski F-Score计算程序")
    
    # 检查akshare是否安装
    try:
        import akshare as bs
        logger.info("✅ akshare库已成功安装")
    except ImportError:
        logger.error("❌ akshare库未安装，请先运行: pip install akshare")
        return False
    
    # 检查股票列表文件是否存在
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')
    stock_list_file = os.path.join(cache_dir, 'stockA_list.csv')
    
    if not os.path.exists(stock_list_file):
        logger.warning(f"⚠️ 股票列表文件不存在: {stock_list_file}")
        logger.info("📝 创建测试用的股票列表文件...")
        
        # 创建cache目录（如果不存在）
        os.makedirs(cache_dir, exist_ok=True)
        
        # 创建测试用的股票列表文件（使用一些常见的A股代码）
        test_stocks = ['600000', '600036', '600519', '000858', '000333', '000001']
        
        # 写入CSV文件
        import pandas as pd
        df = pd.DataFrame({'股票代码': test_stocks})
        df.to_csv(stock_list_file, index=False, encoding='utf-8')
        logger.info(f"✅ 已创建测试用股票列表文件: {stock_list_file}")
    
    # 运行主程序（测试模式）
    main_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'get_stock_fscore_akshare.py')
    
    if not os.path.exists(main_script):
        logger.error(f"❌ 主程序文件不存在: {main_script}")
        return False
    
    logger.info("🏃‍♂️ 运行主程序（测试模式）...")
    
    try:
        import subprocess
        # 运行主程序并捕获输出
        result = subprocess.run(
            [sys.executable, main_script, '--test'],
            capture_output=True,
            text=True,
            timeout=300  # 设置超时时间为5分钟
        )
        
        # 输出程序的标准输出和错误输出
        if result.stdout:
            logger.info(f"程序输出:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"程序错误输出:\n{result.stderr}")
        
        # 检查程序是否成功执行
        if result.returncode == 0:
            logger.info("✅ 程序执行成功")
        else:
            logger.error(f"❌ 程序执行失败，返回码: {result.returncode}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("❌ 程序执行超时")
        return False
    except Exception as e:
        logger.error(f"❌ 运行程序时发生错误: {str(e)}")
        return False
    
    # 检查结果文件是否生成
    output_file = os.path.join(cache_dir, 'stockA_fscore_akshare.csv')
    
    if os.path.exists(output_file):
        logger.info(f"✅ 结果文件已生成: {output_file}")
        
        # 读取并显示前几行结果
        try:
            df = pd.read_csv(output_file)
            logger.info(f"📊 计算了 {len(df)} 只股票的F-Score")
            logger.info("📋 结果示例（前5行）:")
            print(df.head())
            
            # 显示F-Score分布
            f_score_counts = df['F-Score'].value_counts().sort_index(ascending=False)
            logger.info("📊 F-Score分布情况:")
            for score, count in f_score_counts.items():
                logger.info(f"   F-Score={score}: {count}只股票")
        except Exception as e:
            logger.error(f"❌ 读取结果文件时发生错误: {str(e)}")
    else:
        logger.error(f"❌ 结果文件未生成: {output_file}")
        return False
    
    # 检查日志文件是否生成
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    current_time = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(log_dir, f'stock_fscore_akshare_{current_time}.log')
    
    if os.path.exists(log_file):
        logger.info(f"✅ 日志文件已生成: {log_file}")
    else:
        logger.warning(f"⚠️ 日志文件未生成: {log_file}")
    
    logger.info("🎉 测试完成")
    return True

if __name__ == '__main__':
    success = test_fscore_akshare()
    sys.exit(0 if success else 1)