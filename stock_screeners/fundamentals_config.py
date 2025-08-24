#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股股票基本面数据缓存配置
"""

import os

class FundamentalsConfig:
    """基本面数据配置类"""
    
    # 缓存配置
    CACHE_DIR = 'cache'
    FUNDAMENTALS_CACHE = os.path.join(CACHE_DIR, 'stockA_fundamentals.csv')
    STOCK_LIST_CACHE = os.path.join(CACHE_DIR, 'stockA_list.csv')
    UPDATE_LOG = os.path.join(CACHE_DIR, 'update_log.json')
    
    # 更新周期（天）
    UPDATE_INTERVAL_DAYS = int(os.getenv('UPDATE_INTERVAL_DAYS', 30))
    
    # 分批处理配置
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 50))
    MAX_STOCKS = int(os.getenv('MAX_STOCKS', 0))  # 0表示处理所有
    
    # 数据字段配置
    REQUIRED_FIELDS = {
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
        
        # 资产负债表
        'total_assets': '总资产(万)',
        'total_liabilities': '总负债(万)',
        'total_equity': '股东权益(万)',
        'current_assets': '流动资产(万)',
        'current_liabilities': '流动负债(万)',
        
        # 利润表
        'revenue': '营业收入(万)',
        'operating_profit': '营业利润(万)',
        'net_profit': '净利润(万)',
        'gross_profit': '毛利润(万)',
        
        # 现金流量表
        'operating_cash_flow': '经营现金流(万)',
        'investing_cash_flow': '投资现金流(万)',
        'financing_cash_flow': '筹资现金流(万)',
        'free_cash_flow': '自由现金流(万)',
        
        # 关键财务指标
        'eps': '每股收益(元)',
        'pe_static': '市盈率(静)',
        'pe_ttm': '市盈率(TTM)',
        'pb': '市净率',
        'roe': '净资产收益率(%)',
        'roa': '总资产收益率(%)',
        'gross_margin': '毛利率(%)',
        'net_margin': '净利率(%)',
        'debt_ratio': '资产负债率(%)',
        'current_ratio': '流动比率',
        'quick_ratio': '速动比率',
        'revenue_growth': '营收增长率(%)',
        'profit_growth': '净利润增长率(%)',
        'assets_growth': '总资产增长率(%)',
        
        # 市场数据
        'current_price': '当前价格',
        'market_cap': '总市值(亿)',
        'dividend_yield': '股息率(%)',
        'dividend_per_share': '每股股利(元)',
        
        # 更新时间
        'update_time': '更新时间'
    }
    
    # 数据源优先级
    DATA_SOURCES = {
        'company_info': [
            'ak.stock_individual_info_em',
            'ak.stock_individual_info_em'
        ],
        'financial_abstract': [
            'ak.stock_financial_abstract_ths',
            'ak.stock_financial_analysis_indicator'
        ],
        'market_data': [
            'ak.stock_zh_a_spot_em',
            'ak.stock_zh_a_spot'
        ]
    }
    
    # 重试配置
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # 秒
    
    # 日志配置
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def get_config_summary(cls):
        """获取配置摘要"""
        return {
            'update_interval_days': cls.UPDATE_INTERVAL_DAYS,
            'batch_size': cls.BATCH_SIZE,
            'max_stocks': cls.MAX_STOCKS,
            'cache_dir': cls.CACHE_DIR,
            'total_fields': len(cls.REQUIRED_FIELDS)
        }