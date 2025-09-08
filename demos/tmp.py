import baostock as bs
import pandas as pd
from datetime import datetime, timedelta

# 获取最近交易日函数
def get_lastest_trade_date():
    """
    使用baostock的query_trade_dates()函数获取最近的1个交易日
    
    返回:
    str: 最近的1个交易日，格式为"YYYY-MM-DD"
         如果获取失败，返回空字符串
    """
    try:
        # 登录baostock
        lg = bs.login()
        # 检查登录是否成功
        if lg.error_code != '0':
            print(f"登录失败: {lg.error_msg}")
            return ""
        
        # 计算查询日期范围，往前查询60天以确保能找到交易日
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        
        # 查询交易日历信息
        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
        
        # 检查查询是否成功
        if rs.error_code != '0':
            print(f"获取交易日历信息失败: {rs.error_msg}")
            return ""
        
        # 获取查询结果
        trade_dates = rs.get_data()
        
        # 转换is_trading_day列为整数类型
        try:
            trade_dates['is_trading_day'] = trade_dates['is_trading_day'].astype(int)
        except ValueError:
            print("警告：无法将is_trading_day列转换为整数类型")
            return ""
        
        # 筛选交易日并按日期降序排序
        trading_days = trade_dates[trade_dates['is_trading_day'] == 1]
        trading_days_sorted = trading_days.sort_values('calendar_date', ascending=False)
        
        # 如果有交易日数据，返回最近的1个交易日
        if not trading_days_sorted.empty:
            return trading_days_sorted.iloc[0]['calendar_date']
        else:
            print("未找到交易日数据")
            return ""
        
    except Exception as e:
        print(f"获取最近交易日时发生异常: {str(e)}")
        return ""
    finally:
        # 无论如何都要登出
        bs.logout()

# 测试函数
def test_get_lastest_trade_date():
    """测试获取最近交易日的函数"""
    print("\n=== 测试获取最近交易日 ===")
    
    # 获取最近的1个交易日
    latest_trade_date = get_lastest_trade_date()
    
    if latest_trade_date:
        print(f"最近的1个交易日是: {latest_trade_date}")
    else:
        print("未能获取到最近的交易日")

# 主程序
if __name__ == "__main__":
    print("开始获取最近交易日...")
    test_get_lastest_trade_date()
    print("\n获取完成")

