import baostock as bs
import pandas as pd
from datetime import datetime, timedelta

def _get_lastest_trade_date(offset=0):
    """
    使用baostock的query_trade_dates()函数获取最近的1个交易日

    返回:
    str: 最近的offset个交易日（offset为0，则为最近一个交易日，1就是最近倒数第二个交易日），格式为"YYYY-MM-DD"
         如果获取失败，返回空字符串
    """
    try:
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
            return trading_days_sorted.iloc[offset]['calendar_date']
        else:
            print("未找到交易日数据")
            return ""
        
    except Exception as e:
        print(f"获取最近交易日时发生异常: {str(e)}")
        return ""


def get_stock_industry_info(stock_code):
    """
    使用baostock的query_stock_industry()函数获取特定股票的股票名称和类别信息
    industry_info = get_stock_industry_info(code)
    code_name = industry_info.iloc[0].get('code_name', '未知')
    industry = industry_info.iloc[0].get('industry', '未知')

    参数:
    stock_code (str): 股票代码，格式为6位数字，如"600000"、"000001"、"300001"
    
    返回:
    pandas.DataFrame: 包含股票类别信息的数据框
    updateDate	更新日期
    code	    证券代码
    code_name	证券名称
    industry	所属行业
    industryClassification	所属行业类别
    """
    try:
        # 登录baostock
        lg = bs.login()
        # 检查登录是否成功
        if lg.error_code != '0':
            print(f"登录失败: {lg.error_msg}")
            return None
        
        # 使用query_stock_industry()获取股票类别信息
        # 注意：baostock的股票代码需要带交易所前缀，如"sh.600000"或"sz.000001"
        # 所以需要先转换股票代码格式
        if stock_code.startswith('6'):
            # 上交所股票
            baostock_code = f"sh.{stock_code}"
        else:
            # 深交所股票 (0或3开头)
            baostock_code = f"sz.{stock_code}"
        
        # 查询股票类别信息
        rs = bs.query_stock_industry(code=baostock_code)
        
        # 检查查询是否成功
        if rs.error_code != '0':
            print(f"获取股票类别信息失败: {rs.error_msg}")
            return None
        
        # 获取查询结果
        industry_info = rs.get_data()
        
        return industry_info
    except Exception as e:
        print(f"获取股票类别信息时发生异常: {str(e)}")
        return None
    finally:
        # 无论如何都要登出
        bs.logout()

def convert_stock_code(stock_code):
    """
    将6位股票代码转换为9位格式（带交易所前缀）
    
    参数:
    stock_code (str): 6位股票代码字符串
    
    返回:
    str: 9位股票代码字符串，格式为SHxxxxxx或SZxxxxxx
    
    异常:
    ValueError: 当输入不是有效的6位数字代码时抛出
    """
    # 验证输入是否为6位数字
    if not isinstance(stock_code, str) or len(stock_code) != 6 or not stock_code.isdigit():
        raise ValueError("请输入有效的6位股票代码")
    
    # 判断交易所并添加前缀
    first_char = stock_code[0]
    first_three = stock_code[:3]
    
    # 上交所股票: 6开头或900开头(B股)
    if first_char == '6' or first_three == '900':
        return f"SH.{stock_code}"
    # 深交所股票: 0、3开头或200开头(B股)
    elif first_char in ['0', '3'] or first_three == '200':
        return f"SZ.{stock_code}"
    else:
        raise ValueError(f"无法识别的股票代码: {stock_code}")

def get_stock_pe_pb(stock_code, offset=0):
    """
    获取股票财务分析指标，也包括PE-TTM和PB数据
    参数:
        stock_code: 股票代码 (字符串格式)
        date: 年份 (字符串格式)
    返回:
        股票财务分析指标数据
    """
    init_baostock()
    data_list = []
    
    stock_code = convert_stock_code(stock_code)
    lastest_trade_date = _get_lastest_trade_date(offset)
    try:
        rs = bs.query_history_k_data_plus(stock_code
            , "date,code,peTTM,pbMRQ"
            ,start_date=lastest_trade_date
            ,frequency="d"
            ,adjustflag="3")
        print(f"API调用返回错误码: {rs.error_code}")
        print(f"API调用返回错误信息: {rs.error_msg}")
        # 处理API返回的数据
        print(f"INOF:最近交易日 {lastest_trade_date}")
        #print(f"INOF:股票代码 {rs.}")
        #print(rs)
        while (rs.next()):
            row_data = rs.get_row_data()
            print(f"原始数据行: {row_data}")  # 添加打印语句检查原始数据
            if len(row_data) >= 4:
                code = row_data[1]
                pe_ttm = row_data[2]
                pbMRQ = row_data[3] # 市净率
                data_list.append({
                    "date": row_data[0],
                    "code": code,
                    "peTTM": pe_ttm,
                    "pbMRQ": pbMRQ
                })
            return data_list
        return None
    except Exception as e:
        print(f"❌ 获取股票财务分析指标时发生异常: {str(e)}")
        return None
    
    finally:
        # 无论如何都要登出
        logout_baostock()
    
    return result




# 初始化baostock连接
def init_baostock():
    """
    初始化baostock连接
    返回: 是否连接成功
    """
    try:
        lg = bs.login()
        if lg.error_code != '0':
            print(f"❌ baostock登录失败: {lg.error_msg}")
            return False
        return True
    except Exception as e:
        print(f"❌ 连接baostock时发生异常: {str(e)}")
        return False

# 登出baostock连接
def logout_baostock():
    """
    登出baostock连接
    """
    try:
        bs.logout()
    except Exception as e:
        print(f"❌ 登出baostock时发生异常: {str(e)}")

# 通过baostock获取股票名称
def get_stock_name_by_code(stock_code):
    """
    通过股票代码获取股票名称
    
    参数:
        stock_code: 股票代码 (字符串格式)
    
    返回:
        股票名称 (字符串)
    """
    import datetime

    tmp_date = "2025-07-30"
    
    if not isinstance(stock_code, str):
        print(f"❌ 股票代码必须是字符串类型，当前类型: {type(stock_code)}")
        return f"未知股票({stock_code})"
    
    # 确保股票代码为6位格式
    stock_code = stock_code.strip()
    if len(stock_code) < 6:
        stock_code = stock_code.zfill(6)
    
    try:
        # 初始化连接
        if not init_baostock():
            return f"未知股票({stock_code})"
        
        # 获取当前日期
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        print(f"📅 当前日期: {today}")
        
        # baostock的股票代码格式为sh.600000或sz.000000
        stock_prefix = 'sh.' if stock_code.startswith('6') else 'sz.'
        full_code = f"{stock_prefix}{stock_code}"
        print(f"🔧 格式化后的完整代码: {full_code}")
        
        # 1. 方式一：直接查询股票基本信息（不带date参数）
        print("📊 尝试方式一：直接查询股票基本信息...")
        try:
            # 注意：这里不使用date参数，因为API不支持
            rs_basic = bs.query_stock_basic(code=full_code)
            
            if rs_basic.error_code == '0':
                print("✅ 股票基本信息查询成功")
                
                # 获取结果列名
                fields = rs_basic.fields
                print(f"📋 结果字段: {fields}")
                
                if rs_basic.next():
                    stock_info = rs_basic.get_row_data()
                    print(f"📝 原始数据: {stock_info}")
                    
                    # 查找股票名称（尝试不同位置）
                    # 常见位置：第2列或包含'name'的列
                    if len(stock_info) > 1:
                        # 尝试第2列（传统位置）
                        stock_name = stock_info[1]
                        if stock_name and stock_name.strip() and stock_name != stock_code:
                            print(f"✅ 从第2列获取股票名称: {stock_name}")
                            return stock_name.strip()
        except Exception as e:
            print(f"⚠️ 方式一执行异常: {str(e)}")
        
        # 2. 方式二：查询全部股票列表
        print("🔄 尝试方式二：查询全部股票列表...")
        try:
            # query_all_stock方法支持date参数
            rs_all = bs.query_all_stock(date=today)
            
            if rs_all.error_code == '0':
                stock_list = []
                while (rs_all.error_code == '0') & rs_all.next():
                    stock_list.append(rs_all.get_row_data())
                
                if stock_list:
                    print(f"✅ 成功获取{len(stock_list)}只股票列表")
                    
                    # 检查是否包含目标股票
                    for stock in stock_list:
                        if len(stock) > 0 and full_code in stock[0]:
                            print(f"🔍 在全部股票列表中找到目标股票")
                            # 再次尝试查询基本信息，但不带date参数
                            try:
                                rs_basic_retry = bs.query_stock_basic(code=full_code)
                                if rs_basic_retry.error_code == '0' and rs_basic_retry.next():
                                    retry_info = rs_basic_retry.get_row_data()
                                    print(f"📝 重试获取的原始数据: {retry_info}")
                                    if len(retry_info) > 1:
                                        stock_name = retry_info[1]
                                        if stock_name and stock_name.strip() and stock_name != stock_code:
                                            return stock_name.strip()
                            except Exception as e:
                                print(f"⚠️ 重试查询异常: {str(e)}")
        except Exception as e:
            print(f"⚠️ 方式二执行异常: {str(e)}")
        
        # 3. 方式三：尝试查询股票日线数据，从返回信息中提取名称
        print("🔄 尝试方式三：查询股票日线数据...")
        try:
            # 查询最近一个交易日的数据
            rs_daily = bs.query_history_k_data_plus(
                code=full_code,
                fields="date,code,open,high,low,close",
                start_date=(datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d'),
                end_date=today,
                frequency="d",
                adjustflag="3"
            )
            
            if rs_daily.error_code == '0':
                print("✅ 日线数据查询成功")
                # 检查返回字段，可能包含名称信息
                fields = rs_daily.fields
                print(f"📋 日线数据字段: {fields}")
                
                # 即使无法直接获取名称，也确认股票代码存在
                if rs_daily.next():
                    print(f"✅ 确认股票代码{stock_code}存在")
                    return f"股票{stock_code}"
        except Exception as e:
            print(f"⚠️ 方式三执行异常: {str(e)}")
        
        print(f"❌ 所有查询方式均失败，无法获取股票{stock_code}的名称")
        return f"未知股票({stock_code})"
    
    except Exception as e:
        print(f"❌ 获取股票{stock_code}名称时发生异常: {str(e)}")
        return f"未知股票({stock_code})"
    
    finally:
        # 无论如何都要登出
        logout_baostock()

