from operator import ge
import baostock as bs
import pandas as pd

def query_all_stock():
    login_result = bs.login()
    result = bs.query_all_stock(day="2025-06-30")
    while (result.error_code == '0') & result.next():
        print(result.get_row_data())
        
    bs.logout()


def download_data(date):
    bs.login()

    # 获取指定日期的指数、股票数据
    stock_rs = bs.query_all_stock(date)
    stock_df = stock_rs.get_data()
    data_df = pd.DataFrame()
    for code in stock_df["code"]:
        print(code)
        name = stock_df[stock_df["code"] == code]["code_name"].values[0]
        print(name)

        code= "sh.600016"
        print("Downloading :" + code)
        k_rs = bs.query_history_k_data_plus(code, "date,code,open,high,low,close", date, date)
        print(k_rs.get_data())
        exit(0)
    bs.logout()
    
    print(data_df)


if __name__ == '__main__':
    query_all_stock()
    # 获取指定日期全部股票的日K线数据
    download_data("2025-06-30")