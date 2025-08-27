import pandas as pd
import numpy as np
import time
import json
import os
import requests
from datetime import datetime
import warnings
import akshare as ak

# 东方财富API基础URL
base_url = "http://push2.eastmoney.com/api"
code =  "000061"        
# 获取股票基本信息
url = f"{base_url}/qt/stock/get"
params = {
            'secid': f"0.{code}" if str(code).startswith(('0', '3')) else f"1.{code}",
            'fields': 'f43,f44,f45,f46,f48,f49,f50,f51,f52,f57,f58,f60,f62,f84,f85,f116,f117,f162,f163,f164,f167,f168,f169,f170,f171,f172,f173,f174,f175,f176,f177,f178,f184,f185,f186,f187,f188,f189,f190,f191,f277'
}
        
response = requests.get(url, params=params, timeout=10)
data = response.json()
        
            
stock_data = data['data']
print(stock_data)