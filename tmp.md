# TMP

~~~shell
# 获取全部A股（约5000+只）
python3 all_a_share_cache.py

# 获取指定数量（如1000只）用于测试
python3 all_a_share_cache.py --max-stocks 1000

# 查看缓存数据
head -10 cache/stockA_fundamentals.csv

# A股股票基本面数据缓存系统
bash -c python3 stock_fundamentals_cache.py

ls -l cache/
wc -l cache/stockA_list.csv
wc -l cache/stockA_fundamentals.csv

# 分析200只股票
MAX_STOCKS=200 python3 stock_selector.py

# 分析500只股票
MAX_STOCKS=500 python3 stock_selector.py

# 测试500只股票，每批50只
BATCH_SIZE=50 MAX_STOCKS=500 python3 stock_selector.py

# 处理所有5,741只股票
BATCH_SIZE=100 python3 stock_selector.py

# 立即使用（推荐）
python3 free_real_cache.py

# 验证数据
head -5 cache/stockA_fundamentals.csv
~~~