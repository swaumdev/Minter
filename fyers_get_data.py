from pymongo_get_database import get_database
from dateutil import parser
from minter_function import *
import time

config_file = "minter_config_s_f.yml"
config = set_env(config_file)

# data = {"symbol": "NSE:NIFTYBANK-INDEX", "resolution": "1", "date_format": "1",
#         "range_from": "2023-02-16", "range_to": "2023-02-16", "cont_flag": "1"}

data = {"symbol": "NSE:SBIN-EQ", "ohlcv_flag": "1"}


# data_history = fyers.history(data)
# data_history_df = pd.DataFrame(data_history)
# print(data_history_df.to_dict())
# print(data_history)
fyers = get_fyers_session(config)
dbname = get_database()
collection_name = dbname["stock_date"]

for i in range(100):
    data_history = fyers.depth(data)
    collection_name.insert_one(data_history)
    time.sleep(1)
