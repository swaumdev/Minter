"""


"""
from minter_function import *
from datetime import datetime, timedelta

# Define variables
INDEX_NAME = "NIFTY BANK"
DAY_INTERVAL = "day"


prev_day = datetime.strptime('2022-02-18', '%Y-%m-%d')
# run_day = '2022-02-16'
run_day = datetime.strptime('2022-02-19', '%Y-%m-%d')
next_day = '2023-02-18'

config_file = "minter_config_s_z.yml"
config = set_env(config_file)

# Get Kite Connection
kite = get_kite_connect_session(config)
update_instrument(config, kite)

# Get Previous Days ohlc data
# historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False)

# index_token = get_token(config, INDEX_NAME)
# # prev_day_data = kite.historical_data(
# #     instrument_token=index_token, from_date=run_day, to_date=run_day, interval="day", continuous=False, oi=False)

# # prev_day_df = pd.DataFrame(prev_day_data)
# # prev_day_df['date'] = prev_day_df['date'].dt.tz_localize(None)
# # print(prev_day_df)


while run_day < datetime.strptime('2023-02-19', '%Y-%m-%d'):
    update_data_by_minute(config, kite, INDEX_NAME, prev_day, run_day)
    print("Updated", prev_day)
    prev_day += timedelta(days=1)
    run_day += timedelta(days=1)
