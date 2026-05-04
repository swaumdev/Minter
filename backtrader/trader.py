from minter_function import *
import backtrader as bt
import datetime
from strategy import TestStrategy

config_file = "minter_config_h_z.yml"
config = set_env(config_file)

# Define variables
INDEX_NAME = "NIFTY BANK"
DAY_INTERVAL = "day"

prev_day = '2023-02-14'
run_day = '2023-02-19'

index_token = get_token(config, INDEX_NAME)

df = get_range_data(config, index_token, prev_day, run_day)

cerebro = bt.Cerebro()
cerebro.broker.set_cash(100000)
print("Starting Portfolio Value : %.2f" % cerebro.broker.getvalue())

data = bt.feeds.YahooFinanceCSVData(
    dataname="stock.txt",
    # Do not pass values before this date
    fromdate=datetime.datetime(2000, 1, 1),
    # Do not pass values after this date
    todate=datetime.datetime(2020, 12, 31),
    reverse=False)
cerebro.adddata(data)
cerebro.addstrategy(TestStrategy)
cerebro.addsizer(bt.sizers.FixedSize, stake=100)
cerebro.run()

print("Final Portfolio Value : %.2f" % cerebro.broker.getvalue())

cerebro.plot()
