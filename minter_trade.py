from minter_function import *
# from pandasql import sqldf

INDEX_NAME = "NIFTY BANK"
PIVOT_BLOCK = .001

OPTION_PREFIX = "BANKNIFTY23216"
OPTION_BLOCK = 100

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
BUY_TIMESTAMP_ADJUST = 1
TREND_SECONDS = 15

BUY_QUANTITY_BLOCK = 25
BUY = "Buy"
SELL = "Sell"
CALL = "Call"
PUT = "Put"

SELL_DIFF = 20

config_file = "minter_config_s_f.yml"
config = set_env(config_file)

pivot_price = 0
option_adj_timestamp = None
Trend = None
call_pending = False
call_pending_quantity = 0
put_pending = False
put_pending_quantity = 0
sl_hit_count = 0


Index_Move = namedtuple(
    "Index_Move", ["name", "token", "value", "min", "max", "min_sl", "max_sl", "time"])


index_token = get_token(config, INDEX_NAME)
index_ticks = get_ticks_data(config, index_token)

option_trade = pd.DataFrame(
    columns=['id', 'option', 'type', 'action', 'price', 'quantity', 'value', 'net_quantity', "index", "timestamp"])
trade = 0
for index, tick in index_ticks.iterrows():

    timestamp = tick["timestamp"]

    if option_adj_timestamp != None and datetime.strptime(timestamp, TIMESTAMP_FORMAT) < option_adj_timestamp:
        # print("Continue")
        continue

    if not pivot_price:
        pivot_price = tick["last_price"]
        pivot_diff = pivot_price * PIVOT_BLOCK
        pivot_diff_sl = pivot_diff / 2
        index_move = Index_Move(INDEX_NAME, index_token, pivot_price, pivot_price - pivot_diff,
                                pivot_price + pivot_diff, pivot_price - pivot_diff_sl,
                                pivot_price + pivot_diff_sl, timestamp)

    if tick["last_price"] < index_move.min or tick["last_price"] > index_move.max:

        pivot_price = tick["last_price"]
        if pivot_price < index_move.min:
            Trend = "DOWN"
        elif pivot_price > index_move.max:
            Trend = "UP"

        pivot_diff = pivot_price * PIVOT_BLOCK
        pivot_diff_sl = pivot_diff / 2
        index_move = Index_Move(INDEX_NAME, index_token, pivot_price, pivot_price - pivot_diff,
                                pivot_price + pivot_diff, pivot_price - pivot_diff_sl,
                                pivot_price + pivot_diff_sl, timestamp)

        option_adj_timestamp = datetime.strptime(
            timestamp, TIMESTAMP_FORMAT) + timedelta(seconds=BUY_TIMESTAMP_ADJUST)

        if Trend == "DOWN":

            put_option = 'BANKNIFTY23216' + \
                str(round(int(pivot_price), -2)) + 'PE'

            put_option_token = get_token(config, put_option)

            put_option_trend = get_instrument_trend(config,
                                                    put_option_token, TREND_SECONDS, option_adj_timestamp)
            trade += 1
            option_trade.loc[len(
                option_trade), option_trade.columns] = trade, put_option, PUT, BUY, put_option_trend.close, BUY_QUANTITY_BLOCK, put_option_trend.close * BUY_QUANTITY_BLOCK, BUY_QUANTITY_BLOCK, pivot_price, timestamp

            call_pending = True
            call_pending_quantity += BUY_QUANTITY_BLOCK
            print(put_option, put_option_trend.close,
                  put_option_trend.timestamp)

        elif Trend == "UP":
            call_option = 'BANKNIFTY23216' + \
                str(round(int(pivot_price), -2)) + 'CE'

            call_option_token = get_token(config, call_option)

            call_option_trend = get_instrument_trend(config,
                                                     call_option_token, TREND_SECONDS, option_adj_timestamp)

            trade += 1
            option_trade.loc[len(
                option_trade), option_trade.columns] = trade, call_option, CALL, BUY, call_option_trend.close, BUY_QUANTITY_BLOCK, call_option_trend.close * BUY_QUANTITY_BLOCK, BUY_QUANTITY_BLOCK, pivot_price, timestamp

            put_pending = True
            put_pending_quantity += BUY_QUANTITY_BLOCK
            print(call_option, call_option_trend.close,
                  call_option_trend.timestamp)

    if call_pending == True and pivot_price > index_move.max_sl:

        option_adj_timestamp = datetime.strptime(
            timestamp, TIMESTAMP_FORMAT) + timedelta(seconds=BUY_TIMESTAMP_ADJUST)

        call_option = 'BANKNIFTY23216' + \
            str(round(int(pivot_price), -2)) + 'CE'

        call_option_token = get_token(config, call_option)

        call_option_trend = get_instrument_trend(config,
                                                 call_option_token, TREND_SECONDS, option_adj_timestamp)

        trade += 1
        option_trade.loc[len(
            option_trade), option_trade.columns] = trade, call_option, CALL, BUY, call_option_trend.close, call_pending_quantity, call_option_trend.close * call_pending_quantity, call_pending_quantity, pivot_price, timestamp

        call_pending = False
        call_pending_quantity = 0
        sl_hit_count += 1
        print(call_option, call_option_trend.close,
              call_option_trend.timestamp)

    if put_pending == True and pivot_price < index_move.min_sl:

        option_adj_timestamp = datetime.strptime(
            timestamp, TIMESTAMP_FORMAT) + timedelta(seconds=BUY_TIMESTAMP_ADJUST)

        put_option = 'BANKNIFTY23216' + \
            str(round(int(pivot_price), -2)) + 'PE'

        put_option_token = get_token(config, put_option)

        put_option_trend = get_instrument_trend(config,
                                                put_option_token, TREND_SECONDS, option_adj_timestamp)

        trade += 1
        option_trade.loc[len(
            option_trade), option_trade.columns] = trade, put_option, PUT, BUY, put_option_trend.close, put_pending_quantity, put_option_trend.close * put_pending_quantity, put_pending_quantity, pivot_price, timestamp

        put_pending = False
        put_pending_quantity = 0
        sl_hit_count += 1
        print
        print(put_option, put_option_trend.close,
              put_option_trend.timestamp)

print(f"Trade:{trade}, SL Trade:{sl_hit_count}")
print(option_trade.to_csv())
option_buy_summary = option_trade.groupby(
    ['option', 'type', 'action']).apply(f)
option_buy_summary['avg_value'] = option_buy_summary['avg_value'].round(
    decimals=2)
print(option_buy_summary)
option_net = option_trade.groupby(['action'])['value'].agg(['sum'])
print(option_net)
