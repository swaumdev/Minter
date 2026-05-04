from minter_function import *
# from pandasql import sqldf

INDEX_NAME = "NIFTY BANK"
PIVOT_BLOCK = 40

OPTION_PREFIX = "BANKNIFTY23209"
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

# kite = get_kite_connect_session(config)
# # kite_t = get_kite_connect_session(config)
# print(kite.profile())

# fyers = get_fyers_session(config)
# print(fyers.get_profile())

pivot_price = 0
option_adj_timestamp = None

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

    if tick["last_price"] < pivot_price - PIVOT_BLOCK or tick["last_price"] > pivot_price + PIVOT_BLOCK:

        option_adj_timestamp = datetime.strptime(
            timestamp, TIMESTAMP_FORMAT) + timedelta(seconds=BUY_TIMESTAMP_ADJUST)

        current_option = get_option(tick["last_price"],
                                    OPTION_PREFIX, OPTION_BLOCK)
        option_token = get_token(config, current_option.put)
        if tick["last_price"] < pivot_price - PIVOT_BLOCK:
            pivot_price -= PIVOT_BLOCK

        buy_option_trend = get_instrument_trend(config,
                                                option_token, TREND_SECONDS, option_adj_timestamp)

        trade += 1
        option_trade.loc[len(
            option_trade), option_trade.columns] = trade, current_option.put, PUT, BUY, buy_option_trend.close, BUY_QUANTITY_BLOCK, buy_option_trend.close * BUY_QUANTITY_BLOCK, BUY_QUANTITY_BLOCK, tick["last_price"], tick["timestamp"]

    # if tick["last_price"] > pivot_price + PIVOT_BLOCK:

        # option_adj_timestamp = datetime.strptime(
        #     timestamp, TIMESTAMP_FORMAT) + timedelta(seconds=BUY_TIMESTAMP_ADJUST)

        current_option = get_option(tick["last_price"],
                                    OPTION_PREFIX, OPTION_BLOCK)
        option_token = get_token(config, current_option.call)
        if tick["last_price"] > pivot_price + PIVOT_BLOCK:
            pivot_price += PIVOT_BLOCK

        buy_option_trend = get_instrument_trend(config,
                                                option_token, TREND_SECONDS, timestamp)

        trade += 1
        option_trade.loc[len(
            option_trade), option_trade.columns] = trade, current_option.call, CALL, BUY, buy_option_trend.close, BUY_QUANTITY_BLOCK, buy_option_trend.close * BUY_QUANTITY_BLOCK, BUY_QUANTITY_BLOCK, tick["last_price"], tick["timestamp"]

option_sell = option_trade[option_trade["net_quantity"].to_numpy() > 0]

for index, sell in option_sell.iterrows():

    sell_token = get_token(config=config, instrument=sell["option"])
    sell_trend = get_instrument_trend(
        config=config, token=sell_token, interval=3600, timestamp=option_adj_timestamp)
    print(sell_trend.close, sell["price"])
    # if sell_trend.close > sell["price"]:
    option_adj_timestamp = datetime.strptime(
        timestamp, TIMESTAMP_FORMAT) + timedelta(seconds=BUY_TIMESTAMP_ADJUST)

    trade += 1
    option_trade.loc[len(
        option_trade), option_trade.columns] = trade, sell["option"], sell["type"], SELL, sell_trend.close, BUY_QUANTITY_BLOCK, sell_trend.close * BUY_QUANTITY_BLOCK, 0, tick["last_price"], tick["timestamp"]

    option_trade.loc[option_trade["id"] == sell["id"],
                     "net_quantity"] = 0
print(option_trade.to_csv())
option_buy_summary = option_trade.groupby(
    ['option', 'type', 'action']).apply(f)
option_buy_summary['avg_value'] = option_buy_summary['avg_value'].round(
    decimals=2)

option_net = option_trade.groupby(['action'])['value'].agg(['sum'])
print(option_net)
print(option_buy_summary.to_csv())


#         for index, sell in option_sell.iterrows():
#             if instrument["type"] == PUT and instrument["action"] == BUY and instrument["net_quantity"] > 0:

#                 sell_token = get_token(
#                     config=config, instrument=sell["option"])
#                 sell_trend = get_instrument_trend(
#                     config=config, token=sell_token, interval=TREND_INTERVAL, timestamp=option_adj_timestamp)

#                 if sell_trend.close > sell["price"] + SELL_DIFF:
#                     option_adj_timestamp = datetime.strptime(
#                         timestamp, TIMESTAMP_FORMAT) + timedelta(seconds=BUY_TIMESTAMP_ADJUST)

#                     trade += 1
#                     option_trade.loc[len(
#                         option_trade), option_trade.columns] = trade, sell["option"], sell["type"], SELL, sell_trend.close, BUY_QUANTITY_BLOCK, sell_trend.close * BUY_QUANTITY_BLOCK, 0, tick["last_price"], tick["timestamp"]

#                     # print(
#                     #     option_trade.loc[option_trade["id"] == sell["id"], "value"])
#                     option_trade.loc[option_trade["id"] == sell["id"],
#                                      "net_quantity"] = 0

# for index, instrument in option_trade.iterrows():
#     if instrument["type"] == PUT and instrument["action"] == BUY and instrument["net_quantity"] > 0:
#         # print(instrument)
#         buy_token = get_token(config, instrument["option"])
#         buy_option_trend = get_instrument_trend(config,
#                                                 buy_token, TREND_SECONDS, option_adj_timestamp)

#         if buy_option_trend.close - instrument["price"] > 0:
#             trade += 1
#             option_trade.loc[len(
#                 option_trade), option_trade.columns] = trade, instrument["option"], PUT, SELL, buy_option_trend.close, BUY_QUANTITY_BLOCK, buy_option_trend.close * BUY_QUANTITY_BLOCK, BUY_QUANTITY_BLOCK
#             option_trade.loc[index,
#                              "net_quantity"] = instrument["quantity"] - BUY_QUANTITY_BLOCK
#             continue

# option_buy_summary = option_trade.groupby('option').apply(f)
# print(option_trade)
# print(option_buy_summary, tick["last_price"], tick["timestamp"])

# option_buy = option_buy.join(
#     f'"option_name":"{option.put}","option_price":"{buy_option_trend.close}","buy_quantity":"{BUY_QUANTITY_BLOCK}"')

# print(option_trade)
# input("C")
# option_trade.loc[option_trade["id"].to_numpy(
# ) == sell["id"], "net_quantity"] = sell_trend["quantity"] - sell_trend["quantity"]

# for index, instrument in option_trade.iterrows():
#     if instrument["type"] == CALL and instrument["action"] == BUY and instrument["net_quantity"] > 0:
#         # print(instrument)
#         sell_token = get_token(config, instrument["option"])
#         sell_option_trend = get_instrument_trend(config,
#                                                  sell_token, TREND_SECONDS, option_adj_timestamp)
#         if sell_option_trend.close - instrument["price"] > 0:
#             trade += 1
#             option_trade.loc[len(
#                 option_trade), option_trade.columns] = trade, instrument["option"], CALL, SELL, sell_option_trend.close, BUY_QUANTITY_BLOCK, sell_option_trend.close * BUY_QUANTITY_BLOCK, BUY_QUANTITY_BLOCK
#             option_trade.loc[index,
#                              "net_quantity"] = instrument["quantity"] - BUY_QUANTITY_BLOCK
#             continue

# print(pivot_price)
# print(tick["timestamp"])
