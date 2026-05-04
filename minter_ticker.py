from minter_function import *
from collections import namedtuple


config_file = "minter_config_h_z.yml"
config = set_env(config_file)

kite = get_kite_connect_session(config)
# print(kite.profile())
kws = get_kite_ticker_session(config)

PIVOT_DIFF = .001
current_index_value = 0
step_value = 0
current_pivot_min = 0
current_pivot_max = 0
index = None


# update_instrument(config, kite)

class Ticks:
    Index = namedtuple("Index", field_names=[
        "value", "i_min", "i_max", "i_min_sl", "i_max_sl"])

    index = Index(0, 0, 0, 0, 0)
    token_list = []
    inst_list = pd.DataFrame(columns=['name', 'token'])

    inst_quote = pd.DataFrame(columns=[
                              'name', 'token', 'price', 'volume', 'buy_qty', 'sell_qty', 'time', 'oi', 'io_min', 'oi_max'])

    inst_list = get_option_token(config, kite, inst_list)
    # print(inst_list)
    # token_list = "[" + ','.join(str(x)
    #                             for x in inst_list["token"].to_numpy()) + "]"

    # token_list = inst_list[inst_list.columns[1:]].to_numpy()
    for index, tk in inst_list.iterrows():
        token_list.append(tk["token"])
    # print(inst_list)

    def get_dataframe(ticks):

        ticks_df = pd.DataFrame(ticks)
        # print(ticks_df)
        # print(ticks_df.columns)
        for idx, row in ticks_df.iterrows():

            i_t = row["instrument_token"]
            l_p = row["last_price"]
            e_t = row["exchange_timestamp"]
            if row["tradable"]:
                v_t = round(row["volume_traded"] / 100000, 2)
                t_b_q = round(row["total_buy_quantity"] / 100000, 2)
                t_s_q = round(row["total_sell_quantity"] / 100000, 2)
                o_i = round(row["oi"] / 100000, 2)
                oi_day_low = round(row["oi_day_low"] / 100000, 2)
                oi_day_high = round(row["oi_day_high"] / 100000, 2)
            else:
                v_t = 0
                t_b_q = 0
                t_s_q = 0
                o_i = 0
                oi_day_low = 0
                oi_day_high = 0

            updated = 0
            for oti, otv in Ticks.inst_quote.iterrows():
                # print(row)
                if row["instrument_token"] == otv["token"]:
                    Ticks.inst_quote.loc[Ticks.inst_quote["token"] == row["instrument_token"], [
                        'price', 'volume', 'buy_qty', 'sell_qty', 'time', 'oi', 'io_min', 'oi_max']] = l_p, v_t, t_b_q, t_s_q, e_t, o_i, oi_day_high, oi_day_low
                    updated = 1

            if updated == 0:
                option_name = Ticks.inst_list.loc[Ticks.inst_list["token"]
                                                  == row["instrument_token"]]["name"].to_string(index=False)
                Ticks.inst_quote.loc[len(
                    Ticks.inst_quote), Ticks.inst_quote.columns] = option_name, i_t, l_p, v_t, t_b_q, t_s_q, e_t, o_i, oi_day_high, oi_day_low
        print(Ticks.inst_quote.sort_values(by=["name"]))

        # i_value = df["last_price"].to_numpy()[0]
        # if Ticks.index.value == 0:
        #     i_min = round(i_value - i_value * PIVOT_DIFF, 2)
        #     i_max = round(i_value + i_value * PIVOT_DIFF, 2)
        #     i_min_sl = round(i_value - (i_value * PIVOT_DIFF / 2), 2)
        #     i_max_sl = round(i_value + (i_value * PIVOT_DIFF / 2), 2)
        #     Ticks.index = Ticks.Index(
        #         i_value, i_min, i_max, i_min_sl, i_max_sl)
        # if i_value < Ticks.index.i_min:
        #     i_min = round(i_value - i_value * PIVOT_DIFF, 2)
        #     i_max = round(i_value + i_value * PIVOT_DIFF, 2)
        #     i_min_sl = round(i_value - (i_value * PIVOT_DIFF / 2), 2)
        #     i_max_sl = round(i_value + (i_value * PIVOT_DIFF / 2), 2)
        #     Ticks.index = Ticks.Index(
        #         i_value, i_min, i_max, i_min_sl, i_max_sl)

        # if i_value > Ticks.index.i_max:
        #     i_min = round(i_value - i_value * PIVOT_DIFF, 2)
        #     i_max = round(i_value + i_value * PIVOT_DIFF, 2)
        #     i_min_sl = round(i_value - (i_value * PIVOT_DIFF / 2), 2)
        #     i_max_sl = round(i_value + (i_value * PIVOT_DIFF / 2), 2)
        #     Ticks.index = Ticks.Index(
        #         i_value, i_min, i_max, i_min_sl, i_max_sl)

        # if i_value < Ticks.index.i_min_sl:
        #     pass

        # if i_value < Ticks.index.i_max_sl:
        #     pass

        # Ticks.index = Ticks.Index(
        #     df["last_price"].to_numpy()[0], Ticks.index.i_min, Ticks.index.i_max, Ticks.index.i_min_sl, Ticks.index.i_max_sl)

        # print(Ticks.index)
        # if index.🇻alue == 0:
        #     index.🇻alue = df["last_price"].to_string()
        # print(index.🇻alue)

    # update_instrument(kite)
    # token_list = get_option_token(kite)

    # get_order_details(kite)

    # Callback for tick reception.

    def on_ticks(ws, ticks):
        # get_dataframe(ticks, index)
        if len(ticks) > 0:
            Ticks.get_dataframe(ticks)

    def on_order_update(ws, ticks):
        print("Order Placed:")
        print(ticks)
        # ticks = str(ticks).replace("None", "'None'")
        # ticks = eval(str(ticks).replace("{}", "''"))
        # insert_tick_order_details([ticks])

    # Callback for successful connection.

    def on_connect(ws, response):
        print("Successfully connected. Response: {}".format(response))
        ws.subscribe(Ticks.token_list)
        ws.set_mode(ws.MODE_FULL, Ticks.token_list)
        print("Subscribe to tokens in Full mode: {}".format(Ticks.token_list))

    # Callback when current connection is closed.

    def on_close(ws, code, reason):
        print(
            "Connection closed: {code} - {reason}".format(code=code, reason=reason))

    # Callback when connection closed with error.

    def on_error(ws, code, reason):
        print(
            "Connection error: {code} - {reason}".format(code=code, reason=reason))

    # Callback when reconnect is on progress

    def on_reconnect(ws, attempts_count):
        print("Reconnecting: {}".format(attempts_count))

    # Callback when all reconnect failed (exhausted max retries)

    def on_noreconnect(ws):
        print("Reconnect failed.")

    # # Infinite loop on the main thread.
    # # You have to use the pre-defined callbacks to manage subscriptions.
    # kws_init.connect(threaded=True)


# Assign the callbacks.
kws.on_ticks = Ticks.on_ticks
kws.on_close = Ticks.on_close
kws.on_error = Ticks.on_error
kws.on_connect = Ticks.on_connect
kws.on_reconnect = Ticks.on_reconnect
kws.on_noreconnect = Ticks.on_noreconnect
kws.on_order_update = Ticks.on_order_update

# Infinite loop on the main thread.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=True)

# Block main thread
print(
    "This is main thread. Will change webosocket mode every 5 seconds.")

while True:
    pass
