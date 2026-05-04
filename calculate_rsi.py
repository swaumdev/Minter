from minter_function import *
import talib
import matplotlib.pyplot as plt
# Get symbol OHLC data

config_file = "minter_config_s_z.yml"
config = set_env(config_file)

prev_day = '2023-02-16'
run_day = '2023-02-17'

kite = get_kite_connect_session(config)

data_list = kite.historical_data(
    instrument_token=260105, from_date=run_day, to_date=run_day, interval="minute", continuous=False, oi=False)

data = pd.DataFrame(data_list)

print(data.info())
# print(data)
reversed_df = data.iloc[::-1]
data["RSI"] = talib.RSI(reversed_df["close"], 14)
print(data.info())
# print(data.to_csv())

ax1 = plt.subplot2grid((10, 1), (0, 0), rowspan=4, colspan=1)
ax2 = plt.subplot2grid((10, 1), (5, 0), rowspan=4, colspan=1)
ax1.plot(data['close'], linewidth=2.5)
ax1.set_title('Nifty Bank')
ax2.plot(data['RSI'], color='red', linewidth=1.5)
ax2.axhline(30, linestyle='--', linewidth=1.5, color='grey')
ax2.axhline(70, linestyle='--', linewidth=1.5, color='grey')
ax2.set_title('Nifty Bank RSI')

plt.show()


# def RSI(data, window=14, adjust=False):
#     delta = data['close'].diff(1).dropna()
#     loss = delta.copy()
#     gains = delta.copy()

#     gains[gains < 0] = 0
#     loss[loss > 0] = 0

#     gain_ewm = gains.ewm(com=window - 1, adjust=adjust).mean()
#     loss_ewm = abs(loss.ewm(com=window - 1, adjust=adjust).mean())

#     RS = gain_ewm / loss_ewm
#     RSI = 100 - 100 / (1 + RS)

#     return RSI
