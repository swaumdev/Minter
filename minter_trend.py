"""
minter trend
1.  This script would get details of previous days market movement.

2. Calcualted Fibonacci Pivot Points 

3. Get minue by minute data for the day

4. Check how many times support and resistance line is crossed

5. Just check if based on this data for multiple days we can predict the trend for the day.

About Pivot Point

1. Simple Pivot Point

    To calculate Standard Pivot Points, you start with a Base Pivot Point, which is the simple average of High, Low and Close from a prior period.
    To calculate the Base Pivot Point: (P) = (High + Low + Close)/3 ( Of Previous day)
    To calculate the First Support Level: Support 1 (S1) = (P x 2) = High
    To calculate the Second Support Point: Support 2 (S2) = P  =  (High  =  Low)
    To calculate the First Resistance Level: Resistance 1 (R1) = (P x 2) = Low 
    To calculate the Second Resistance Level: Resistance 2 (R2) = P + (High  =  Low) 

2. Fibonacci Pivot Points 

Just like Standard Pivot Points, Fibonacci Pivot Points also start with a Base Pivot Point. The main difference is that they also incorporate Fibonacci levels in their calculations. Most traders use 38.2%, 61.8% and 100% retracements in their calculations and, therefore, Fibonacci Pivot Points represent three support and three resistance levels.

    To calculate the Base Pivot Point: Pivot Point (P) = (High + Low + Close)/3 
    To calculate the First Support Level: Support 1 (S1) = P - {.382 * (High  -  Low)} 
    To calculate the Second Support Level: Support 2 (S2) = P - {.618 * (High  -  Low)} 
    To calculate the First Resistance Level: Resistance 1 (R1) = P + {.382 * (High  -  Low)} 
    To calculate the Second Resistance Level: Resistance 2 (R2) = P + {.618 * (High  -  Low)} 
    To calculate the Third Resistance Level: Resistance 3 (R3) = P + {1 * (High  -  Low)} 

Stage: Development 

# Get Previous Days ohlc data
# Calculate Pivot Points
# Get Minute by Minute Data for the day
# Check if any pivot point is broken during the minute
# Predit the trend based on pivot point
# Check if the prediction is correct
# Calculated the % suceess of the prediction
# Publish the results

Testing: Not Done

Related Link 
https://stockstotrade.com/pivot-points/

"""
from minter_function import *

# Define variables
INDEX_NAME = "NIFTY BANK"
DAY_INTERVAL = "day"


prev_day = '2023-01-01'
run_day = '2023-02-01'
next_day = '2023-02-18'

pivot_status = None
trend = None
trend_count = 0
trend_success = 0
trend_fail = 0

config_file = "minter_config_s_z.yml"
config = set_env(config_file)

# Get Kite Connection
kite = get_kite_connect_session(config)


# Get Previous Days ohlc data
# historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False)

index_token = get_token(config, INDEX_NAME)
prev_day_data = kite.historical_data(
    instrument_token=index_token, from_date=prev_day, to_date=run_day, interval="day", continuous=False, oi=False)

prev_day_df = pd.DataFrame(prev_day_data)
prev_day_df['date'] = prev_day_df['date'].dt.tz_localize(None)
print(prev_day_df)

# Calculate Pivot Points
day_fib = get_pivot_fib(prev_day_df, prev_day)

print(day_fib)

# Get Minute by Minute Data for the day

curr_min_data = kite.historical_data(
    instrument_token=index_token, from_date=run_day, to_date=next_day, interval="3minute", continuous=False, oi=False)

curr_min_df = pd.DataFrame(curr_min_data)
curr_min_df['date'] = curr_min_df['date'].dt.tz_localize(None)
# print(curr_min_df)
print(curr_min_df.to_csv())
# Check if any pivot point is broken during the minute
for index, minute in curr_min_df.iterrows():
    pivot_changed = False
    min_ohlc = get_ohlc(minute)

    movement = get_trend(min_ohlc)

    pivot_status_change, pivot_changed = get_pivot_fib_status(
        min_ohlc, day_fib, pivot_status, pivot_changed)

    if pivot_status_change != pivot_status:
        pivot_status = pivot_status_change

    # Check if the prediction is correct
    if trend:
        trend_count += 1
        if trend == "UP":
            if min_ohlc.close > min_ohlc.open:
                trend_success += 1
            if min_ohlc.close < min_ohlc.open:
                trend_fail += 1
        if trend == "DOWN":
            if min_ohlc.close < min_ohlc.open:
                trend_success += 1
            if min_ohlc.close > min_ohlc.open:
                trend_fail += 1

    # Predit the trend based on pivot point
    # if pivot_changed:
    #     if movement == "UP":
    #         pivot_status
    #         trend = "DOWN"
    #         # print(f"Trend Changed, {pivot_status} upheld. {min_ohlc.time} ")
    #     if movement == "DOWN":
    #         trend = "UP"

    if min_ohlc.close < day_fib.third_supp:
        level = "FDOWN"
        trend = "DOWN"
    elif day_fib.third_supp < min_ohlc.close < day_fib.second_supp:
        level = "TS"
        trend = "DOWN"
    elif day_fib.second_supp < min_ohlc.close < day_fib.fist_supp:
        level = "SS"
        trend = "DOWN"
    elif day_fib.fist_supp < min_ohlc.close < day_fib.base_pivot:
        level = "FS"
        trend = "DOWN"
    elif day_fib.base_pivot < min_ohlc.close < day_fib.first_resi:
        level = "FR"
        trend = "UP"
    elif day_fib.first_resi < min_ohlc.close < day_fib.second_resi:
        level = "SR"
        trend = "UP"
    elif day_fib.second_resi < min_ohlc.close < day_fib.third_resi:
        level = "TR"
        trend = "UP"
    elif min_ohlc.close < day_fib.third_resi:
        level = "FUP"
        trend = "UP"

# Calculated the % suceess of the prediction
success_per = round((trend_success / trend_count) * 100, 2)

# Publish the results

print(
    f"Total Run:{trend_count}, Success:{trend_success}, Fail:{trend_fail},Percentage:{success_per}")
