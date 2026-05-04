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


prev_day = '2023-02-14'
run_day = '2023-02-19'


config_file = "minter_config_s_z.yml"
config = set_env(config_file)

index_token = get_token(config, INDEX_NAME)

df = get_range_data(config, index_token, prev_day, run_day)

streak = False
last_trend = False
open_date = False
close_date = False
open_index = 0
close_index = 0
streak_str = None
for idx, row in df.iterrows():
    if idx < 2:
        continue

    if not streak:
        print(
            f"Open:{open_date}, Open Index:{open_index} Streak:{streak_str}, Close Index:{close_index}, Close:{close_date}")
        open_date = df.loc[idx-1]["date"]
        open_index = df.loc[idx-1]["close"]
        if df.loc[idx-2]["close"] < df.loc[idx-1]["close"]:
            last_trend = "UP"
            streak_str = "UP"
        elif df.loc[idx-2]["close"] > df.loc[idx-1]["close"]:
            last_trend = "DOWN"
            streak_str = "DOWN"
        else:
            continue
        streak = True
    else:
        if df.loc[idx]["close"] > df.loc[idx-1]["close"]:
            if last_trend == "DOWN":
                streak = False
                close_date = df.loc[idx]["date"]
                close_index = df.loc[idx]["close"]
            last_trend = "UP"
            streak_str += ",UP"
        elif df.loc[idx]["close"] < df.loc[idx-1]["close"]:
            if last_trend == "UP":
                streak = False
                close_date = df.loc[idx]["date"]
                close_index = df.loc[idx]["close"]
            last_trend = "DOWN"
            streak_str += ",DOWN"
