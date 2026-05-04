from collections import namedtuple
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
from fyers_api import fyersModel
from fyers_api import accessToken
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta


import undetected_chromedriver as uc
from pandasql import sqldf
import yaml
import json
import time
import pyotp
import sqlite3
import pandas as pd

TREND_INTERVAL = 15
OPTION_PREFIX = "BANKNIFTY23FEB"
INDEX_NAME = 'NIFTY BANK'
OPTION_LEVELS = 3


def set_env(config_file):

    with open(f'config/{config_file}', 'r') as file:
        config = yaml.safe_load(file)

    return config


def get_kite_connect_session(config):

    key_file = config['zerodha']['keyfile']
    api_key = config['zerodha']['apikey']
    api_secret = config['zerodha']['apisecret']
    user_name = config['zerodha']['username']
    user_password = config['zerodha']['password']
    twofa = config['zerodha']['totphash']

    try:
        f = open(key_file, "r")
        keys = f.read()
        keys_json = json.loads(keys)

        api_key = keys_json['api_key']
        api_secret = keys_json['secret']
        # request_token = keys_json['req_token']
        access_token = keys_json['acc_token']
        f.close()

        kite = KiteConnect(api_key=api_key)

        kite.set_access_token(access_token)
        profile = kite.profile()
        print("Connect to Zerodha API")
        return kite

    except Exception as err:
        print(' Exception From get_kite_connect_session')
        print(f"Unexpected {err=}, {type(err)=}")

        kite = zerodha_api_login(
            config, key_file, api_key, api_secret, user_name, user_password, twofa)

        return kite


def zerodha_api_login(config, key_file, api_key, api_secret, user_name, user_password, twofa):
    # global kite
    key_file = config['zerodha']['keyfile']
    try:
        options = uc.ChromeOptions()
        # options.headless = True
        driver = uc.Chrome(options=options)
        # driver = uc.Chrome()

        driver.get(config['zerodha']['loginurl'])
        login_id = WebDriverWait(driver, 10).until(
            lambda x: x.find_element(by=By.XPATH, value='//*[@id="userid"]'))
        login_id.send_keys(user_name)

        pwd = WebDriverWait(driver, 10).until(
            lambda x: x.find_element(by=By.XPATH, value='//*[@id="password"]'))
        pwd.send_keys(user_password)

        submit = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="container"]/div/div/div[2]/form/div[4]/button'))
        submit.click()

        time.sleep(1)

        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="container"]/div/div/div[2]/form/div[2]/input'))
        authkey = pyotp.TOTP(twofa)
        totp.send_keys(authkey.now())

        continue_btn = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="container"]/div/div/div[2]/form/div[3]/button'))
        continue_btn.click()

        time.sleep(1)

        url = driver.current_url
        initial_token = url.split('request_token=')[1]
        request_token = initial_token.split('&')[0]
        time.sleep(1)
        driver.close()

        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)

        access_token = data['access_token']
        kite.set_access_token(access_token)
        profile = kite.profile()

        f = open(key_file, "w")
        f.writelines('{"api_key" : "' + api_key + '","secret" : "' + api_secret +
                     '","req_token" : "' + request_token + '","acc_token" : "' + access_token + '"}')
        f.close()

        return kite
    except Exception as err:
        print(' Exception From zerodha_api_login')
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def get_kite_ticker_session(config):
    key_file = config['zerodha']['keyfile']
    try:

        f = open(key_file, "r")
        keys = f.read()
        keys_json = json.loads(keys)

        api_key = keys_json['api_key']
        api_secret = keys_json['secret']
        # request_token = keys_json['req_token']
        access_token = keys_json['acc_token']
        f.close()

        kws = KiteTicker(api_key, access_token)
        print("Connected to Zerodha Ticker API")
        return kws

    except Exception as err:
        print(' Exception From get_kite_ticker_session')
        print(f"Unexpected {err=}, {type(err)=}")
        try:

            kite = get_kite_connect_session()

            f = open(key_file, "r")
            keys = f.read()
            keys_json = json.loads(keys)

            api_key = keys_json['api_key']
            # api_secret = keys_json['secret']
            # request_token = keys_json['req_token']
            access_token = keys_json['acc_token']
            f.close()

            kws = KiteTicker(api_key, access_token)

            return kws

        except Exception as err:
            print(' Exception From get_kite_ticker_session second part')
            print(f"Unexpected {err=}, {type(err)=}")


def get_fyers_session(config):

    client_code = config['fyers']['username']
    user_pin = config['fyers']['user_pin']
    app_id = config['fyers']['apikey']
    api_key = config['fyers']['apisecret']
    twofa = config['fyers']['totphash']
    url_redirect = config['fyers']['loginurl']
    response_type = config['fyers']['response_type']
    api_grant = config['fyers']['api_grant']
    api_state = config['fyers']['api_state']
    log_path = config['fyers']['log_path']
    auth_file = config['fyers']['auth_file']

    try:

        auth_file_object = open(f"{auth_file}", "r+")
        # auth_code = auth_file_object.readline()

        # session = accessToken.SessionModel(client_id=app_id,
        #                                    secret_key=api_key, redirect_uri=url_redirect,
        #                                    response_type=response_type, grant_type=api_grant,
        #                                    state=api_state)

        # session.set_token(auth_code)
        # response = session.generate_token()
        # # print(response)
        # access_token = response["access_token"]
        access_token = auth_file_object.readline()
        # app_id = f"{app_id}:{api_key}"
        # print("Print Access Token")
        # print(access_token)
        fyers = fyersModel.FyersModel(
            client_id=app_id, token=access_token, log_path=log_path)
        is_async = True
        # Set to true in you need async API calls
        auth_file_object.close()

        validate = fyers.get_profile()

        if "error" in validate["s"] or "error" in validate["message"] or "expired" in validate["message"]:
            print("Getting a new fyers access token!")
            raise

        return fyers

    except Exception as err:
        print(' Error Connecting to Fyers Api')
        print(f"Unexpected {err=}, {type(err)=}")
        try:

            session = accessToken.SessionModel(client_id=app_id,
                                               secret_key=api_key, redirect_uri=url_redirect,
                                               response_type=response_type, grant_type=api_grant,
                                               state=api_state)
            response = session.generate_authcode()
            # print(response)

            auth_code = fyers_api_login(response, client_code, twofa, user_pin)
            session.set_token(auth_code)
            response = session.generate_token()
            access_token = response["access_token"]
            auth_file_object = open(f"{auth_file}", "w+")
            auth_file_object.write(access_token)
            auth_file_object.close()

            fyers = fyersModel.FyersModel(
                client_id=app_id, token=access_token, log_path=log_path)
            # Set to true in you need async API calls
            is_async = True
            validate = fyers.get_profile()

            if "error" in validate["s"] or "error" in validate["message"] or "expired" in validate["message"]:
                print("Getting a new fyers access token!")
                raise

            return fyers

        except Exception as err:
            print(' Error Connecting to Fyers Api')
            print(f"Unexpected {err=}, {type(err)=}")


def fyers_api_login(response, app_id, twofa, user_pin):
    global driver
    try:
        options = uc.ChromeOptions()
        options.headless = True
        # driver = uc.Chrome(options=options)
        # options.set_capability("detach", True)
        # driver = uc.Chrome(options=options, use_subprocess=True)  # uc.Chrome()
        driver = uc.Chrome()

        driver.get(f'{response}')

        time.sleep(1)

        login_id = WebDriverWait(driver, 10).until(
            lambda x: x.find_element(by=By.XPATH, value='//*[@id="fy_client_id"]'))
        login_id.send_keys(app_id)

        submit = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="clientIdSubmit"]'))
        submit.click()

        time.sleep(1)

        authkey = pyotp.TOTP(twofa)

        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="first"]'))

        totp.send_keys(authkey.now()[0])

        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="second"]'))

        totp.send_keys(authkey.now()[1])

        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="third"]'))

        totp.send_keys(authkey.now()[2])

        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="fourth"]'))

        totp.send_keys(authkey.now()[3])

        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="fifth"]'))

        totp.send_keys(authkey.now()[4])

        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="sixth"]'))

        totp.send_keys(authkey.now()[5])

        confirm_otp_submit = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="confirmOtpSubmit"]'))
        confirm_otp_submit.click()

        time.sleep(1)

        pin = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="pin-container"]/input[1]'))

        pin.send_keys(user_pin[0])

        pin = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="pin-container"]/input[2]'))

        pin.send_keys(user_pin[1])

        pin = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="pin-container"]/input[3]'))

        pin.send_keys(user_pin[2])

        pin = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="pin-container"]/input[4]'))

        pin.send_keys(user_pin[3])

        verify_pin_submit = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="verifyPinSubmit"]'))
        verify_pin_submit.click()

        auth_code = WebDriverWait(driver, 10).until(lambda x: x.find_element(
            by=By.XPATH,
            value='//*[@id="s_auth_code"]'))
        auth_code_value = auth_code.text

        time.sleep(1)

        return auth_code_value

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def get_sqlite_db_connection(config):

    sqlite_database = config['bank_nifty_db']['name']
    # sqlite_database = 'database/bank_nifty_07Feb23.db'
    sqlite_conn = sqlite3.connect(sqlite_database)

    return sqlite_conn


def get_token(config, instrument):
    """
        This function takes instrument name as argument and returns token id.

        return token --> int
    """
    try:
        # connect to sqlite database and open a cursor
        sqliteConnection = get_sqlite_db_connection(config)
        cursor = sqliteConnection.cursor()

        # create query with a trading symbol parameter, execute the query with parameter, fetch the first record into tuple
        sqlite_select_query = f"""SELECT instrument_token FROM instrument_master WHERE tradingsymbol = ? limit 1"""
        cursor.execute(sqlite_select_query, (instrument,))
        record = cursor.fetchone()
        cursor.close()

        # return token for the instrument
        return record[0]

    except sqlite3.Error as error:
        print("get_token:Failed to get token details from sqlite table", error)
    finally:
        # Finally close database connection
        if sqliteConnection:
            sqliteConnection.close()


def get_range_data(config, token, from_date, to_date):
    """
        This function takes instrument name as argument and returns token id.

        return token --> int
    """
    try:
        # connect to sqlite database and open a cursor
        sqliteConnection = get_sqlite_db_connection(config)
        # cursor = sqliteConnection.cursor()

        # create query with a trading symbol parameter, execute the query with parameter, fetch the first record into tuple
        sqlite_select_query = f"""SELECT * FROM instrument_history_minute WHERE instrument_token = ? and date >= ? and date < ?"""

        df = pd.read_sql(sqlite_select_query, sqliteConnection,
                         params=[token, from_date, to_date])
        sqliteConnection.close()

        # return token for the instrument
        return df

    except sqlite3.Error as error:
        print("get_range_data:Failed to get data from sqlite table", error)
    finally:
        # Finally close database connection
        if sqliteConnection:
            sqliteConnection.close()


def update_instrument(config, kite):
    try:

        sqlite_conn = get_sqlite_db_connection(config)

        instruments = kite.instruments(exchange=kite.EXCHANGE_NSE)
        df = pd.DataFrame(instruments)
        df.to_sql("instrument_master_stage", sqlite_conn, if_exists='append')
        sqlite_conn.commit()

        instruments = kite.instruments(exchange=kite.EXCHANGE_NFO)
        df = pd.DataFrame(instruments)
        df.to_sql("instrument_master_stage", sqlite_conn, if_exists='append')
        sqlite_conn.commit()

        sqlite_crsr = sqlite_conn.cursor()

        # another SQL command to insert the data in the table
        sql_command = """   UPDATE instrument_master
                            SET 	exchange_token = instrument_master_stage.exchange_token,
                                    tradingsymbol = instrument_master_stage.tradingsymbol,
                                    name = instrument_master_stage.name,
                                    last_price = instrument_master_stage.last_price,
                                    expiry = instrument_master_stage.expiry,
                                    strike = instrument_master_stage.strike,
                                    tick_size = instrument_master_stage.tick_size,
                                    lot_size = instrument_master_stage.lot_size,
                                    instrument_type = instrument_master_stage.instrument_type,
                                    segment = instrument_master_stage.segment,
                                    exchange = instrument_master_stage.exchange
                            FROM 	instrument_master_stage
                            WHERE 	instrument_master.instrument_token = instrument_master_stage.instrument_token;"""

        sqlite_crsr.execute(sql_command)
        sqlite_conn.commit()

        sql_command = """   INSERT OR IGNORE INTO instrument_master
                                (	"instrument_token","exchange_token","tradingsymbol","name","last_price","expiry",
									"strike","tick_size","lot_size","instrument_type","segment","exchange")
                            SELECT 	"instrument_token","exchange_token","tradingsymbol","name","last_price","expiry",
									"strike","tick_size","lot_size","instrument_type","segment","exchange"
                            FROM 	instrument_master_stage;"""
        sqlite_crsr.execute(sql_command)
        sqlite_conn.commit()

        sql_command = " DELETE FROM instrument_master_stage;"
        sqlite_crsr.execute(sql_command)
        sqlite_conn.commit()

        sqlite_crsr.close()
        sqlite_conn.close()
        print('Updated Zerodha Instruments.')

    except Exception as err:
        print(' Exception From update_instrument_token')
        print(f"Unexpected {err=}, {type(err)=}")


def get_instrument_token(config, index_name):
    try:

        sqlite_conn = get_sqlite_db_connection(config)
        sqlite_crsr = sqlite_conn.cursor()

        sql_command = f"  SELECT instrument_token FROM instrument_master  where tradingsymbol = '{index_name}' limit 1;"
        sqlite_crsr.execute(sql_command)
        sqlite_conn.commit()

        for instrument_token in sqlite_crsr:
            index_token = instrument_token[0]

        sqlite_crsr.close()
        sqlite_conn.close()

        return index_token

    except Exception as err:
        print(' Exception From get_instrument_token')
        print(f"Unexpected {err=}, {type(err)=}")


def get_option_token(config, kite, inst_list):

    # tokens = []

    index_token = get_instrument_token(config, INDEX_NAME)

    if not (inst_list["name"] == INDEX_NAME).any():
        inst_list.loc[len(inst_list),
                      inst_list.columns] = INDEX_NAME, index_token

    # tokens.append(index_token)

    sqlite_conn = get_sqlite_db_connection(config)

    try:
        index_ltp_dict = kite.ltp(index_token)
        index_ltp = index_ltp_dict[f"{index_token}"]['last_price']

        base_value = round(index_ltp, -2)
        option_symbols = []
        levels = 0

        while levels < OPTION_LEVELS:
            if levels == 0:
                option_symbols.append(f"{OPTION_PREFIX}{int(base_value)}CE")
                option_symbols.append(f"{OPTION_PREFIX}{int(base_value)}PE")

            else:
                option_symbols.append(
                    f"{OPTION_PREFIX}{int(base_value + levels * 100)}CE")
                option_symbols.append(
                    f"{OPTION_PREFIX}{int(base_value + levels * 100)}PE")
                option_symbols.append(
                    f"{OPTION_PREFIX}{int(base_value - levels * 100)}CE")
                option_symbols.append(
                    f"{OPTION_PREFIX}{int(base_value - levels * 100)}PE")
            levels += 1

        option_symbols_string = ' '.join(
            str("'" + x + "',") for x in option_symbols)

        option_symbols_string += "'DUMMY'"
        sql_command = f"  SELECT tradingsymbol, instrument_token FROM instrument_master  where tradingsymbol in ({option_symbols_string}) order by tradingsymbol;"

        sqlite_crsr = sqlite_conn.cursor()
        sqlite_crsr.execute(sql_command)
        sqlite_conn.commit()

        for instrument_token in sqlite_crsr:
            if not (inst_list["name"] == instrument_token[0]).any():
                inst_list.loc[len(inst_list),
                              inst_list.columns] = instrument_token[0], instrument_token[1]
            # index_token = instrument_token[0]
            # tokens.append(index_token)

        sqlite_crsr.close()
        print("Added following Options to the ticker:", option_symbols)
        return inst_list

    except Exception as err:
        print(' Exception From get_instrument_token')
        print(f"Unexpected {err=}, {type(err)=}")
    finally:
        sqlite_conn.close()


def get_ticks_data(config, token, timestamp=None):
    """
        This function takes instrument name as argument and returns token id.

        return token --> int
    """
    try:
        # connect to sqlite database and open a cursor
        sqliteConnection = get_sqlite_db_connection(config)

        if timestamp:
            # create query with a trading symbol parameter, execute the query with parameter, fetch the first record into tuple
            sqlite_select_query = f"""SELECT * FROM option_ticks  WHERE instrument_token = ? and timestamp = ? order by timestamp"""
            ticks = pd.read_sql(sqlite_select_query, sqliteConnection,
                                params=[token, timestamp])
        else:
            # create query with a trading symbol parameter, execute the query with parameter, fetch the first record into tuple
            sqlite_select_query = f"""SELECT * FROM option_ticks  WHERE instrument_token = ? order by timestamp"""
            ticks = pd.read_sql(sqlite_select_query, sqliteConnection,
                                params=[token])

        # return token for the instrument
        return ticks

    except sqlite3.Error as error:
        print("get_ticks:Failed to get token details from sqlite table", error)
    finally:
        # Finally close database connection
        if sqliteConnection:
            sqliteConnection.close()


def get_option(current_index_value, option_prefix, OPTION_BLOCK, OPTION_LEVEL=0):
    """
        This function take current index value and return current call and put options.

        input param current_index_value --> float  option_prefix --> option prefix with intended option chain

        return namedtuple "call", "put"
    """
    try:
        Option = namedtuple("Option", ["call", "put"], rename=True)

        current_index_level = int(
            current_index_value // OPTION_BLOCK) * OPTION_BLOCK
        call = option_prefix + \
            str(current_index_level - OPTION_LEVEL * OPTION_BLOCK) + "CE"
        put = option_prefix + \
            str(current_index_level + OPTION_BLOCK +
                OPTION_LEVEL * OPTION_BLOCK) + "PE"
        option = Option(call, put)

        return option
    except Exception as error:
        print("get_option: Failed to get option.", error)


def get_instrument_trend(config, token, interval=TREND_INTERVAL, timestamp=None, reverse=False, last_timestamp=None):
    """
        This function takes token name as argument and returns all timestamp associated with that token.
        If timestamp option is not passed, it would look for most recent record.

        input param token --> int  timestamp --> none or timestamp

        return namedtuple "instrument_token", "open", "close", "min_value", "max_value", "avg_value", "movement", "timestamp"
    """
    try:
        # connect to sqlite database and open a cursor
        sqliteConnection = get_sqlite_db_connection(config)
        cursor = sqliteConnection.cursor()
        if timestamp == None:
            where_clause = f"""
                            instrument_token = {token} AND
                            ot.timestamp > datetime(
                                'now','localtime','-{interval} seconds')
                            """
        elif timestamp and not reverse:
            where_clause = f"""
                            instrument_token = {token} AND
                            ot.timestamp > datetime('{timestamp}', "-{interval} seconds") AND
                            ot.timestamp <= '{timestamp}'
                            """
        elif reverse:
            where_clause = f"""
                            instrument_token = {token} AND
                            ot.timestamp >= '{last_timestamp}' AND
                            ot.timestamp <= '{timestamp}' 
                            """

        # create query with a token parameter, execute the query with parameter, fetch all record to a list
        sqlite_select_query = f"""
                                SELECT 	instrument_token,
                                        (
                                            SELECT 	cp.last_price
                                            FROM 	option_ticks cp
                                            WHERE 	cp.instrument_token = value.instrument_token AND
                                                    cp.timestamp=value.open_time
                                            ORDER BY timestamp limit 1
                                        ) open,
                                        (
                                            SELECT 	cp.last_price
                                            FROM 	option_ticks cp
                                            WHERE 	cp.instrument_token = value.instrument_token AND
                                                    cp.timestamp=value.close_time
                                            ORDER BY timestamp limit 1
                                        ) close,
                                        round(min_value,2) min_value,
                                        round(max_value,2) max_value,
                                        round(avg_value,2) avg_value,
                                        round(movement,2) movement,
                                        round(avg_movement,2) abs_movement,
                                        close_time
                                FROM 	(
                                            SELECT 	instrument_token,
                                                    min(timestamp) open_time,
                                                    max(timestamp) close_time,
                                                    min(current_value) min_value,
                                                    max(current_value) max_value,
                                                    avg(current_value) avg_value,
                                                    sum(round(current_value - ifnull(last_value,current_value),2)) movement,
                                                    sum(abs(current_value - ifnull(last_value,current_value))) / count(instrument_token)  avg_movement
                                            FROM 	(
                                                        SELECT	ot.instrument_token,
                                                                ot.last_price current_value,
                                                                ot.timestamp,
                                                                (	SELECT	last_price
                                                                    FROM 	option_ticks otl
                                                                    WHERE 	otl.instrument_token = ot.instrument_token AND
                                                                            otl.timestamp < ot.timestamp
                                                                    order by timestamp desc
                                                                    LIMIT 1
                                                                ) last_value
                                                        FROM	option_ticks ot
                                                        WHERE 	{where_clause}
                                                        ORDER BY timestamp DESC

                                                    ) base
                                        ) value
                                """
        # print(sqlite_select_query)
        # input("Y")
        cursor.execute(sqlite_select_query)
        record = cursor.fetchone()
        cursor.close()
        Trend = namedtuple("Trend", ["instrument_token", "open", "close", "min_value",
                           "max_value", "avg_value", "movement", "avg_move", "timestamp"], rename=True)
        trend = Trend(record[0], record[1], record[2], record[3],
                      record[4], record[5], record[6], record[7], record[8])
        return trend

    except sqlite3.Error as error:
        print("get_instrument_trend: Failed to get instrument trends.", error)
    finally:
        # Finally close database connection
        if sqliteConnection:
            sqliteConnection.close()


def open_position(config):

    sqliteConnection = get_sqlite_db_connection(config)

    order_details = pd.DataFrame(
        columns=['option_symbol', 'quantity', 'value', 'brokrage'])

    sql_command = f"""  select  ROW_NUMBER () OVER (
                                    ORDER BY exchange_update_timestamp
                                ) RowNum,tradingsymbol, filled_quantity, transaction_value, transaction_type,
                                average_price, brokrage, transaction_charges,sebi_charges, stt_charges, gst_charges,total_charges,exchange_update_timestamp
                        from    order_details
                        where   status = 'COMPLETE'
                        order by RowNum
                    """

    sqlite_crsr = sqliteConnection.cursor()
    sqlite_crsr.execute(sql_command)
    # index_df = pd.read_sql(sql_command, sqlite_conn)
    sqliteConnection.commit()
    no_of_trade = 0
    for row in sqlite_crsr:
        no_of_trade += 1
        exists = 0
        for i in order_details.index:
            if order_details['option_symbol'][i] == row[1] and order_details['quantity'][i] != 0:
                valq = order_details['quantity'][i]
                valv = order_details['value'][i]
                if row[4] == 'BUY':
                    order_details.at[i, 'quantity'] = valq + row[2]
                    order_details.at[i, 'value'] = valv + row[3]
                else:
                    order_details.at[i, 'quantity'] = valq - row[2]
                    order_details.at[i, 'value'] = row[3] - valv
                valb = order_details['brokrage'][i]
                order_details.at[i, 'brokrage'] = valb + row[6]
                exists = 1

        if not exists:
            order_details.loc[len(order_details.index)] = [
                row[1], row[2], row[3], row[6]]

    sqlite_crsr.close()
    sqliteConnection.close()

    query = f"""    SELECT  sum(quantity) qtyo,
                            sum(value) valo
                    FROM    order_details
                    WHERE   quantity > 0
                """

    order_details_summary = sqldf(query, locals())

    open_value = order_details_summary['valo'][0]
    open_volume = order_details_summary['qtyo'][0]

    if open_value == None:
        open_value = 0
    if open_volume == None:
        open_volume = 0

    return open_volume


def get_pivot_fib(day_df, prev_day):

    try:
        p_d_o = day_df.loc[(day_df['date'] == prev_day)
                           ]["open"].to_numpy()[0]
        p_d_h = day_df.loc[(day_df['date'] == prev_day)
                           ]["high"].to_numpy()[0]
        p_d_l = day_df.loc[(day_df['date'] == prev_day)]["low"].to_numpy()[0]
        p_d_c = day_df.loc[(day_df['date'] == prev_day)
                           ]["close"].to_numpy()[0]

        base_pivot = round((p_d_h + p_d_l + p_d_c) / 3, 2)
        fist_supp = round(base_pivot - (.382 * (p_d_h - p_d_l)))
        second_supp = round(base_pivot - (.618 * (p_d_h - p_d_l)))
        third_supp = round(base_pivot - (p_d_h - p_d_l))
        first_resi = round(base_pivot + (.382 * (p_d_h - p_d_l)))
        second_resi = round(base_pivot + (.618 * (p_d_h - p_d_l)))
        third_resi = round(base_pivot + (p_d_h - p_d_l))

        PivotFib = namedtuple("PivotFib", [
                              "base_pivot", "fist_supp", "second_supp", "third_supp", "first_resi", "second_resi", "third_resi"])
        pivot_fib = PivotFib(base_pivot, fist_supp, second_supp,
                             third_supp, first_resi, second_resi, third_resi)
        return pivot_fib

    except Exception as err:
        print(err)


def get_ohlc(minute):

    try:
        p_m_d = minute["date"]
        p_m_o = minute["open"]
        p_m_h = minute["high"]
        p_m_l = minute["low"]
        p_m_c = minute["close"]

        OHLC = namedtuple("OHLC", ["time", "open", "high", "low", "close"])

        ohlc = OHLC(p_m_d, p_m_o, p_m_h, p_m_l, p_m_c)

        return ohlc

    except Exception as err:
        print(err)


def get_trend(ohlc):

    if ohlc.open > ohlc.close:
        trend = "DOWN"
    elif ohlc.open < ohlc.close:
        trend = "UP"
    else:
        trend = "NONE"

    return trend


def get_pivot_fib_status(ohlc, day_fib, pivot_status, pivot_changed):
    try:
        if ohlc.low < day_fib.base_pivot < ohlc.high:
            pivot_status = "BASE"
            pivot_changed = True
        elif ohlc.low < day_fib.fist_supp < ohlc.high:
            pivot_status = "FS"
            pivot_changed = True
        elif ohlc.low < day_fib.second_supp < ohlc.high:
            pivot_status = "SS"
            pivot_changed = True
        elif ohlc.low < day_fib.third_supp < ohlc.high:
            pivot_status = "TS"
            pivot_changed = True
        elif ohlc.low < day_fib.first_resi < ohlc.high:
            pivot_status = "FR"
            pivot_changed = True
        elif ohlc.low < day_fib.second_resi < ohlc.high:
            pivot_status = "SR"
            pivot_changed = True
        elif ohlc.low < day_fib.third_resi < ohlc.high:
            pivot_status = "TR"
            pivot_changed = True

        return pivot_status, pivot_changed

    except Exception as err:
        print(err)


def update_data_by_minute(config, kite, instrument, from_date, to_date):

    interval = 'minute'
    continuous = False
    oi = False
    try:

        token = get_token(config, instrument)

        history = kite.historical_data(
            token, from_date, to_date, interval, continuous, oi)
        history_df = pd.DataFrame(history)

        sqliteConnection = get_sqlite_db_connection(config)
        cursor = sqliteConnection.cursor()
        if not history_df.empty:
            history_df['date'] = history_df['date'].dt.tz_localize(None)
            history_df["instrument_token"] = token
            history_df.to_sql(name='instrument_history_minute_stage', con=sqliteConnection, schema=None, if_exists='append',
                              index=True, index_label=None)  # chunksize=10000

        # another SQL command to insert the data in the table
        sql_command = """   UPDATE  instrument_history_minute
                            SET 	open = B.open,
                                    high = B.high,
                                    low = B.low,
                                    close = B.close,
                                    volume = B.volume
                            FROM    instrument_history_minute A INNER JOIN instrument_history_minute_stage B
                                            ON A.instrument_token = B.instrument_token and A.date = B.date;"""

        sqliteConnection.execute(sql_command)
        sqliteConnection.commit()

        sql_command = """  INSERT OR IGNORE INTO instrument_history_minute (instrument_token,date, open, high, low, close, volume)
                                    SELECT 	instrument_token,date, open, high, low, close, volume
                                    FROM 	instrument_history_minute_stage; """
        sqliteConnection.execute(sql_command)

        # another SQL command to insert the data in the table
        sql_command = """   UPDATE  instrument_history_minute
                            SET 	tradingsymbol = B.tradingsymbol
                            FROM    instrument_history_minute A INNER JOIN instrument_master B
                                    ON A.instrument_token = B.instrument_token 
                            WHERE   A.tradingsymbol is NULL;"""
        sqliteConnection.execute(sql_command)
        sqliteConnection.commit()
        sqliteConnection.commit()

        sql_command = " DELETE FROM instrument_history_minute_stage;"
        sqliteConnection.execute(sql_command)
        sqliteConnection.commit()

        sqliteConnection.close()

    except Exception as err:
        print('Inside get_index_data_by_minute')
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def f(x):
    d = {}
    d['quantity'] = x['quantity'].sum()
    d['value'] = x['value'].sum()
    d['avg_value'] = (x['value'].sum() / x['quantity'].sum())
    return pd.Series(d, index=['quantity', 'value', 'avg_value'])
