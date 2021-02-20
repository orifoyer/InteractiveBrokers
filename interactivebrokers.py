# ======================================================================================================================
	# Load the Python libraries
# ======================================================================================================================

import sys
import os
import math
import time
import logging
import traceback
import pandas as pd
from ib_insync import *
from ib_insync import IB
from ib_insync import Stock
from ib_insync import util
from datetime import datetime, date
from dbconnect import market_status, create_connection, select_all_tasks
import yfinance as yf


pid = str(os.getpid())
pidfile = "/tmp/ib.pid"
if os.path.exists(pidfile):
    print("%s already exists, exiting" %pidfile)
    exit()
open(pidfile, 'w').write(pid)


# ======================================================================================================================
#  Set the flags
# ======================================================================================================================

FLAG_FOR_DB_CONNECTION = True
FLAG_FOR_TESTING = False

# ======================================================================================================================
#  Logger Function
# ======================================================================================================================


def create_logger(filename, log_path, level=logging.INFO):

    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    logging_filename = filename + '.log'
    logging_path = os.path.join(log_path, logging_filename)

    # Add logging to file
    handler = logging.FileHandler(logging_path)
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Add logging to stdout
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


# create a logger. This will create a log file.
filename = 'stocks_strategy'
log_path = os.getcwd()
logger = create_logger(filename, log_path, level=logging.INFO)

# ======================================================================================================================
#  Logic Functions
# ======================================================================================================================


def connect_to_ib(platform_name, ip_address_no, tws_socket_no, ib_gateway_socket_no, client_id_no):

    logger.info('Establish connection with the IB account.')

    ib_connection = IB()

    try:
        if platform_name == 'TWS':
            # For TWS (use port 7496 or 7497)
            ib_connection.connect(ip_address_no, tws_socket_no, clientId=client_id_no)
        elif platform_name == 'IB Gateway':
            # For IB Gateway
            ib_connection.connect(ip_address_no, ib_gateway_socket_no, clientId=client_id_no)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error(''.join('!! ' + line for line in lines))

    return ib_connection


def determine_PnL(ib):

        account = ib.managedAccounts()[0]
        ib.reqPnL(account, '')
        IB.sleep(8)
        data = ib.pnl()
        print(data) 
        ib.cancelPnL(account, '')
        dailyPnL =0.0
        unrealizedPnL =0.0
        realizedPnL =0.0

        return dailyPnL, unrealizedPnL, realizedPnL



def extract_symbols_from_db(remove_stocks_list):

    # Create a db connection and extract the list of symbols.
    stocks_to_trade = select_all_tasks(create_connection('pirend'), 'Algo')

    # remove the unwanted stocks from the list.
    for stock_name in stocks_to_trade:
        if stock_name in remove_stocks_list:
            stocks_to_trade.remove(stock_name)

    # Create a db connection and extract the list of symbols to short.
    stocks_to_short = select_all_tasks(create_connection('pirend'), 'Short')

    return stocks_to_trade, stocks_to_short


def extract_net_balance(ib_connection, buffer_amt):

    net_balance_amount = None

    logger.info('Extract the available balance from the IB account.')

    for _ in range(3):
        try:
            avs = [v for v in ib_connection.accountValues() if v.tag == 'AvailableFunds']
            available_balance = float(avs[0].value)
            net_balance_amount = max(available_balance - buffer_amt, 0)
            break
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.info(''.join('!! ' + line for line in lines))

    return net_balance_amount


def extract_quotes(stock_contract_obj, ib_connection):

    last_price_value = None
    todays_cum_volume = None
    latest_open_change_value = None
    latest_prevclose_change_value = None

    logger.info('Extract historical quotes data without realtime updates')
    
    bar_size = '1 day'
    duration = '2 D'
    what_to_show = 'TRADES'
    use_rth = True

    df_hist = get_historical_data(stock_contract_obj, ib_connection, bar_size, duration,
                                  what_to_show, use_rth, False)

    logger.info('Extract historical data with realtime updates')
   
    bar_size = '1 min'
    duration = '1 D'
    what_to_show = 'TRADES'
    use_rth = True
    df_hist_realtime = get_historical_data(stock_contract_obj, ib_connection, bar_size, duration,
                                           what_to_show, use_rth, True)

    if df_hist is not None and df_hist_realtime is not None:
        prev_close_price = df_hist.close.iloc[-2]
        todays_open_price = df_hist.open.iloc[-1]
        todays_low_price = df_hist.low.iloc[-1]

        last_price_value = df_hist_realtime.close.iloc[-1]

        latest_open_change_value = ((last_price_value - todays_open_price) / todays_open_price) * 100
        latest_prevclose_change_value = ((last_price_value - prev_close_price) / last_price_value) * 100

        df_hist_realtime['todays_cum_volume'] = df_hist_realtime['volume'].cumsum()
        todays_cum_volume = df_hist_realtime.todays_cum_volume.iloc[-1]

    return last_price_value, todays_cum_volume, latest_prevclose_change_value, latest_open_change_value


def y_quotes(x):
    c = yf.Ticker(x)
    d = yf.Ticker(x).info
    data = c.history()
    last_price_value = d['regularMarketPrice']
    closeprice = d['previousClose']
    opening_price = d['open']
    latest_prevclose_change_value = ((last_price_value - closeprice)/last_price_value)*100
    latest_open_change_value = ((last_price_value - opening_price)/last_price_value)*100
    todays_cum_volume = d['volume']
    return last_price_value, todays_cum_volume, latest_prevclose_change_value, latest_open_change_value

def get_historical_data(stock_contract_obj, ib_connection, bar_size_val, duration_val,
                        what_to_show_str, use_rth_bool, up_to_date):

    df_historical = None

    # use the qualify contracts function to automatically fill in the required additional information.
    ib_connection.qualifyContracts(stock_contract_obj)

    # extract historical data
    try:
        historical_data = ib_connection.reqHistoricalData(stock_contract_obj, '', barSizeSetting=bar_size_val,
                                                          durationStr=duration_val, whatToShow=what_to_show_str,
                                                          useRTH=use_rth_bool, keepUpToDate=up_to_date)

        # create a DataFrame from the candle data
        df_historical = util.df(historical_data)

        ib.cancelHistoricalData(historical_data)

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error(''.join('!! ' + line for line in lines))

    return df_historical


def check_conditions(latest_prevclose, latest_prevclose_threshold, latest_open_value, latest_open_threshold,
                     totalvolume_value, volume_threshold, net_balance_value, last_price_value):

    condition_1 = latest_prevclose < latest_prevclose_threshold
    condition_2 = latest_open_value < latest_open_threshold

    price_change_cond = condition_1 or condition_2
    balance_cond = net_balance_value > last_price_value
    volume_cond = totalvolume_value > volume_threshold

    return balance_cond, price_change_cond, volume_cond


def compute_qty_to_trade(net_balance_amt, last_price_value, price_change_cond, balance_cond,
                         stock_price_threshold, net_balance_threshold_1, net_balance_threshold_2):

    trade_qty = None


    if price_change_cond and balance_cond:
        if last_price_value < stock_price_threshold and net_balance_amt < net_balance_threshold_1:
            trade_qty = int(net_balance_amt / last_price_value)
        elif last_price_value < stock_price_threshold and net_balance_amt > net_balance_threshold_1:
            trade_qty = int(net_balance_threshold_1 / last_price_value)
        elif last_price_value > stock_price_threshold and net_balance_amt > net_balance_threshold_2:
            trade_qty = int(net_balance_threshold_2 / last_price_value)
        elif last_price_value > stock_price_threshold and net_balance_amt < net_balance_threshold_2:
            trade_qty = int(net_balance_amt / last_price_value)
        else: trade_qty= 0
    return trade_qty


def place_order(stock_name, stock_contract_obj, signal_type, price, qty, take_profit_percent, stop_loss_percent,
                wait_time_to_cancel, ib_connection):

    # if FLAG_FOR_TESTING:
    #     price = round(price - (price * (0.5/100)), 2)

    # determine the target profit and the stop loss price.
    take_profit_price = None
    stop_loss_price = None
    if signal_type == 'BUY':
        take_profit_price = round(price + (price * (take_profit_percent/100)), 2)
        stop_loss_price = round(price - (price * (stop_loss_percent/100)), 2)
    elif signal_type == 'SELL':
        take_profit_price = round(price - (price * (take_profit_percent/100)), 2)
        stop_loss_price = round(price + (price * (stop_loss_percent/100)), 2)

    # set the bracket order for the stock.
    bracket_order = ib_connection.bracketOrder(signal_type, qty, limitPrice=round(price, 2),
                                               takeProfitPrice=take_profit_price,
                                               stopLossPrice=stop_loss_price)
    counter = 0
    while ib_connection.waitOnUpdate():
        for single_order in bracket_order:
            if counter == 0:
                parent_order_obj = ib_connection.placeOrder(stock_contract_obj, single_order)
                parent_order_status = parent_order_obj.isDone()
                counter += 1
            else:
                bracket_order_obj = ib_connection.placeOrder(stock_contract_obj, single_order)
                counter += 1

        logger.info('Placed bracket order for stock %s' % stock_name)
        logger.info('Status of bracket order is: %s' % bracket_order_obj.orderStatus.status)
        logger.info('Parent order: %s' % str(bracket_order.parent))
        logger.info('takeProfit order: %s' % str(bracket_order.takeProfit))
        logger.info('stopLoss order: %s' % str(bracket_order.stopLoss))

        logger.info('Parent order status is: %s' % str(parent_order_status))
        ib_connection.sleep(wait_time_to_cancel)
        if not parent_order_status:
            logger.info('Parent order status is still False')
            ib_connection.cancelOrder(bracket_order.parent)
            logger.info('Cancelled submitted bracket order for %s after waiting for %s seconds' %
                        (stock_name, wait_time_to_cancel))

        break


def long_order(stock_name, stock_contract_obj, signal_type, current_pct, stop_param_value, price, qty,
               take_profit_percentage_value, stop_limit_percentage_value, wait_time_to_cancel, ib_connection):

    last_price, todays_total_volume, \
    latest_prevclose_change, latest_open_change = y_quotes(stock_name)

    if all(number is not None for number in
           [last_price, todays_total_volume, latest_prevclose_change, latest_open_change]):

        stop_pct = current_pct + stop_param_value

        new_pct = latest_prevclose_change
        if new_pct > stop_pct:
            logger.info('If latest percentage change is greater than stop_pct, then place order')
            place_order(stock_name, stock_contract_obj, signal_type, price, qty,
                        take_profit_percentage, stop_limit_percentage, wait_time_to_cancel, ib_connection)
        else:
            logger.info('If latest percentage change is lesser than stop_pct then wait for 5 seconds and check again')
            time.sleep(5)
            last_price, todays_total_volume, \
            latest_prevclose_change, latest_open_change = y_quotes(stock_name)

            if all(number is not None for number in
                   [last_price, todays_total_volume, latest_prevclose_change, latest_open_change]):

                price = last_price
                current_pct = latest_prevclose_change

                current_pct_condition = current_pct > stop_pct
                if FLAG_FOR_TESTING:
                    current_pct_condition = True

                if current_pct_condition:
                    place_order(stock_name, stock_contract_obj, signal_type, price, qty,
                                take_profit_percentage_value, stop_limit_percentage_value, wait_time_to_cancel,
                                ib_connection)
                else:
                    logger.info('%s may be heading lower, so re-run the function' % stock_name)


def short_order(stock_name, stock_contract_obj, signal_type, current_pct, stop_param_value, price,
                qty, take_profit_percentage_value, stop_limit_percentage_value, wait_time_to_cancel,
                ib_connection):

    last_price, todays_total_volume, \
    latest_prevclose_change, latest_open_change = y_quotes(stock_name)

    if all(number is not None for number in
           [last_price, todays_total_volume, latest_prevclose_change, latest_open_change]):

        stop_pct = current_pct + stop_param_value

        new_pct = latest_prevclose_change
        if new_pct < stop_pct:
            place_order(stock_name, stock_contract_obj, signal_type, price, qty,
                        take_profit_percentage, stop_limit_percentage, wait_time_to_cancel, ib_connection)
        else:
            time.sleep(5)
            last_price, todays_total_volume, \
            latest_prevclose_change, latest_open_change = y_quotes(stock_name)

            if all(number is not None for number in
                   [last_price, todays_total_volume, latest_prevclose_change, latest_open_change]):

                price = last_price
                current_pct = latest_prevclose_change

                current_pct_condition = current_pct < stop_pct
                if FLAG_FOR_TESTING:
                    current_pct_condition = True

                if current_pct_condition:
                    place_order(stock_name, stock_contract_obj, signal_type, price, qty,
                                take_profit_percentage_value, stop_limit_percentage_value, wait_time_to_cancel,
                                ib_connection)
                else:
                    logger.info('%s may be heading higher, so re-run the function' % stock_name)


# ======================================================================================================================
# Execute Tasks
# ======================================================================================================================


if __name__ == '__main__':

    try:

        # --------------------------------------------------------------------------------------------------------------
        # Set the params
        # --------------------------------------------------------------------------------------------------------------

        platform = 'TWS'  # options 'TWS' or 'IB Gateway'
        ip_address = '192.168.1.77'
        tws_socket = 7497
        ib_gateway_socket = 4002
        client_id = 1

        # set the stocks list in case db_connection is not to be used.
        if not FLAG_FOR_DB_CONNECTION:
            symbols_list = ['FSLR', 'AMZN', 'AAPL']
            symbols_short_list = ['TSLA']
            exchange = 'SMART'  # set the exchange
            currency = 'USD'    # set the currency of stock.

        if FLAG_FOR_DB_CONNECTION:
            stocks_to_remove = ['NKLA', 'AAL', 'FIRE', 'HITI']
            exchange = 'SMART'  # set the exchange
            currency = 'USD'    # set the currency of stock.
            symbols_list, symbols_short_list = extract_symbols_from_db(stocks_to_remove)
#            other_list, symbols_short_list = extract_symbols_from_db(stocks_to_remove)
#            symbols_list = ['GPL']
        # set a buffer amount to prevent the script from overspending
        # and entering a margin call.
        buffer_amount = 500         # In $ terms

        # set the limits for stock price and for net_balance.
        stock_price_limit = 250     # In $ terms
        net_balance_limit_1 = 500   # In $ terms
        net_balance_limit_2 = 1000  # In $ terms

        # set the limit for latest-over-PrevClose percentage change,
        # for latest-over-Open percentage change and the trading volume of the stock.
        latest_prevclose_change_limit = -3    # in % terms e.g. 2 means 2%
        latest_open_change_limit = -3         # in % terms e.g. 2 means 2%
        volume_limit = 10

        latest_prevclose_change_limit_for_shorting = 3   # in % terms e.g. 2 means 2%
        volume_limit_for_shorting = 10

        # set the take_profit and the stop_limit for the bracket order.
        take_profit_percentage = 1   # in % terms e.g. 2 means 2%
        stop_limit_percentage = 6    # in % terms e.g. 2 means 2%

        # stop percentage to set relative to the stocks current percentage.
        stop_param = 0.01   # means 0.01%

        # time after which the bracket order will be cancelled.
        cancel_order_time = 10  # in secs

        # --------------------------------------------------------------------------------------------------------------
        # Load the stock list, establish connection with IB and compute the net balance
        # --------------------------------------------------------------------------------------------------------------

        # create a db connection and extract the list of symbols
        if FLAG_FOR_DB_CONNECTION:
            symbol_list, symbols_short_list = extract_symbols_from_db(stocks_to_remove)

        # establish Connection with Interactive Brokers
        ib = connect_to_ib(platform, ip_address, tws_socket, ib_gateway_socket, client_id)

       # determine PnL
        determine_PnL(ib)

        # extract the initial net balance by subtracting the buffer amount
        net_balance = extract_net_balance(ib, buffer_amount)
        logger.info('Net balance is %s' % net_balance)

        # --------------------------------------------------------------------------------------------------------------
        # Run the strategy logic for every stock in the stock list
        # --------------------------------------------------------------------------------------------------------------

        now = datetime.now(); current_time = now.strftime("%H:%M")
        day = datetime.today().weekday(); current_date = date.today()

        logger.info('Execute the strategy logic for date %s %s' % (current_date, current_time))

        trade_list = []
        for stock in symbols_list:
            for trade_fill_no in range(len(ib.fills())):
                if stock == ib.fills()[trade_fill_no][0].symbol and ib.fills()[trade_fill_no][1].side == 'BOT':
                    logger.info("A trade for %s has already been filled. Adding it to trade_list" % stock)
                    trade_list.append(stock)

        trade_list = list(set(trade_list))

        for stock_count, stock in enumerate(symbols_list):

            # check if a trade for the symbol has already been filled
            stock_traded_condition = stock in trade_list

            if FLAG_FOR_TESTING:
                stock_traded_condition = False

            if stock_traded_condition:
                logger.info("A trade for %s has already been filled. Moving to the next stock" % stock)
                
            else:
                logger.info("Run the strategy's buy logic for %s stock (stock number %s in the list of %s stocks)"
                            % (stock, stock_count + 1, len(symbols_list)))

                logger.info('Create the contract object')
                stock_contract = Stock(stock, exchange, currency)
                ib.qualifyContracts(stock_contract)

                last_price, todays_total_volume, \
                latest_prevclose_change, latest_open_change = extract_quotes(stock_contract, ib)


                todays_total_volume = y_quotes(stock)[1]
#                print( str(stock) + ' Price: ' + str(last_price) + ' Vol: ' + str(todays_total_volume) + ' PrevCloseChage: ' + str(latest_prevclose_change) + ' OpenChange: ' + str(latest_open_change))

                if (net_balance > 0) and all(number is not None for number in [last_price, todays_total_volume,
                                                                               latest_prevclose_change,
                                                                               latest_open_change]):

                    # check for the required conditions
                    logger.info('Check for the required balance, price and volume conditions')
                    balance_condition, price_change_condition, \
                    volume_condition = check_conditions(latest_prevclose_change, latest_prevclose_change_limit,
                                                        latest_open_change, latest_open_change_limit,
                                                        todays_total_volume, volume_limit, net_balance, last_price)

                    if FLAG_FOR_TESTING:
                        balance_condition, price_change_condition, volume_condition = True, True, True

                    logger.info('Compute the quantity to trade')
                    qty_to_trade = compute_qty_to_trade(net_balance, last_price, price_change_condition,
                                                        balance_condition, stock_price_limit, net_balance_limit_1,
                                                        net_balance_limit_2)

                    if FLAG_FOR_TESTING:
                        qty_to_trade = 3

                    if volume_condition and qty_to_trade is not None and (qty_to_trade > 0):
                        logger.info('Apply the strategy logic after all the conditions are satisfied')
                        long_order(stock, stock_contract, "BUY", latest_prevclose_change, stop_param,
                                   last_price, qty_to_trade, take_profit_percentage, stop_limit_percentage,
                                   cancel_order_time, ib)
                        logger.info('Logic executed for %s stock' % stock)
                    else:
                        logger.info('Volume condition failed or qty is zero/None. Moving to the next stock')

                    logger.info('Moving to the next stock')

                    # extract the net balance after checking/executing for every stock
                    net_balance = extract_net_balance(ib, 0)
                    logger.info('Net balance is %s' % net_balance)

                else:
                    logger.info('Incomplete data for %s, moving to the next stock.' % stock)

        # --------------------------------------------------------------------------------------------------------------
        # Run the strategy logic for every stock in the shorting stock's list
        # --------------------------------------------------------------------------------------------------------------

        for short_stock_count, short_stock in enumerate(symbols_short_list):

            logger.info("Run the strategy's shorting logic for %s stock (stock number %s in the list of %s stocks)"
                        % (short_stock, short_stock_count+1, len(symbols_short_list)))

            logger.info('Create the contract object')
            stock_contract = Stock(short_stock, exchange, currency)
            ib.qualifyContracts(stock_contract)

            last_price, todays_total_volume, \
            latest_prevclose_change, latest_open_change = extract_quotes(stock_contract, ib)


            todays_total_volume = y_quotes(stock)[1]

            if (net_balance > 0) and all(number is not None for number in [last_price, todays_total_volume,
                                                                           latest_prevclose_change, latest_open_change]):

                # extract the net balance after executing for every stock
                net_balance = extract_net_balance(ib, 0)

                # check for the required conditions
                logger.info('Check for the required balance, price and volume conditions')
                volume_condition_for_shorting = todays_total_volume > volume_limit_for_shorting

                if latest_prevclose_change > latest_prevclose_change_limit_for_shorting:
                    if net_balance > last_price:
                        logger.info('Compute the quantity to trade')
                        if net_balance > net_balance_limit_1:
                            qty_to_short = int(net_balance_limit_1/last_price)
                        else:
                            qty_to_short = int(net_balance/last_price)

                        if volume_condition_for_shorting and qty_to_short is not None and (qty_to_short > 0):
                            logger.info('Apply the strategy logic after all the conditions are satisfied')
                            short_order(short_stock, stock_contract, "SELL", latest_prevclose_change, stop_param,
                                        last_price, qty_to_short, take_profit_percentage,
                                        stop_limit_percentage, cancel_order_time, ib)
                            logger.info('Logic executed for %s stock' % short_stock)
                        else:
                            logger.info('Volume condition failed or qty is zero/None. Moving to the next stock')

                logger.info('Moving to the next stock')

                # extract the net balance after checking/executing for every stock
                net_balance = extract_net_balance(ib, 0)
                logger.info('Net balance is %s' % net_balance)
            else:
                logger.info('Incomplete data for %s, moving to the next stock.' % short_stock)

        logger.info('Disconnect the IB connection after the logic has been run on all the stocks')
        ib.disconnect()

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error(''.join('!! ' + line for line in lines))
        os.unlink(pidfile)

    finally:
        if os.path.exists(pidfile):
            os.unlink(pidfile)



