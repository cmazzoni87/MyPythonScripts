# Claudio Mazzoni April 23th,
# Timer based program that reads the html of the Yahoo Finance wesite, get current price
# Volume is extracted realtime from Yahoo finance API
# S&P data is extracted via googlefinace package 
# Perform Analytics and upload the data to SQL Server Managment Studio

from googlefinance import getQuotes
import time
import datetime
import pandas as pd
import quandl as ql
import sqlalchemy
import Yahoo_API_Extract as y_api           #YAHOO FINANCE API HAS MORE DATA THAN GOOGLE


def stock_listener(ticker):
    risk_free_return = ql.get("USTREASURY/BILLRATES", authtoken="ABBDkAozJF6dquecyX_m",
                              trim_start=(datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                              trim_end=(datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
    # treasury price for yesterday
    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://LAPTOP-S29GML4S/Day_Trading_py?driver=ODBC+Driver+13+for+SQL+Server', echo=False)
    timeout = datetime.time(16, 00, 00)
    if engine.dialect.has_table(engine.connect(), ticker + '_Data'):        #Check if Exist
        to_sql_server = pd.read_sql_query(sql="SELECT TOP(30) * FROM dbo." + ticker +
                                            "_Data ORDER BY convert(varchar, [Time], 109) DESC", con=engine).iloc[::-1]
        to_sql_server = to_sql_server.reset_index(drop=True) #reset index to enforce consistancy
    else:
        to_sql_server = pd.DataFrame()  # Initialize table

    while True:
        if datetime.datetime.now().time() >= timeout:
            break
        try:
            record_slice = pd.Series()
            time.sleep(5)               # let it get the data
            goog_snp_dta = getQuotes('.INX')        # S&P 500 data from GOOGLE FINANCE
            record_slice['index_price'] = float(goog_snp_dta[0]['LastTradePrice'].replace(',', ''))
            record_slice['index_prev_close'] = float(goog_snp_dta[0]['PreviousClose'].replace(',', ''))
            record_slice['Index_Volume'] = float(y_api.getme_index_data('^GSPC')['Volume'].replace(',', ''))
            record_slice['Price'] = float(y_api.parse(ticker)['Current Price'])     #FROM YAHOO FINANCE
            record_slice['Prev_Close'] = float(y_api.parse(ticker)['Previous Close'])
            record_slice['Volume'] = float(y_api.parse(ticker)['Volume'].replace(',', ''))
            record_slice['PCT_change_SnP'] = (record_slice['index_price'] - record_slice['index_prev_close']) /\
                                             (record_slice['index_prev_close'] * 100)
            record_slice['Time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            record_slice['PCT_change'] = (record_slice['Price'] - record_slice['Prev_Close']) /\
                                         (record_slice['Prev_Close'] * 100)
            record_slice['Beta'] = float((record_slice['PCT_change'] -
                                          risk_free_return['13 Wk Bank Discount Rate'].values) /
                                         float(record_slice["PCT_change_SnP"] -
                                               risk_free_return['13 Wk Bank Discount Rate'].values))
            record_slice = pd.DataFrame.transpose(pd.Series.to_frame(record_slice))
            to_sql_server = to_sql_server.append(record_slice, ignore_index=True)
            to_sql_server['Momentum'] = to_sql_server['Price'].diff(30)
            to_sql_server['SMA'] = to_sql_server['Price'].rolling(window=10, center=False).mean()
            to_sql_server['EMA'] = to_sql_server['Price'].ewm(span=10, min_periods=10).mean()
            to_sql_server['MACD'] = to_sql_server['Price'].ewm(span=12, min_periods=10).mean() - \
                                    to_sql_server['Price'].ewm(span=26, min_periods=10).mean()
            to_sql_server['Volatility'] = to_sql_server['Price'].rolling(window=10, center=False).std()
            to_sql_server['Boll B Upper'] = to_sql_server['SMA'] + (to_sql_server['Prev_Close'].rolling(
                window=10, center=False).std() * 2)
            to_sql_server['Boll B Lower'] = to_sql_server['SMA'] - (to_sql_server['Prev_Close'].rolling(
                window=10, center=False).std() * 2)
        except Exception as e:
            print e
            record_slice = pd.Series()
            time.sleep(15)               # let it get the data
            goog_snp_dta = getQuotes('.INX')        # S&P 500 data from GOOGLE FINANCE
            record_slice['index_price'] = float(goog_snp_dta[0]['LastTradePrice'].replace(',', ''))
            record_slice['index_prev_close'] = float(goog_snp_dta[0]['PreviousClose'].replace(',', ''))
            record_slice['Index_Volume'] = float(y_api.getme_index_data('^GSPC')['Volume'].replace(',', ''))
            record_slice['Price'] = float(y_api.parse(ticker)['Current Price'])     #FROM YAHOO FINANCE
            record_slice['Prev_Close'] = float(y_api.parse(ticker)['Previous Close'])
            record_slice['Volume'] = float(y_api.parse(ticker)['Volume'].replace(',', ''))
            record_slice['PCT_change_SnP'] = (record_slice['index_price'] - record_slice['index_prev_close']) /\
                                             (record_slice['index_prev_close'] * 100)
            record_slice['Time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            record_slice['PCT_change'] = (record_slice['Price'] - record_slice['Prev_Close']) /\
                                         (record_slice['Prev_Close'] * 100)
            record_slice['Beta'] = float((record_slice['PCT_change'] -
                                          risk_free_return['13 Wk Bank Discount Rate'].values) /
                                         float(record_slice["PCT_change_SnP"] -
                                               risk_free_return['13 Wk Bank Discount Rate'].values))
            record_slice = pd.DataFrame.transpose(pd.Series.to_frame(record_slice))
            to_sql_server = to_sql_server.append(record_slice, ignore_index=True)
            to_sql_server['Momentum'] = to_sql_server['Price'].diff(30)
            to_sql_server['SMA'] = to_sql_server['Price'].rolling(window=10, center=False).mean()
            to_sql_server['EMA'] = to_sql_server['Price'].ewm(span=10, min_periods=10).mean()
            to_sql_server['MACD'] = to_sql_server['Price'].ewm(span=12, min_periods=10).mean() - \
                                    to_sql_server['Price'].ewm(span=26, min_periods=10).mean()
            to_sql_server['Volatility'] = to_sql_server['Price'].rolling(window=10, center=False).std()
            to_sql_server['Boll B Upper'] = to_sql_server['SMA'] + (to_sql_server['Prev_Close'].rolling(
                window=10, center=False).std() * 2)
            to_sql_server['Boll B Lower'] = to_sql_server['SMA'] - (to_sql_server['Prev_Close'].rolling(
                window=10, center=False).std() * 2)

    print to_sql_server.iloc[31:]
    if engine.dialect.has_table(engine.connect(), ticker + '_Data'):
        to_sql_server.iloc[31:].to_sql(name=ticker + '_Data', con=engine, if_exists='append', index=False)
    else:
        to_sql_server.to_sql(name=ticker + '_Data', con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    stock_listener('MMM')
