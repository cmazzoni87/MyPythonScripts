#Claudio Mazzoni Feb 5th, 2017
#Stock data from various sources [Yahoo, Google, etc] via Quandl


import pandas as pd
import os, quandl, time

def key_statistics(strt_dtate, end_dtate, sources = "YAHOO",const_api = "Your Personal API KEY"
                   sp500fld_p ="YOUR FOLDER",
                   path = "YOUR PATH", stock_index = 1):

    if stock_index == 1:
        stock_list = pd.read_csv(path + 'S&P_indx.csv', header=None)
    else:
        stock_list = pd.read_csv(path + 'DOWJOWNS_indx.csv', header=None)
    os.makedirs(sp500fld_p + timstp)
    for each_dir in stock_list[0]:
        try:                                          
            ticker = each_dir.split("\\")[-1]
            name = sources + '/' + ticker.upper()
            df =quandl.get(name, trim_start=strt_dtate,
                  trim_end=end_dtate,
                  authtoken=const_api)  #yyyy-mm-dd
            df['HL_PCT'] = (df['High'] - df['Low']) / df['Close'] * 100.0
            df['PCT_change'] = (df['Adjusted Close'] - df['Open']) / df['Open'] * 100.0
            df['Rolling Mean'] = df['Adjusted Close'].rolling(window=6, center=False).mean()
            df['Boll B Upper'] = df['Rolling Mean'] + (df['Adjusted Close'].rolling(window=6, center=False).std() * 2)  # Computing Bolinger bands
            df['Boll B Lower'] = df['Rolling Mean'] - (df['Adjusted Close'].rolling(window=6, center=False).std() * 2)
            df.fillna(-99999, inplace=True)
            df = df[['Adjusted Close', 'HL_PCT', 'PCT_change', 'Volume', 'Rolling Mean', 'Boll B Upper', 'Boll B Lower']]
            df.to_csv(sp500fld_p + timstp +"\\" + ticker + ".csv")
        except Exception as e:
            print(str(e))               #incase we lose connection or we fail to access the data we wait and try again
            time.sleep(5)
            try:
                ticker = each_dir.split("\\")[-1]
                name = sources + '/' + ticker.upper()
                df = quandl.get(name, trim_start=strt_dtate,
                                trim_end=end_dtate,
                                authtoken=const_api)  # yyyy-mm-dd
                df['HL_PCT'] = (df['High'] - df['Low']) / df['Close'] * 100.0
                df['PCT_change'] = (df['Adjusted Close'] - df['Open']) / df['Open'] * 100.0
                df['Rolling Mean'] = df['Adjusted Close'].rolling(window=6,center=False).mean()
                df['Boll B Upper'] = df['Rolling Mean'] + (df['Adjusted Close'].rolling(window=6,center=False).std() * 2) #2 is the number of STD
                df['Boll B Lower'] = df['Rolling Mean'] - (df['Adjusted Close'].rolling(window=6, center=False).std() * 2)
                df.fillna(-99999, inplace=True)
                df = df[['Adjusted Close', 'HL_PCT', 'PCT_change', 'Volume', 'Rolling Mean']]
                df.to_csv(sp500fld_p + timstp + "\\" + ticker + ".csv")
            except Exception as e:
