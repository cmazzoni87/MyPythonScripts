#Claudio Mazzoni Feb 5th, 2017
#Stock data from various sources [Yahoo, Google, etc] via Quandl
#For values in British pounds I convert the prices into USD 

import pandas as pd
import os, quandl, time
from forex_python.converter import CurrencyRates
import datetime

def key_statistics(strt_dtate, end_dtate, sources = "WIKI",const_api = "YOUR API KEY",
                   path = "YOUR FOLDER", stock_index = "SnP"):
    fullpath = path + stock_index + '_indx.csv'
    stock_list = pd.read_csv(fullpath)
    if not os.path.isdir(path + stock_index + '_Results'):
        os.makedirs(path + stock_index + '_Results')
    if stock_index == "FTSE":
        sources = "LSE"
        c = CurrencyRates()
        st_close = 'Last Close'
        st_high = 'High'
        st_low = 'Low'
        st_volume = 'Volume'
    else:
        st_close = 'Adj. Close'
        st_high = 'Adj. High'
        st_low = 'Adj. Low'
        st_volume = 'Adj. Volume'
        print st_close

    for each_dir in stock_list['Ticker']:
        try:
            ticker = each_dir.split("\\")[-1]
            print ticker
            name = sources + '/' + ticker.upper()
            df =quandl.get(name, trim_start=strt_dtate,
                  trim_end=end_dtate,
                  authtoken=const_api)  #yyyy-mm-dd
            if stock_index == "FTSE":
                currency = c.get_rates('GBP')
                df[st_close] = [row * currency[u'USD'] for row in df[st_close]]
                df[st_low] = [row * currency[u'USD'] for row in df[st_low]]
                df[st_high] = [row * currency[u'USD'] for row in df[st_high]]
                df['fake_open'] = df[st_close].shift(1)
                df['PCT_change'] = (df[st_close]-df['fake_open'])/df['fake_open']  #[(row/100) for row in df['Change']]
            else:
                df['PCT_change'] = (df[st_close] - df['Adj. Open']) / df['Adj. Open']

            df['HL_PCT'] = (df[st_high] - df[st_low]) / df[st_close]
            df['Rolling Mean'] = df[st_close].rolling(window=6, center=False).mean()
            df['Boll B Upper'] = df['Rolling Mean'] + (df[st_close].rolling(window=6, center=False).std() * 2)  # 2 is the number of STD
            df['Boll B Lower'] = df['Rolling Mean'] - (df[st_close].rolling(window=6, center=False).std() * 2)
            df.fillna(-99999, inplace=True)
            df = df[[st_close, 'HL_PCT', 'PCT_change', st_volume, 'Rolling Mean', 'Boll B Upper', 'Boll B Lower']]
            if df.empty == False:
                df.to_csv(path + stock_index + '_Results' + "\\" + ticker + ".csv")
        except Exception as e:
            print(str(e))
            time.sleep(5)
            try:
                ticker = each_dir.split("\\")[-1]
                print ticker
                name = sources + '/' + ticker.upper()
                df = quandl.get(name, trim_start=strt_dtate,
                                trim_end=end_dtate,
                                authtoken=const_api)  # yyyy-mm-dd
                if stock_index == "FTSE":
                    currency = c.get_rates('GBP')
                    df[st_close] = [row * currency[u'USD'] for row in df[st_close]]
                    df[st_low]= [row * currency[u'USD'] for row in df[st_low]]
                    df[st_high]= [row * currency[u'USD'] for row in df[st_high]]
                    df['fake_open'] = df[st_close].shift(1)
                    df['PCT_change'] = (df[st_close]-df['fake_open'])/df['fake_open'] # (row/100) for row in df['Change']]
                else:
                    df['PCT_change'] = (df[st_close] - df['Adj. Open']) / df['Adj. Open']

                df['HL_PCT'] = (df[st_high] - df[st_low]) / df[st_close]
                df['Rolling Mean'] = df[st_close].rolling(window=6, center=False).mean()
                df['Boll B Upper'] = df['Rolling Mean'] + (df[st_close].rolling(window=6, center=False).std() * 2)  # 2 is the number of STD
                df['Boll B Lower'] = df['Rolling Mean'] - (df[st_close].rolling(window=6, center=False).std() * 2)
                df.fillna(-99999, inplace=True)
                df = df[[st_close, 'HL_PCT', 'PCT_change', st_volume, 'Rolling Mean', 'Boll B Upper', 'Boll B Lower']]
                df.to_csv(path + stock_index + '_Results' + "\\" + ticker + ".csv")
            except Exception as e:
                print(str(e))

# if __name__ == "__main__":
#     a = key_statistics(strt_dtate = "2014-03-20", end_dtate = "2017-03-24")
#     print("DONE!: ", a)
