# Claudio Mazzoni Sep 25th, 2017
# Extract market data from quandl and googlefinance.client library
# Get Technical indicators on demand

import pandas as pd
import quandl as ql
import googlefinance.client as gfc
from quandl.errors.quandl_error import NotFoundError, AuthenticationError, InvalidRequestError


class doAnalitica():

    def __init__(self, ticker, dtfrom, dtto, indxlib):
        self.myhushhushkey = 'YOUAPIKEYPLEASE'
        self.ticker = ticker
        self.dtfrom = dtfrom
        self.dtto = dtto
        try:
            self.df = ql.get("WIKI/{}".format(self.ticker), trim_start=dtfrom,
                                                trim_end=dtto, authtoken=self.myhushhushkey)
        except (NotFoundError, AuthenticationError, InvalidRequestError):
            print('Invalid request: \nCheck credentials and/or params')
            quit()
        #risk free returns for risk analysis comming soon
        self.rf = ql.get("USTREASURY/BILLRATES", trim_start=dtfrom,
                                            trim_end=dtto, authtoken=self.myhushhushkey)
        self.snp = gfc.get_price_data(indxlib)

    def simple_mov_average(self, lag, field):
        data = self.df
        simple_moving = data[field].rolling(window=lag, center=False).mean()
        return simple_moving

    def expo_mov_average(self, field, spn, periods):
        data = self.df
        expo_moving = data[field].ewm(span=spn, min_periods=periods).mean()
        return expo_moving

    def MACD(self, fields, expo_m1=12, expo_m2=26, plot_xpo=9):
        """
        MACD Line: (12-day EMA - 26-day EMA)
        Signal Line: 9-day EMA of MACD Line
        MACD Histogram: MACD Line - Signal Line
        """
        data = self.df
        field = data[fields]
        a = self.expo_mov_average(field=data[field], spn=expo_m1, periods=plot_xpo)
        b = self.expo_mov_average(field=data[field], spn=expo_m2, periods=plot_xpo)
        #MACD Line
        macd = a - b
        return macd

    def stochastic_oscillator(self, ts=14):
        """
        %K = 100(C - L14)/(H14 - L14)
        Where:
        C = the most recent closing price
        L14 = the low of the 14 previous trading sessions
        H14 = the highest price traded during the same 14-day period
        %K= the current market rate for the currency pair
        %D = 3-period moving average of %K
        """
        L14 = self.df['Adj. Low'].rolling(window=ts).min()
        H14 = self.df['Adj. High'].rolling(window=ts).max()
        prct_K = (100 * self.df['Adj. Close'] - L14)/(H14-L14)
        prct_D = prct_K.rolling(window=3, center=False).mean()
        prct_K = prct_K.dropna()
        prct_D = prct_D.dropna()
        return prct_K, prct_D

    def relative_strength_index(self):
        """
        RSI = 100 - 100 / (1 + RS)
        """
        pass

    def volatility(self, lag, tag):
        data = self.df[tag]
        vol = data.rolling(window=lag, center=False).std()
        return vol

    def momentum(self, lag, tag):
        mo = self.df[tag] - self.df[tag].diff(lag)
        MO = mo.dropna()
        return MO

    def force_index(self, period=1):
        data = self.df['Adj. Close']
        fi = data - data.diff(period) * self.df['Adj. Volume']
        FI = fi.dropna()
        return FI

    def boillinger_bands(self, lag):
        data = self.df
        sma = data['Adj. Close'].rolling(window=lag, center=False).mean()
        stdev = data['Adj. Close'].rolling(window=lag, center=False).std() * 2
        BBUpper = sma + stdev
        BBLower = sma - stdev
        return BBUpper, BBLower


    # def getFundamentals(self):
    #     pass
if __name__ == "__main__":
  ##Sample Run and fetch Stochastic Oscillator
    paramSNP = \
    {
        'q': ".INX",  # Stock symbol (ex: "AAPL")
        'i': "1800",  # Interval size in seconds ("86400" = 1 day intervals)
        'x': "INDEXSP",  # Stock exchange symbol on which stock is traded (ex: "NASD")
        'p': "1Y"  # Period (Ex: "1Y" = 1 year)
    }
    a = doAnalitica('AAPL', '2018-01-01', '2018-03-19', paramSNP)
    b, c = a.stochastic_oscillator(14)
    print(b, c)
