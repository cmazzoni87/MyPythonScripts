##Claudio Mazzoni March 25th, 2017. Stock data from Yahoo Finance:
## Class Module used to create our analysis using our data extractions
## will add more

import pandas as pd

class Stock_Technicals:

    def __init__(self,Indexes, Ticker):
        self.stock_data = 'C:\\Users\\cmazz\\PycharmProjects\\Investment_Project\\' + Indexes + '_Results\\' + Ticker + '.csv'
        #self.yahoo = yh.Share(Ticker)
        self.df = pd.read_csv(self.stock_data, header=0, index_col=0)

    def lcl_st_data(self,col_close,col_volume):
        time_series = self.df[col_close]
        mnth_volume_change = pd.Series.mean((self.df[col_volume].tail(30) / (self.df[col_volume].tail(30).shift(1)) - 1))
        volatility = self.df['PCT_change'].tail(252).std()
        return  mnth_volume_change, volatility, time_series

    def yahoo_st_data(self,col_close,col_volume):
        yr_low = self.df[col_close].tail(252).min() #self.yahoo.get_year_low()
        yr_high = self.df[col_close].tail(252).max() #self.yahoo.get_year_high()
        yr_volume = self.df[col_volume].tail(252).mean() #self.yahoo.get_avg_daily_volume()
        return yr_low, yr_high, yr_volume

    def lcl_st_close(self,col_close):
        adj_close = float(self.df[col_close].tail(1).values)
        return adj_close

    def lcl_st_prct(self):
        prc_change = float(self.df['PCT_change'].tail(1).values)
        return prc_change
