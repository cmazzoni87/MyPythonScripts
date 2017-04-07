#Claudio Mazzoni Feb 5th, 2017
#Web Scrape lists of stocks such as index from the web (exp Wikipedia)
#Able to filter by industry sector.
#Added FTSE 100. Newer version also appends the last close value and percent change to the output
import urllib2
import pandas as pd
from bs4 import BeautifulSoup
import GetTicketData as gtd

#https://en.wikipedia.org/wiki/NASDAQ-100#Components

def wikipedia_snp(sp500fld = "******s\\****_Project", to_CSV = True, tic_index = 'DJ'):
    if tic_index == 'SnP':
        site = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sector_n = 3
        companyname_m = 1
        ticker_n = 0
    elif tic_index == 'DJ':
        site = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
        sector_n = 3
        companyname_m = 0
        ticker_n = 2
    elif tic_index == "FTSE":
        site = "https://en.wikipedia.org/wiki/FTSE_100_Index"
        sector_n = 2
        companyname_m = 0
        ticker_n = 1
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib2.Request(site, headers=hdr)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        try:
            col = row.findAll('td')
            if len(col) > 0:
                sector = str(col[sector_n].string)
                companyname = str(col[companyname_m].string.strip())
                ticker = str(col[ticker_n].string.strip())
                if ticker[-1] == ".":
                    ticker = ticker[:-1]
                if ticker not in sector_tickers:
                    sector_tickers[ticker] = list()
                sector_tickers[ticker].append(companyname)
                sector_tickers[ticker].append(sector)
        except AttributeError, e:
            print "Error:", e
            continue
    df = pd.DataFrame.from_dict(sector_tickers, orient='index', dtype=None)
    df.index.name = "Ticker"
    df.columns = ["Company Name", "Industry"]

    if to_CSV == True:
        df.to_csv(sp500fld + "\\" + tic_index + "_indx.csv")
    else:
        return df
    append_numbers(tic_index, sp500fld + "\\" + tic_index + "_indx.csv")

def append_numbers(tic_index, marker):
    indx = pd.read_csv(marker, index_col="Ticker")
    list = []
    list2 = []
    if tic_index == 'FTSE':
        col_close = 'Last Close'
        #col_volume = 'Volume'
    else:
        col_close = 'Adj. Close'
        #col_volume = 'Adj. Volume'
    for each_tick in indx.index:
        print each_tick
        try:
            list.append(gtd.Stock_Technicals(tic_index,each_tick).lcl_st_close(col_close))
            list2.append(gtd.Stock_Technicals(tic_index,each_tick).lcl_st_prct())
        except IOError, e:
            list.append("NO DATA")
            list2.append("NO DATA")
    indx["Price"] = pd.Series(list, index=indx.index)
    indx["% Change"] = pd.Series(list2, index=indx.index)
    indx.to_csv(marker)
