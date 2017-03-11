#Claudio Mazzoni Feb 5th, 2017
#Web Scrape lists of stocks such as index from the web (exp Wikipedia) S&P Index, Dow Jones Industrial
#Able to filter by industry sector
import urllib2
import pandas as pd
from bs4 import BeautifulSoup


def wikipedia_snp(sp500fld = "C:\\Users\cmazz\PycharmProjects\\Investment_Project", to_CSV = True, tic_index = 'DJ'):
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
    else:
        site = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sector_n = 3
        companyname_m = 1
        ticker_n = 0
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib2.Request(site, headers=hdr)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[sector_n].string) #.strip()).lower().replace(' ', '_') #To  rid of empty spaces
            companyname = str(col[companyname_m].string.strip())
            ticker = str(col[ticker_n].string.strip())
            if ticker not in sector_tickers:
                sector_tickers[ticker] = list()
            sector_tickers[ticker].append(companyname)
            sector_tickers[ticker].append(sector)
    df = pd.DataFrame.from_dict(sector_tickers, orient='index', dtype=None)
    df.index.name = "Ticker"
    df.columns = ["Company Name", "Industry"]
    if to_CSV == True and tic_index == 'SnP':
        df.to_csv(sp500fld + "\\" + "S&P_indx.csv")
    elif to_CSV == True and tic_index == 'DJ':
        df.to_csv(sp500fld + "\\" + "DOWJOWNS_indx.csv")
    else:
        return df
