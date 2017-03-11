# Claudio Mazzoni Dec 10, 2016      Api call to get housing Data
import CONS_file, requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil import relativedelta

def myRealstateFunction(mystate='NV', myspread='monthly', stdate='2006-03-31', json_lastday='', run_avchange=True):               #sprd_by='monthly' # DEFAULT sprd_by='quarterly'#sprd_by='annual'
    for state in mystate:
        results = requests.get(CONS_file.passparam(state, sprd_by=myspread, dt_start=stdate)).json()
        if json_lastday == '': json_lastday = results['dataset']['end_date']            #if end date exist use that else use last day available
        json_vals = [r.pop(1) for r in results['dataset']['data']]                  #Filters the date column [0] (r.pop) and returns a list of numeric values
        pird_spread = countMonths(date_start=stdate, date_end=json_lastday)           #gets number of months #Returns int
        dates = pd.date_range(start=stdate, periods=pird_spread, freq='MS')
        dta_frame = pd.DataFrame(json_vals[::-1],index=dates, columns=[state])            #we flip the dictionary to match with time [::-1]
        dfrows_count = int((len(dta_frame.index)) / 10)                                 # Calculate the window of the moving average
        dta_frame['Moving Average'] = dta_frame.rolling(window=dfrows_count).mean()     # Moving Average Column creation

        if run_avchange==True:                                                           #if true we add percent change subplot to the chart
            results_chngpercent = requests.get(CONS_file.passparam(state, sprd_by=myspread, dt_start=stdate, transform=True)).json()
            percent_change = pd.DataFrame([r.pop(1) for r in results_chngpercent['dataset']['data'][::-1]], columns=['Percent Change'])
            fig, axes = plt.subplots(nrows=2, ncols=1, gridspec_kw={'height_ratios':[3, 1]})
            ax = dta_frame.plot(colormap='jet', title=results['dataset']['name'] + ' (' + myspread + ')', ax=axes[0])
            pch = percent_change.plot(ax=axes[1], title='Percent of Change')
            pch.legend(loc='upper center', prop={'size':13},bbox_to_anchor=(0.5, -0.11),
                fancybox=True, shadow=True, ncol=5)
        else:
            ax = dta_frame.plot(colormap='jet', title=results['dataset']['name'])  # we plot the results
            ax.set_xlabel('Length of Time = ' + myspread)
        ax.set_ylabel('Amount in Thousands')
        ax.legend(fancybox=True, shadow=True)
    plt.show()


def countMonths(date_start='1987-11-22', date_end='2001-07-29'):            #returns number of months
    date_start = datetime.strptime(str(date_start), '%Y-%m-%d')
    date_end = datetime.strptime(str(date_end), '%Y-%m-%d')
    r = relativedelta.relativedelta(date_end, date_start)
    yrs = r.years * 12
    mth = r.months
    mth_count = yrs + mth + 1
    return mth_count

myRealstateFunction(mystate=['NY', 'NEWNY'])
