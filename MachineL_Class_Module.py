##Claudio Mazzoni Apr 7th.
##Class Module used to format dataframes into workable arrays and run machine learning analysis
##currently only regression will add more

import time , math
import numpy as np
import pandas as pd, datetime
from sklearn import preprocessing, cross_validation
from sklearn.linear_model import LinearRegression

class Machine_Learning_Algorithms:

    def __init__(self):
        pass

    def get_model(self,each_dir, fldr, forecast_col,lookback):
        filespredict = 'R:\******\\***_Project\\' \
                       + fldr + '_Results\\' + each_dir + '.csv'
        df = pd.read_csv(filespredict, header=0, index_col=0)
        forecast_out = int(math.ceil(0.001 * len(df)))
        df['label'] = df[forecast_col].shift(-forecast_out)
        newdf = df.copy(deep=True)
        newdf.dropna(inplace=True)
        X = np.array(df.drop(['label'], 1))
        X = preprocessing.scale(X)
        X_lately = X[-lookback:]
        X = X[:-forecast_out]
        y = np.array(newdf['label'])
        last_date = df.iloc[-1].name
        return X, y, X_lately, last_date

    def get_regression(self,X, y, X_lately):
        X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.2)
        clf = LinearRegression(n_jobs=-1)
        fitness = clf.fit(X_train, y_train)
        confidence = clf.score(X_test, y_test)
        forecast_set = clf.predict(X_lately)
        return confidence, fitness, forecast_set

    def assemble_timeseries(self,forecast_set, col_name, last_day):
        df = pd.DataFrame()
        df[col_name + ' Forecast'] = np.nan
        last_date = datetime.datetime.strptime(last_day +   # df.iloc[-1].name +
                                               " 00:00:00", "%Y-%m-%d %H:%M:%S")
        last_unix = time.mktime(last_date.timetuple())
        one_day = 86400
        next_unix = last_unix + one_day
        for i in forecast_set:
            next_date = datetime.datetime.fromtimestamp(next_unix)
            while next_date.isoweekday() > 5:  # skip weekends
                next_unix += 86400
                next_date = datetime.datetime.fromtimestamp(next_unix)
            next_unix += 86400
            df.loc[next_date] = [np.nan for _ in range(len(df.columns) - 1)] + [i]
        return df
