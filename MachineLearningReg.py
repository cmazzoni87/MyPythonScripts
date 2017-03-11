#Claudio Mazzoni Feb 5th, 2017
#Machine Learning tools used along passed data with the goal of return predictions
#Stock prices are gathered using another sript I wrote and that is what this one uses to return the predictions
#Tools also compute confidence on the models and percentage of error
#Tool can also return forecast data that match or criteria
####Inspired by: https://github.com/akshaynagpal/beancoder_examples/blob/master/linear_reg_article/linear_regression.py

import math, time, os
import numpy as np
import pandas as pd
from sklearn import preprocessing, cross_validation, svm
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import datetime

def csv_predictions(index_folder = "YOUR FOLDERPATH"):
    forecast_col = "Adjusted Close" #Select which column to predict
    stock_list = pd.read_csv('index_folder', header=None)
    df_results = pd.DataFrame()
    for each_dir in stock_list[0]: #LOOP CSV FOR THE STOCK TICKER
        if each_dir == 'Ticker':
            continue
        filespredict = 'index_folder' + each_dir +'.csv'
        if not os.path.exists(filespredict):
            continue
        df = pd.read_csv(filespredict, header=0,index_col = 0)
        forecast_out = int(math.ceil(0.01 * len(df)))
        df['label'] = df[forecast_col].shift(-forecast_out)
        newdf = df.copy(deep=True)
        newdf.dropna(inplace=True)
        X = np.array(df.drop(['label'], 1))   #WE GET OUR FEATURES
        X = preprocessing.scale(X)
        X_lately = X[-forecast_out:]
        X = X[:-forecast_out]
        y = np.array(newdf['label'])
        X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.2) #GETTING OUR TEST AND TRAIN DATA

        clf = LinearRegression(n_jobs=-1)
        clf.fit(X_train, y_train)
        confidence = clf.score(X_test, y_test)
        if confidence > 0.985:
            print confidence, each_dir
            forecast_set = clf.predict(X_lately)
            df[ each_dir + ' Forecast'] = np.nan
            last_date = datetime.datetime.strptime(df.iloc[-1].name + " 00:00:00", "%Y-%m-%d %H:%M:%S") #FORMAT DATA
            last_unix = time.mktime(last_date.timetuple())
            one_day = 86400
            next_unix = last_unix + one_day
            for i in forecast_set:
                next_date = datetime.datetime.fromtimestamp(next_unix)
                next_unix += 86400
                df.loc[next_date] = [np.nan for _ in range(len(df.columns) - 1)] + [i]
            df_results = pd.concat([df_results,df[each_dir + ' Forecast']], axis=1)
    df_results.dropna(inplace=True)
    return df_results
