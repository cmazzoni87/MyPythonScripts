##Claudio Mazzoni Apr 7th.
##Class Module used to format dataframes into workable arrays and run machine learning analysis
##Only regression, have to add will add more
##Early draft of a machine learning application that runs different types of regression analysis using
## stock data and basic technical analysis as features
import sqlalchemy
import pandas as pd
from sklearn import model_selection
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.tseries.offsets import *
import numpy as np



class Algorithms:
    def __init__(self):
        pass

    def get_data(self, each_dir, forecast_col):
        filespredict = sqlalchemy.create_engine(
        'mssql+pyodbc://$$$$$$$/Stock_Hist_tseries_py?driver=ODBC+Driver+13+for+SQL+Server', echo=False)
        to_sql_server = pd.read_sql_query(sql="SELECT * FROM dbo." + each_dir +
                                "_Historical ORDER BY [Dates] DESC", con=filespredict).iloc[::-1]
        to_sql_server = to_sql_server.set_index('Dates')
        y_df = to_sql_server[forecast_col]
        x_df = to_sql_server.drop([forecast_col], 1)
        return x_df, y_df, to_sql_server
    # Linear regression
    def get_Linear_Regression(self, each_dir, forecast_col, look_back):
        x, y, allData= self.get_data(each_dir, forecast_col)
        X_train, X_test, y_train, y_test = model_selection.train_test_split(x, y, test_size=0.33)
        model = LinearRegression()
        model.fit(X_train, y_train)
        return model, X_test, y_test, allData

    def trend(self,df):
        df = df.copy().sort_index()
        dates = df.index.to_julian_date().values[:, None]
        x = np.concatenate([np.ones_like(dates), dates], axis=1)
        y = df.values
        return pd.DataFrame(np.linalg.pinv(x.T.dot(x)).dot(x.T).dot(y).T,
                            df.columns, ['Constant', 'Trend'])


if __name__ == "__main__":
    filespredict = sqlalchemy.create_engine(
        'mssql+pyodbc://$$$$$$$/Stock_Hist_tseries_py?driver=ODBC+Driver+13+for+SQL+Server', echo=False)
    stock_list = pd.read_sql_query(sql="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
                      , con=filespredict)
    for each_dir in stock_list['TABLE_NAME']:
        ticket = each_dir.split('_')[0]
        model, x_test, y_test, allData = Algorithms().get_Linear_Regression(ticket, 'Adj. Close', 20)
        confidence = model.score(x_test, y_test)
        print ticket, ': ',confidence
        if confidence > .97:
            intrcpt = model.intercept_
            coef = model.coef_
            predictions = model.predict(x_test)
            features_coef = pd.DataFrame(coef,allData.drop(["Adj. Close"], 1).columns,columns=['Coeff'])
            plt.scatter(y_test,predictions,c='black')
            plt.ylabel('Actual Closing Values')
            plt.xlabel('Predicted Closing Values')
            plt.show()
            sns.distplot((y_test-predictions))
            plt.show()
            timer = pd.date_range(str(allData.index[-1]), periods=predictions.size, freq=BDay())
            set_plot = pd.DataFrame(predictions,columns=['Predictions']).set_index(timer)
            coef = Algorithms().trend(set_plot)
            set_plot['Trend'] = (coef.iloc[0, 1] * set_plot.index.to_julian_date() + coef.iloc[0, 0])
            set_plot.plot(style=['.', '-'])
            # sns.regplot(x=range(predictions.size), y=set_plot['Predictions'], data=set_plot)
            plt.show()
