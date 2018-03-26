"""
Created on Tue Aug 22 14:52:58 2017
@author: claudio.mazzoni
"""
#Simple way of fetching data from mongo without having to write mongo fromatted queries

from  pymongo import MongoClient
from  pymongo.errors import ConnectionFailure, OperationFailure
import urllib.parse
import pandas as pd
import datetime



class MongoConnect:
    def __init__():
        pass

    def FetchCollection(usrnm, pswr, serverid, datbase, colleNm):
        #user and pass are kept in method FetchCollecion
        username = urllib.parse.quote_plus(usrnm)
        password = urllib.parse.quote_plus(pswr)    
        try:
            client = MongoClient('mongodb://%s:%s@%s' %
                                      (username, password,serverid),
                                      readPreference='secondaryPreferred')  #Create Connection

            db = client[datbase]    #Select our database
            db.authenticate(usrnm,pswr)     #for us to get data we need to double Authenticate
            collection = db[colleNm]        #fetch the collection we want
            return collection
        except (ConnectionFailure, OperationFailure):
            return 'Opearion is invalid'

           
    def QueryConsructor(collection, columns_=[], conditions_= [], gt_lt =[], or_and ='',reg_exp =[]):

        qry = {}        #Declare dictionary formatted query
        try:
            if len(columns_) > 1:       #Query criteria involves more than one column
                if or_and != '':
                    qry = {or_and:[]}
                    qry[or_and] = [{columns_[i]:(conditions_[i] if type(conditions_[i]) != type([])
                    else {'$in':conditions_[i]})} for i in range(len(columns_))]
                    if len(gt_lt) > 0 and len(gt_lt) % 3==0:        #Less Greater criteria
                        cond = {gt_lt[i + 2]:gt_lt[i + 1] for i in range(len(gt_lt)) if i % 3 == 0}
                        qry[or_and][columns_.index(gt_lt[0])][gt_lt[0]] = cond 

                    if len(reg_exp) > 0 and len(reg_exp) % 2==0:
                        cond = {'$regex':reg_exp[i] for i in range(len(reg_exp)) if i % 2 == 0 or i == 1}
                        qry[reg_exp[0]] = cond

                else:     
                    qry_2 = {columns_[i]:(conditions_[i] if type(conditions_[i]) != type([])
                    else {'$in':conditions_[i]}) for i in range(len(columns_))}
                    qry.update(qry_2)
                    if len(gt_lt) > 0 and len(gt_lt) % 3==0:
                        cond = {gt_lt[i + 2]:gt_lt[i + 1] for i in range(len(gt_lt)) if i % 3 == 0}
                        qry[gt_lt[0]] = cond  

                    if len(reg_exp) > 0 and len(reg_exp) % 2==0:
                        cond = {'$regex':reg_exp[i] for i in range(len(reg_exp)) if i % 2 == 0 or i == 1}
                        qry[reg_exp[0]] = cond

            elif len(columns_) == 1:
                qry.update({columns_[0]:conditions_[0]})
                if len(gt_lt) > 0 and len(gt_lt) % 3==0:
                    cond = {gt_lt[i + 2]:gt_lt[i + 1] for i in range(len(gt_lt)) if i % 3 == 0}
                    qry[gt_lt[0]] = cond

                if len(reg_exp) > 0 and len(reg_exp) % 2==0:
                    cond = {'$regex':reg_exp[i] for i in range(len(reg_exp)) if i % 2 == 0 or i == 1}
                    qry[reg_exp[0]] = cond

            result = collection.find(qry)
            print(qry)
            return result

        except (TypeError, KeyError, IndexError) as e:
            return 'ERROR ' + e
    

if __name__ == "__main__":

    collection = MongoConnect.FetchCollection('mylogin@mylogin',
                                               'P@ssw0rd','111.222.333.444',
                                               'Dashboard', 'ProcessDetail')
  # Clauses types are passed by placing placeholder variables and then passing them as optional parameters.
    qry_results = MongoConnect.QueryConsructor(collection,            
                                        ['ClientId','ProcessDt','Year'],
                                        ['188','placeholder',2016],
                                        gt_lt = ['ProcessDt',datetime.datetime(2017, 9, 25, 0),'$gt',
                                         'ProcessDt',datetime.datetime(2017, 9, 26, 23),'$lt'])

    if qry_results != 'ERROR':
        df=pd.DataFrame(list(qry_results))
        print(df)
    else:
        print('Invalid Query')
