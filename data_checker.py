__author__='Claudio Mazzoni'
import pandas as pd
import numpy as np


class DataframeChecker():
    def __init__(self, data_for, data_against):
        self.dfr = data_for
        self.dat = data_against

    def __check_dimensions(self, row_count):
        results = {}
        dfr_cols = self.dfr.columns.tolist()
        dat_cols = self.dat.columns.tolist()
        dfr_cols.sort()
        dat_cols.sort()
        if dfr_cols != dat_cols: # check for late binding
            #check if columns match
            sum = self.dfr.columns.tolist() + self.dat.columns.tolist()
            source = self.dfr.columns.tolist()
            compare = self.dat.columns.tolist()
            self.diff_cols = [value for value in sum if (value in source) ^ (value in compare)]
            results['Different Columns'] = {'Source DataFrame': [i for  i in self.diff_cols if i in source],
                                'Comparing DataFrame': [i for i in self.diff_cols if i in compare]}
        else:
            self.dat.columns = self.dfr.columns
        if row_count is True:
            #check row count
            if self.dfr.shape[0] != self.dat.shape[0] and results == {}:
                self.diff_rows = pd.concat([self.dfr, self.dat]).loc[self.dfr.index.symmetric_difference(self.dat.index)]
                results['Different Rows'] = self.diff_rows
            elif self.dfr.shape[0] != self.dat.shape[0] and results != {}:
                results['Different Rows'] = []
                for col in self.dfr.columns.tolist():
                    diffs = pd.concat([self.dfr[col],
                                       self.dat[col]]).loc[self.dfr[col].index.symmetric_difference(self.dat[col].index)]
                    results['Different Rows'][0].append(diffs)
        if results == {}:
            return True
        else:
            return results

    def check_df(self, theresholds=None, keys=None, count_rows=False):
        dimensions = self.__check_dimensions(count_rows)
        threshold = {}
        target = self.dfr.set_index(keys)
        compared = self.dat.set_index(keys)
        if theresholds is None:
            for i in self.dfr.columns.tolist():
                threshold[i] = 0
        else:
            if isinstance(theresholds, dict) is False:
                print('InvalidDataType: Expected dict got {}'.format(type(theresholds)))
                quit()
            lngth_mismtch = [i for i in theresholds.keys() if i not in self.dfr.columns.tolist()]
            if len(lngth_mismtch) > 0:
                print('ColumnMissMatch: Columns {} not found in DataFrame'.format(','.join(lngth_mismtch)))
                quit()
            other_cols = [i for i in self.dfr.columns.tolist() if i not in theresholds.keys() and i not in keys]
            threshold = theresholds
            for i in other_cols:
                del target[i]
                del compared[i]
        all_indx = list(set(target.index.tolist()) ^ set(compared.index.tolist()))
        for indx in all_indx:
            if indx not in target.index:
                target.loc[indx, target.columns] = 0
            if indx not in compared.index:
                compared.loc[indx, compared.columns] = 0
        if  dimensions is True:
            #Compere theresholds
            for col in theresholds.keys():
                if target[col].equals(compared[col]) is False:
                    if target[col].dtype == 'int64' or target[col].dtype == 'float64':
                        bool_cond_upper = target[col] > (compared[col].loc[target.index] * (1 + threshold[col]))
                        bool_cond_lower = target[col] < (compared[col].loc[target.index] * (1 - threshold[col]))
                        target.loc[bool_cond_upper, col + 'UpDif'] = compared[col] / target[col]
                        target.loc[bool_cond_lower, col + 'LwDif'] = target[col] / compared[col]
                        target[col + ' Diff'] = (target[col + 'UpDif'] + target[col + 'LwDif']).fillna(0).map("{:.2%}".format)
                        target = target.drop(columns=[col + 'UpDif', col + 'LwDif'])
                    else:
                        target[col + ' Diff'] = (target[col] == compared[col].loc[target.index])

            return dimensions, target
        else:
            return dimensions
