from textblob import TextBlob
from textblob.classifiers import NaiveBayesClassifier
from textblob import Blobber
from nltk import sent_tokenize
from sklearn.utils import shuffle
import pandas as pd
import pickle
import numpy as np
import os
__author__ = 'Claudio Mazzoni'

# Text Classifier, user can choose between textblob pre trained model Blobber 
# or train his/her own using a NaiveBayerClassifier model 
# class also contains usefull methods such as save, load & test models
# Model takes two text files with possitive and negative senteces placed on two folders labeled neg & pos

class model_nltk():

    def __init__(self, corpora_paths, traintest_split):
        self.corp_paths = corpora_paths
        data_set = pd.DataFrame(columns=['Article', 'Polarity'])
        for paths in self.corp_paths:
            for files in os.listdir(paths):
                print(files)
                pandas_df = pd.read_table(paths + '\\' + files)
                craptopass = [[line, paths.split('\\')[-1]] for line in pandas_df['head'].tolist()]
                data_set = data_set.append(pd.DataFrame(craptopass, columns=['Article', 'Polarity']), ignore_index=True)

        #  Shuffle the data before train test splitting
        self.data_set_pd = shuffle(data_set)
        self.data_set_list = self.data_set_pd[['Article', 'Polarity']].values.tolist()
        self.lenght = len(self.data_set_list)
        train_index = int(traintest_split * self.lenght)  # int(0.60 * lenght)
        # split the data into a train and test data
        self.train, self.test = self.data_set_list[:train_index], self.data_set_list[train_index:]

    def model_train(self, nbc=True):
        if nbc is True:
            print('Initiating Training')
            train_set = np.array(self.train)
            cl = NaiveBayesClassifier(train_set)
            print('Training Complete Accuracy = {}'.format(cl.accuracy(self.test)))
        else:
            cl = Blobber()
            # self.data_set_pd[['pattern_polarity', 'pattern_subj']] = self.data_set_pd.text.apply(
            #     lambda v: pd.Series(tb(v).sentiment))
        return cl

    def model_test(self, text_feed, cl):
        if cl == Blobber():
            blob = cl(text=text_feed)
        else:
            blob = TextBlob(text_feed, classifier=cl)
        return blob

    def save(self, save_name, cl):
        pickled_classifier_file = open('{}.obj'.format(save_name), 'wb')
        pickle.dump(cl, pickled_classifier_file)
        pickled_classifier_file.close()
        print('Model saved sucessfully')

    def load(self, picke_path):
        cl = pickle.load(open(picke_path, 'rb'))
        return cl


if __name__ == "__main__":

    sample = 'Despite Colgates efforts, stocks prices fell sharply'
    root = '...\\stock_reviews\\'
    data_paths = [root + 'neg', root + 'pos']
    model_class = model_nltk(data_paths, 0.70)
    trained = model_class.model_train()
    print(trained.show_informative_features())
    test = model_class.model_test(sample, trained)
    print(test.sentiment)
    model_class.save('Model_Save.obj', trained)
