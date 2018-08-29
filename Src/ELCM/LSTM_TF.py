import os
import random
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from collections import Counter
from datetime import datetime
from sklearn.model_selection import train_test_split

class lstm():

    def __init__(self):
        pass

    def tf_lstm_preprocessing(self):
        # read data
        df_events = pd.read_csv(str(Path(os.path.realpath(__file__)).parent.parent.parent / 'Data/LSTM_Data/events_data_for_RF.csv'), index_col=0)
        df_events = df_events.dropna(axis=0).rename(columns={"DEVICE":"device"})
        df_errors = pd.read_csv(str(Path(os.path.realpath(__file__)).parent.parent.parent / 'Data/LSTM_Data/errors_data_for_RF.csv'), index_col=0)[['device', 'date', 'Management System - Direct Stop']]
        df_merged = df_events.merge(df_errors, how='left', on=['device','date'])
        df_merged.sort_values(by=['device','date'], inplace=True)
        agvs = df_merged.device.unique()
        groupby_agv = {}
        x_abnormal = []
        x_normal =[]
        y_normal = []
        y_abnormal = []
        size_of_input = 10
        print("grouping")
        for a in agvs:
            groupby_agv[a] = df_merged[df_merged['device'] == a]
        # iterate over each agv
        # abnormal
        print("abnormal")
        for agv in groupby_agv.keys():
            last_position = None
            for index, row in enumerate(groupby_agv[agv].itertuples()):
                if index - size_of_input + 1 < 0:
                    continue
                if row[-1] == 1 and (last_position is None or index - last_position >= size_of_input):
                    x_abnormal.append(groupby_agv[agv].iloc[index - size_of_input + 1:index+1,:-2])
                    y_abnormal.append(groupby_agv[agv].iloc[index,-1])
        print("normal")
        for agv in groupby_agv.keys():
            start = None
            for index, row in enumerate(groupby_agv[agv].itertuples()):
                if row[-1] == 0:
                    if not start:
                        start = index
                    elif index - start == size_of_input - 1:
                        x_normal.append(groupby_agv[agv].iloc[start:index+1,:-2])
                        y_normal.append(groupby_agv[agv].iloc[index,-1])
                        start = None
                else:
                    start = None
        # balance the data
        random.seed(617)
        index = sorted(random.sample(range(len(x_normal)), len(x_abnormal)))
        x_normal = [x_normal[i] for i in index]
        y_normal = [y_normal[i] for i in index]
        a = [x.shape[0] for x in x_abnormal+x_normal]
        return x_abnormal + x_normal, y_abnormal + y_normal
    
    def tf_lstm(self):
        x_data, y_data = self.tf_lstm_preprocessing()
        xs = tf.placeholder(tf.float32, [None, len(x_data[0])])
        ys = tf.placeholder(tf.float32, [None, 1])
        # add layer
        l1 = self.add_layer(x_data, len(x_data[0]), 20, activation_function=tf.nn.relu)

        prediction = self.add_layer(l1, 20, 1, activation_function=None)

        loss = tf.reduct_mean(tf.reduct_sum(tf.square(y_data - prediction), reduction_indices=[1]))
        train_step = tf.train.GradientDescentOptimizer(0.1).minimize(loss)
        init = tf.initialize_all_tables()
        sess = tf.Session()
        sesss.run(init)
        for i in range(1000):
            sess.run(tarin_step, feed_dict={xs:x_data, ys:y_data})
            if i%50 == 0:
                print(sess.run(loss, feed_dict={xs: x_data, ys: y_data}))

    def add_layer(self, in_size, out_size, activation_function=None):
        Weights = tf.Variable(tf.random_normal(in_size, out_size))
        bias = tf.Variable(tf.zeros([1, out_size]) + 0.1)
        Wx_plus_b = tf.matmul(intputs, Weights) + biases
        if activation_function is None:
            outputs = Wx_plus_b
        else:
            outputs = activation_function(Wx_plus_b)
        return outputs






def main():
    long_short_term_memory = lstm()
    long_short_term_memory.tf_lstm()

if __name__ == "__main__":
    main()
