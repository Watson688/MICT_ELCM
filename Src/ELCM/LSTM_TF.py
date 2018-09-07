import os
import random
from datetime import datetime
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

    @staticmethod
    def preprocessing():
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
        print("generating abnormal cases")
        for agv in groupby_agv.keys():
            last_position = None
            for index, row in enumerate(groupby_agv[agv].itertuples()):
                if index - size_of_input + 1 < 0:
                    continue
                if row[-1] == 1 and (last_position is None or index - last_position >= size_of_input):
                    x_abnormal.append(groupby_agv[agv].iloc[index - size_of_input + 1:index+1,:-1])
                    y_abnormal.append(groupby_agv[agv].iloc[index,-1])
        print("generating normal cases")
        for agv in groupby_agv.keys():
            start = None
            for index, row in enumerate(groupby_agv[agv].itertuples()):
                if row[-1] == 0:
                    if not start:
                        start = index
                    elif index - start == size_of_input - 1:
                        x_normal.append(groupby_agv[agv].iloc[start:index+1,:-1])
                        y_normal.append(groupby_agv[agv].iloc[index,-1])
                        start = None
                else:
                    start = None  
        # balance the data, split to training and testing
        normal = list(zip(x_normal, y_normal))
        abnormal = list(zip(x_abnormal, y_abnormal))

        c_1 = Counter([x.shape for x in x_abnormal])
        c_2 = Counter([x.shape for x in x_normal])

        number_of_training = int(len(abnormal) * 0.7)
        boundary_time = datetime.strptime(abnormal[number_of_training - 1][0].iloc[-1,-1], '%Y-%m-%d %H:%M:%S')

        normal_pre_boundary = []
        normal_post_boundary = []
        for data_point in normal:
            if datetime.strptime(data_point[0].iloc[-1,-1], '%Y-%m-%d %H:%M:%S') <= boundary_time:
                normal_pre_boundary.append(data_point)
            if datetime.strptime(data_point[0].iloc[0,-1], '%Y-%m-%d %H:%M:%S') > boundary_time:
                normal_post_boundary.append(data_point)

        training_set = abnormal[:number_of_training] + random.sample(normal_pre_boundary, number_of_training)
        testing_set = abnormal[number_of_training:] + random.sample(normal_post_boundary, len(abnormal[number_of_training:]))

        print("last timestamp of training_set:")
        print(abnormal[number_of_training-1][0].iloc[0][-1])
        print("first timestamp of testing_set:")
        print(abnormal[number_of_training][0].iloc[0][-1])
        # removed the timestamp, device
        c_1 = Counter([x[0].shape for x in training_set])
        c_2 = Counter([x[0].shape for x in testing_set])
        print(c_1)
        print(c_2)
        for data_tuple in training_set:
            data_tuple[0].drop(['device', 'date'], axis=1, inplace=True)
        for data_tuple in testing_set:
            data_tuple[0].drop(['device', 'date'], axis=1, inplace=True)

        return training_set, testing_set
    
    def tf_lstm(self):
        x_train, y_train, x_test, y_test = preprocessing()
        for x_ in x_train:
            x_ = x_.iloc[:,:-1].transpose()
        for x_ in x_test:
            x_ = x_.iloc[:,:-1]
        xs = tf.placeholder(tf.float32, [None, 31])
        ys = tf.placeholder(tf.float32, [None, 1])
        # add layer
        l1 = self.add_layer(xs, 31, 20, activation_function=tf.nn.relu)
        prediction = self.add_layer(l1, 20, 1, activation_function=None)
        loss = tf.reduce_mean(tf.reduce_sum(tf.square(ys - prediction), reduction_indices=[1]))
        train_step = tf.train.GradientDescentOptimizer(0.1).minimize(loss)
        init = tf.initialize_all_tables()
        sess = tf.Session()
        sess.run(init)
        for i in range(1000):
            sess.run(train_step, feed_dict={xs:x_train, ys:y_train})
            if i % 50 == 0:
                print(sess.run(loss, feed_dict={xs: x_train, ys: y_train}))

    def add_layer(self, inputs, in_size, out_size, activation_function=None):
        Weights = tf.Variable(tf.random_normal([in_size, out_size]))
        biases = tf.Variable(tf.zeros([1, out_size]) + 0.1,)
        Wx_plus_b = tf.matmul(inputs, Weights) + biases
        if activation_function is None:
            outputs = Wx_plus_b
        else:
            outputs = activation_function(Wx_plus_b,)
        return outputs






def main():
    long_short_term_memory = lstm()
    long_short_term_memory.tf_lstm()

if __name__ == "__main__":
    main()
