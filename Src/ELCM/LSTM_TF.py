import os
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
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
        size_of_input = 10
        print("grouping")
        for a in agvs:
            groupby_agv[a] = df_merged[df_merged['device'] == a]
        # iterate over each agv
        # abnormal
        print("iterating 1")
        for agv in groupby_agv.keys():
            for row in groupby_agv[agv].itertuples(index=True):
                last_position = None
                if row.Index - size_of_input + 1 < 0:
                    continue
                if row[-1] == 1 and (last_position is None or row.Index - last_position >= size_of_input):
                    x_abnormal.append(groupby_agv[agv].iloc[row.Index - size_of_input + 1:row.Index+1])
                    last_position = row.Index

        print("iterating 2")
        for agv in groupby_agv.keys():
            start = None
            for row in groupby_agv[agv].itertuples(index=True):
                if row[-1] == 0.0:
                    if not start:
                        start = row.Index
                    elif row.Index - start == 9:
                        x_normal.append(groupby_agv[agv].loc[start:row.Index])
                        start = None
                else:
                    start = None
        return x_abnormal, x_normal
    
    def tf_lstm(self):

        lr = 0.001
        training_iters = 10000
        batch_size = 128
        n_inputs = 9

        x = tf.placeholder(tf.float32, [None, n_steps, n_inputs])
        y = tf.placeholder(tf.float32, [None, n_classes])




def main():
    long_short_term_memory = lstm()
    long_short_term_memory.tf_lstm()

if __name__ == "__main__":
    main()
