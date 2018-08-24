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

    def tf_lstm(self):
        # read data
        df_events = pd.read_csv(str(Path(os.path.realpath(__file__)).parent.parent.parent / 'Data/LSTM_Data/events_data_for_RF.csv'), index_col=0)
        df_events= df_events.dropna(axis=0).rename(columns={"DEVICE":"device"})
        df_errors = pd.read_csv(str(Path(os.path.realpath(__file__)).parent.parent.parent / 'Data/LSTM_Data/errors_data_for_RF.csv'), index_col=0)[['device', 'date', 'Management System - Direct Stop']]
        df_merged = df_events.merge(df_errors, how='left', on=['device','date'])



def main():
    long_short_term_memory = lstm()
    long_short_term_memory.tf_lstm()

if __name__ == "__main__":
    main()
