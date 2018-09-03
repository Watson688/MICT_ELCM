import os
import random
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from collections import Counter
from datetime import datetime
from sklearn.model_selection import train_test_split
from LSTM_TF import lstm

class RNN():

    def __init__(self):
        n_steps = 10
        num_units = 128
        n_input = 30
        learning_rate = 0.001
        n_classes = 2


    def recurrent_neural_network(self):
        #preprocessing
        x_train, y_train, x_test, y_test = lstm.preprocessing()
        print(1)


def main():
    rnn = RNN()
    rnn.recurrent_neural_network()

if __name__ == "__main__":
    main()