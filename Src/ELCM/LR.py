import os
import random
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from collections import Counter
from datetime import datetime
from sklearn.model_selection import train_test_split
from tensorflow.contrib import rnn
from LSTM_TF import lstm

class RNN():

    def __init__(self):
        pass

    def LSTM(self):
        x_train, y_train, x_test, y_test = lstm.preprocessing()
        x_train = [x.values.ravel() for x in x_train]

        lr = 0.001
        batch_size = tf.placeholder(tf.int32)
        input_size = 30
        timestep_size = 10
        hidden_size = 256
        layer_num = 2
        class_num = 2
        

        _X = tf.placeholder(tf.float32, [None, 300])
        y = tf.placeholder(tf.float32, [None, 2])
        keep_prob = tf.placeholder(tf.float32)
        
        X =tf.reshape(_X, [-1, timestep_size, input_size])
        lstm_cell = rnn.BasicLSTMCell(num_units=hidden_size, forget_bias = 1.0, state_is_tuple=True)
        lstm_cell = rnn.DropoutWrapper(cell=lstm_cell, input_keep_prob=1.0, output_keep_prob=keep_prob)

        mlstm_cell = rnn.MultiRNNCell([lstm_cell] * layer_num, state_is_tuple=True)

        init_state = mlstm_cell.zero_state(batch_size, dtype=tf.float32)
        outputs, state = tf.nn.dynamic_rnn(mlstm_cell, inputs=X, initial_state=init_state, time_major=False)
        h_state = state[-1][1]

        W = tf.Variable(tf.truncated_normal([hidden_size, 2], stddev=0.1), dtype=tf.float32)
        bias = tf.Variable(tf.constant(0.1,shape=[2]), dtype=tf.float32)
        y_pre = tf.nn.softmax(tf.matmul(h_state, W) + bias)

        cross_entropy = -tf.reduce_mean(y * tf.log(y_pre))
        train_op = tf.train.AdamOptimizer(lr).minimize(cross_entropy)
        correct_prediction = tf.equal(tf.argmax(y_pre,1), tf.argmax(y,1))
        accuracy = tf.reduce_mean(tf.cast(correct_prediction, "float"))
        for i in range(2000):
            sess.run(train_op, feed_dict={_X: x_train, y: y_train})



def main():
    rnn = RNN()
    rnn.LSTM()

if __name__ == "__main__":
    main()