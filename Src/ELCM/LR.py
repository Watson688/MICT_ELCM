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
        pass

    def rnn(self):
        #preprocessing
        x_train, y_train, x_test, y_test = lstm.preprocessing()
        lstm_cell = tf.contrib.rnn.BasicLSTMCell(n_hidden)
        init_state = lstm_cell.zero_state(batch_size, dtype=tf.float32)
        outputs, final_state = tf.nn.dynamic_rnn(lstm_cell, X_in, initial_state=init_state, time_major=False)
        results = tf.matmul(final_state[1], weights['out']) + biases['out']
        return results

    def run_rnn(self):
        learning_rate = 0.001
        batch_size = 128
        num_epochs = 1000

        n_hidden = 128
        n_step = 10
        n_input = 31
        n_classes = 2

        x = tf.placeholder(tf.float32, [None, n_input, n_step])
        y = tf.placeholder(tf.float32, [None, n_classes])
        weights = {
            'in': tf.Variable(tf.random_normal([n_input, n_hidden])), 
            'out': tf.Variable(tf.random_normal([n_hidden, n_classes]))
            }
        biases = {
            'in': tf.Variable(tf.constant(0.1, shape=[n_hidden])),
            'out': tf.Variable(tf.constant(0.1, shape=[n_classes]))
            }
        pre = self.rnn(x, weights, biases)
        cost = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=y, logits=pre))
        train_op = tf.train.AdamOptimizer(lr).minimize(cost)
        correct_pred = tf.equal(tf.argmax(pre, 1), tf.argmax(y, 1))
        accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32))
        init = tf.global_variables_initializer()
        with tf.Session() as sess:
            sess.run(init)
            step = 0
            while step*batch_size < training_inter:
                batch_xs, batch_ys = mnist.train.next_batch(batch_size)
                batch_xs = batch_xs.reshape([batch_size, n_step, n_input])
                sess.run([train_op], feed_dict={x: batch_xs, y: batch_ys})
                if step % 20 == 0:
                    print(sess.run(accuracy, feed_dict={x: batch_xs, y: batch_ys}))
                step += 1

def main():
    rnn = RNN()
    rnn.recurrent_neural_network()

if __name__ == "__main__":
    main()