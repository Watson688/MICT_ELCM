import numpy as np
import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data
from LSTM_TF import lstm
"""
    training_set, testing_set = lstm.preprocessing()
    x_train = []
    y_train = []
    for t in training_set:
        x_train.append(t[0])
        if t[1] == 1:
            y_train.append(np.array([[1,0]]))
        if t[1] == 0:
            y_train.append(np.array([[0,1]]))
    x_train = np.dstack(x_train)
    y_train = np.dstack(y_train)
"""


mnist  = input_data.read_data_sets('MNIST_data/', one_hot=True)

lr = 0.001
training_inter = 100000
batch_size = 100
# display_step = 10 #

n_input = 28 # w
n_step = 28 # h
n_hidden = 128
n_classes = 10

# placeholder
x = tf.placeholder(tf.float32, [None, n_input, n_step])
y = tf.placeholder(tf.float32, [None, n_classes])

weights = {
    'in': tf.Variable(tf.random_normal([n_input, n_hidden])), # (28, 128)
    'out': tf.Variable(tf.random_normal([n_hidden, n_classes])) # (128, 10)
}
biases = {
    'in': tf.Variable(tf.constant(0.1, shape=[n_hidden])),
    'out': tf.Variable(tf.constant(0.1, shape=[n_classes]))
}

def RNN(x, weights, biases):
    # 原始的x是3维,需要将其变为2为的，才能和weight矩阵乘法
    # x=(128, 28, 28) ->> (128*28, 28)
    X = tf.reshape(x, [-1, n_input])
    X_in = tf.matmul(X, weights['in']) + biases['in'] # (128*28, 128)
    X_in = tf.reshape(X_in, [-1, n_step, n_hidden]) # (128, 28, 128)
    # 定义LSTMcell
    lstm_cell = tf.contrib.rnn.BasicLSTMCell(n_hidden)
    init_state = lstm_cell.zero_state(batch_size, dtype=tf.float32)
    outputs, final_state = tf.nn.dynamic_rnn(lstm_cell, X_in, initial_state=init_state, time_major=False)
    results = tf.matmul(final_state[1], weights['out']) + biases['out']
    return results

pre = RNN(x, weights, biases)
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