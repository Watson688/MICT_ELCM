import numpy as np
import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data
from LSTM_TF import lstm


x_train = np.load("x_train.npy")
y_train = np.load("y_train.npy")
# 使用LSTM来实现mnist的分类，将输入28*28的图像每一行看作输入像素序列，行行之间具有时间信息。即step=28
# 设置超参数
# 超参数
lr = 0.001
training_inter = 100000
batch_size = 128
# display_step = 10 #

#x_train, y_train, x_test, y_test = lstm.preprocessing()
# np.save("x_train", x_train)
# np.save("y_train", y_train)
n_input = 30 # w
n_step = 10 # h
n_hidden = 128
n_classes = 2


# placeholder
x = tf.placeholder(tf.float32, [None, n_input, n_step]) # 30, 10
y = tf.placeholder(tf.float32, [None, n_classes])

weights = {
    'in': tf.Variable(tf.random_normal([n_input, n_hidden])), # (30, 128)
    'out': tf.Variable(tf.random_normal([n_hidden, n_classes])) # (128, 2)
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