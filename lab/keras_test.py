import math
import numpy as np
from sklearn.preprocessing import MinMaxScaler

import tensorflow as tf

import os
import sys
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')

from pyspark.sql import SparkSession


def read_stock_data(spark, stock, period, interval):
    return spark.read.load("../data/stocks/" + stock +
                           "_period=" + period +
                           "_interval=" + interval +
                           ".parquet")


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("AI_Trading") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = read_stock_data(spark, "BBVA", "10y", "1d").toPandas()

data = df.filter(['Close'])
dataset = data.values

training_data_len = math.ceil(len(dataset) * 0.8)

scaler = MinMaxScaler(feature_range=(0, 1))
scaled_data = scaler.fit_transform(dataset)

train_data = scaled_data[0: training_data_len, :]

x_train = []
y_train = []

for i in range(60, len(train_data)):
    x_train.append(train_data[i - 60:i, 0])
    y_train.append(train_data[i, 0])

x_train, y_train = np.array(x_train), np.array(y_train)

x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))

model = tf.keras.models.Sequential([
    tf.keras.layers.LSTM(50, return_sequences=True),
    tf.keras.layers.Dropout(0.2),
    tf.keras.layers.LSTM(units=50),
    tf.keras.layers.Dense(25),
    tf.keras.layers.Dense(1)
])

model.compile(optimizer='adam',
              loss='mean_squared_error')

print(x_train.shape)
print(y_train.shape)
print(x_train)
print(y_train)

model.fit(x_train, y_train, batch_size=1, epochs=1)

test_data = scaled_data[training_data_len - 60:, :]

x_test = []
y_test = dataset[training_data_len:, :]

for i in range(60, len(test_data)):
    x_test.append(test_data[i - 60:i, 0])

x_test = np.array(x_test)

x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

predictions = model.predict(x_test)
predictions = scaler.inverse_transform(predictions)

rmse = np.sqrt(np.mean(predictions - y_test) ** 2)

train = data[:training_data_len]
valid = data[training_data_len:]
valid['Predictions'] = predictions

"""
plt.figure(figsize=(16, 8))
plt.title('Model')
plt.xlabel('Date', fontsize=18)
plt.ylabel('Close Price', fontsize=18)
plt.plot(train['Close'])
plt.plot(valid[['Close', 'Predictions']])
plt.legend(['Train', 'Val', 'Predictions'], loc='lower right')
plt.show()
"""

df = read_stock_data(spark, "BBVA", "10y", "1d").toPandas()
data = df.filter(['Close'])

last_60_days = data[-60:].values
print(last_60_days)
print(last_60_days.shape)

last_60_days_scaled = scaler.transform(last_60_days)
X_test = [last_60_days_scaled]
X_test = np.array(X_test)
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

print(X_test)

pred_price = model.predict(X_test)
pred_price = scaler.inverse_transform(pred_price)

print(pred_price[0])

last_60_days = np.append(last_60_days, pred_price[0])
last_60_days = np.reshape(last_60_days, (last_60_days.shape[0], 1))

print(last_60_days)
print(last_60_days.shape)


last_60_days_scaled = scaler.transform(last_60_days)
X_test = [last_60_days_scaled]
X_test = np.array(X_test)
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

print(X_test)

pred_price = model.predict(X_test)
pred_price = scaler.inverse_transform(pred_price)

print(pred_price[0])

for i in range(100):

    last_60_days_scaled = scaler.transform(last_60_days)
    X_test = [last_60_days_scaled]
    X_test = np.array(X_test)
    X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

    pred_price = model.predict(X_test)
    pred_price = scaler.inverse_transform(pred_price)

    print(pred_price)

    last_60_days = np.append(last_60_days, pred_price[0])
    last_60_days = np.reshape(last_60_days, (last_60_days.shape[0], 1))


stock = "BBVA"
period = "10y"
interval = "1d"
model.save_weights('../model_weights/' + stock + "_period=" + period + "_interval=" + interval + '.h5')

model.load_weights('../model_weights/' + stock + "_period=" + period + "_interval=" + interval + '.h5')