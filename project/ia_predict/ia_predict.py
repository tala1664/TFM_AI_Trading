import math
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import pandas as pd

plt.style.use('fivethirtyeight')
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from io_stockdata.io_stockdata import read_stock_data, write_portfolio_list, read_portfolio_list
from display.display_utils import interactive_performance_prediction


def build_model_LSTM():
    model = tf.keras.models.Sequential([
        tf.keras.layers.LSTM(100, return_sequences=True),
        tf.keras.layers.LSTM(100, return_sequences=False),
        tf.keras.layers.Dense(25),
        tf.keras.layers.Dense(1)
    ])

    model.compile(optimizer='adam',
                  loss='mean_squared_error')

    return model


def build_model_GRU():
    model = tf.keras.models.Sequential([
        tf.keras.layers.GRU(100, return_sequences=True, activation='tanh'),
        tf.keras.layers.GRU(100, return_sequences=False, activation='tanh'),
        tf.keras.layers.Dense(25),
        tf.keras.layers.Dense(1)
    ])

    model.compile(optimizer='adam',
                  loss='mean_squared_error')

    return model


def train_model(df, model):
    df = df.toPandas()
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

    model.fit(x_train, y_train, batch_size=1, epochs=2)

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

    print("\n*** RSME:" + str(rmse))

    interactive_performance_prediction(df, data, predictions, training_data_len)


def get_prediction(df, model):
    scaler = MinMaxScaler(feature_range=(0, 1))
    df = df.toPandas()
    data = df.filter(['Close'])

    last_60_days = data[-60:].values

    last_60_days_scaled = scaler.fit_transform(last_60_days)
    x_test = [last_60_days_scaled]
    x_test = np.array(x_test)
    x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

    pred_price = model.predict(x_test)
    pred_price = scaler.inverse_transform(pred_price)

    return pred_price


def save_model_weights(model, stock, period, interval):
    model.save_weights('../model_weights/stocks/' + stock + "_period=" + period + "_interval=" + interval + '.h5')


def load_model_weights(model, stock, period, interval):
    return model.load_weights(
        '../model_weights/stocks/' + stock + "_period=" + period + "_interval=" + interval + '.h5')


def save_model_weights_portfolio(model, stock, id_portfolio, model_name):
    model.save_weights('../model_weights/portfolios/' + stock + "_portfolio=" + str(id_portfolio) + '_model=' + model_name + '.h5')


def load_model_weights_portfolio(model, stock, id_portfolio, model_name):
    model.load_weights('../model_weights/portfolios/' + stock + "_portfolio=" + str(id_portfolio) + '_model=' + model_name + '.h5')


def train_portfolio(spark, df_portfolio, id_portfolio):
    model = build_model_GRU()

    stock_close_prices = {}

    stock_list = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Stock_List").rdd.flatMap(lambda x: x).collect()[0]
    min_date = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Min_Date").rdd.flatMap(lambda x: x).collect()[0]
    max_date = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Max_Date").rdd.flatMap(lambda x: x).collect()[0]

    for stock in stock_list:
        print("\n*** Training model for stock: " + stock)
        df_stock = read_stock_data(spark, stock, "max", "1d")
        df_stock = df_stock.filter(df_stock.DateTime > min_date) \
            .filter(df_stock.DateTime < max_date)

        stock_close_prices[stock] = df_stock.select("Close")

        train_model(df_stock, model)
        print("\n*** Saving weights of model for stock: " + stock)
        save_model_weights_portfolio(model, stock, id_portfolio, "GRU")


def predict_portfolio(spark, df_portfolio, id_portfolio):
    model = build_model_GRU()
    model.build(input_shape=(1, 60, 1))

    stock_close_prices = {}
    predicted_next_prices = {}

    stock_list = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Stock_List").rdd.flatMap(lambda x: x).collect()[0]
    min_date = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Min_Date").rdd.flatMap(lambda x: x).collect()[0]
    max_date = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Max_Date").rdd.flatMap(lambda x: x).collect()[0]

    for stock in stock_list:
        df_stock = read_stock_data(spark, stock, "max", "1d")
        df_stock = df_stock.filter(df_stock.DateTime > min_date) \
            .filter(df_stock.DateTime < max_date)

        stock_close_prices[stock] = df_stock.select("Close")

    for stock in stock_close_prices:
        print("\n*** Loading weights for stock: " + stock + " Portfolio: " + str(id_portfolio))
        load_model_weights_portfolio(model, stock, id_portfolio, "GRU")
        print("*** Getting prediction for " + stock + " Portfolio: " + str(id_portfolio))
        predicted_next_prices[stock] = get_prediction(stock_close_prices[stock], model).tolist()[0][0]

    print("\n*** Predicted values for the stocks of your portfolio: ")
    for key, value in predicted_next_prices.items():
        print(key + ": " + str(value))
    print()
