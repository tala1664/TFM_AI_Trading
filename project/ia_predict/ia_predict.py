import math
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

plt.style.use('fivethirtyeight')
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from io_stockdata.io_stockdata import read_stock_data, write_portfolio_list, read_portfolio_list


def prepare_train_data(df):
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

    return x_train, y_train


def build_model():
    model = tf.keras.models.Sequential([
        tf.keras.layers.LSTM(50, return_sequences=True),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(units=50),
        tf.keras.layers.Dense(25),
        tf.keras.layers.Dense(1)
    ])

    model.compile(optimizer='adam',
                  loss='mean_squared_error')

    return model


def train_model(model, x_train, y_train):
    model.fit(x_train, y_train, batch_size=1, epochs=1)


def get_prediction(df, model, length):
    scaler = MinMaxScaler(feature_range=(0, 1))
    df = df.toPandas()
    data = df.filter(['Close'])

    last_60_days = data[-length:].values

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


def save_model_weights_portfolio(model, stock, id_portfolio):
    model.save_weights('../model_weights/portfolios/' + stock + "_portfolio=" + str(id_portfolio) + '.h5')


def load_model_weights_portfolio(model, stock, id_portfolio):
    return model.load_weights('../model_weights/portfolios/' + stock + "_portfolio=" + str(id_portfolio) + '.h5')


def train_portfolio(spark, df_portfolio, id_portfolio):
    model = build_model()

    stock_close_prices = {}

    stock_list = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Stock_List").rdd.flatMap(lambda x: x).collect()[0]
    min_date = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Min_Date").rdd.flatMap(lambda x: x).collect()[0]
    max_date = df_portfolio.filter(df_portfolio.ID == id_portfolio) \
        .select("Max_Date").rdd.flatMap(lambda x: x).collect()[0]

    for stock in stock_list:
        print("Training model for stock: " + stock)
        df_stock = read_stock_data(spark, stock, "max", "1d")
        df_stock = df_stock.filter(df_stock.DateTime > min_date) \
            .filter(df_stock.DateTime < max_date)

        stock_close_prices[stock] = df_stock.select("Close")

        x_train, y_train = prepare_train_data(df_stock)
        train_model(model, x_train, y_train)
        print("Saving weights of model for stock: " + stock)
        model.summary()
        save_model_weights_portfolio(model, stock, id_portfolio)


def predict_portfolio(spark, df_portfolio, id_portfolio):
    model = build_model()

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

    model.build(input_shape=(1, 60, 50))
    model.summary()
    for stock in stock_close_prices:
        print("Loading weights for stock: " + stock + " Portfolio: " + str(id_portfolio))
        model = load_model_weights_portfolio(model, stock, id_portfolio)
        print("Getting prediction for " + stock + " Portfolio: " + str(id_portfolio))
        predicted_next_prices[stock] = get_prediction(stock_close_prices[stock], model, 100)

    print(predicted_next_prices)
