import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt


def display_line_graph(df, x_col, y_col, stock):
    df_pandas = df.select(x_col, y_col).toPandas()
    df_pandas.plot(x=x_col, y=y_col)
    plt.title(stock)
    plt.show()


def display_bar_graph(df, x_col, y_col, stock):
    df_pandas = df.select(x_col, y_col).toPandas()
    df_pandas.plot.bar(x=x_col, y=y_col)
    plt.title(stock)
    plt.show()


def interactive_candlestick_graph(df, stock):
    df_pandas = df.select("*").toPandas()

    df_pandas["DateTime"] = pd.to_datetime(df_pandas["DateTime"])

    fig = go.Figure(data=[
        go.Candlestick(x=df_pandas["DateTime"],
                       open=df_pandas["Open"],
                       high=df_pandas["High"],
                       low=df_pandas["Low"],
                       close=df_pandas["Close"],
                       name="Price")])

    fig.add_trace(go.Scatter(x=df_pandas["DateTime"],
                             y=df_pandas["ma30"],
                             name="30-Day Moving Average",
                             line=dict(color='blue')))

    fig.add_trace(go.Scatter(x=df_pandas["DateTime"],
                             y=df_pandas["ma60"],
                             name="60-Day Moving Average",
                             line=dict(color='red')))

    fig.add_trace(go.Scatter(x=df_pandas["DateTime"],
                             y=df_pandas["ma90"],
                             name="90-Day Moving Average",
                             line=dict(color='green')))

    fig.update_layout(title=stock, xaxis_title="Date", yaxis_title="Price")

    fig.show()


def interactive_performance_graph(df, stock):
    df_pandas = df.select("*").toPandas()
    df_pandas["DateTime"] = pd.to_datetime(df_pandas["DateTime"])

    fig = go.Figure(data=go.Scatter(x=df_pandas["DateTime"],
                                    y=df_pandas["Performance"],
                                    name="Daily Percentage Change"))

    fig.update_layout(title=stock + " Performance Percentage Variation", xaxis_title="Date",
                      yaxis_title="Percentage Change")

    fig.show()


def interactive_performance_prediction(df, data, predictions, training_data_len, stock):
    train = data[:training_data_len]
    valid = data[training_data_len:]
    valid['Predictions'] = predictions
    train['Date'] = df.filter(['DateTime']).values[:training_data_len, :]
    valid['Date'] = df.filter(['DateTime']).values[training_data_len:, :]
    train['Date'] = pd.to_datetime(train['Date'])
    valid['Date'] = pd.to_datetime(valid['Date'])

    fig = go.Figure()
    fig.update_layout(title=stock, xaxis_title="Date", yaxis_title="Price")
    fig.add_trace(go.Scatter(x=train['Date'], y=train['Close'], name='Historic'))
    fig.add_trace(go.Scatter(x=valid['Date'], y=valid['Close'], name='Real'))
    fig.add_trace(go.Scatter(x=valid['Date'], y=valid['Predictions'], name='Predicted'))

    fig.show()


def interactive_simulation(simulations, stock):
    fig = go.Figure()
    fig.update_layout(title=stock, xaxis_title="Days", yaxis_title="Price")

    for i in simulations:
        fig.add_trace(go.Scatter(y=i))

    fig.show()
