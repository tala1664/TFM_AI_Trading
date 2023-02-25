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
    df_pandas["ma"] = df_pandas["Close"].rolling(window=30).mean()

    fig = go.Figure(data=[
        go.Candlestick(x=df_pandas["DateTime"],
                       open=df_pandas["Open"],
                       high=df_pandas["High"],
                       low=df_pandas["Low"],
                       close=df_pandas["Close"],
                       name="Price")])

    fig.add_trace(go.Scatter(x=df_pandas["DateTime"],
                             y=df_pandas["ma"],
                             name="30-Day Moving Average",
                             line=dict(color='blue')))

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
