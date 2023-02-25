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

    # Convert the date column to a datetime object
    df_pandas["DateTime"] = pd.to_datetime(df_pandas["DateTime"])

    # Create a Candlestick chart using Plotly
    fig = go.Figure(data=[
        go.Candlestick(x=df_pandas["DateTime"],
                       open=df_pandas["Open"],
                       high=df_pandas["High"],
                       low=df_pandas["Low"],
                       close=df_pandas["Close"],
                       name="My Candlestick Chart")])

    # Add a title and axis labels to the figure
    fig.update_layout(title=stock, xaxis_title="Date", yaxis_title="Price")

    # Display the plot in a Jupyter notebook
    fig.show()
