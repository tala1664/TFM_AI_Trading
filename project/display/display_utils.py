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
