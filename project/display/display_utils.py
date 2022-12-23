import matplotlib.pyplot as plt


def display_graph(df, x_col, y_col):
    df_pandas = df.select(x_col, y_col).toPandas()
    df_pandas.plot(x=x_col, y=y_col)
    plt.show()
