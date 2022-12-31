import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import scipy.stats as stats
from io_stockdata.io_stockdata import read_stock_data


def correlation_matrix_portfolio(spark, stock_list, min_date, max_date):
    matrix = []
    stock_close_prices = {}

    for stock in stock_list:
        df_stock = read_stock_data(spark, stock, "max", "1d")
        df_stock = df_stock.filter(df_stock.DateTime > min_date) \
                           .filter(df_stock.DateTime < max_date)

        stock_close_prices[stock] = df_stock.select("Close").rdd.flatMap(lambda x: x).collect()

    for stock in stock_list:

        matrix.append([stock])
        for stock2 in stock_list:
            c, p = stats.pearsonr(stock_close_prices[stock], stock_close_prices[stock2])
            matrix[-1].append(float(c))

    stock_list.insert(0, "Correlations")
    df = spark.createDataFrame(matrix, stock_list)

    return df
