import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import scipy.stats as stats
import datetime
import numpy as np
from io_stockdata.io_stockdata import read_stock_data, write_portfolio_list, read_portfolio_list


def covariance_matrix_portfolio(spark, df_portfolio, id_portfolio):
    matrix = []
    stock_close_prices = []

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

        stock_close_prices.append(df_stock.select("Close").rdd.flatMap(lambda x: x).collect())

    cov_matrix = np.cov(stock_close_prices, bias=False).tolist()

    for arr in cov_matrix:
        arr.insert(0, stock_list[cov_matrix.index(arr)])

    stock_list.insert(0, "Covariance")
    df = spark.createDataFrame(cov_matrix, stock_list)

    return df


def correlation_matrix_portfolio(spark, df_portfolio, id_portfolio):
    matrix = []
    stock_close_prices = {}

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

        stock_close_prices[stock] = df_stock.select("Close").rdd.flatMap(lambda x: x).collect()

    for stock in stock_list:

        matrix.append([stock])
        for stock2 in stock_list:
            c, p = stats.pearsonr(stock_close_prices[stock], stock_close_prices[stock2])
            matrix[-1].append(round(float(c), 5))

    stock_list.insert(0, "Correlations")
    df = spark.createDataFrame(matrix, stock_list)

    return df


def create_portfolio(spark, df_stock, stk_list, list_stock):
    stk_list = [i for i in stk_list if i in list_stock]
    df_stock = df_stock.filter(df_stock.Stock.isin(stk_list))
    list_stock = df_stock.select("Stock").rdd.flatMap(lambda x: x).collect()
    list_period = df_stock.select("Period").rdd.flatMap(lambda x: x).collect()
    list_interval = df_stock.select("Interval").rdd.flatMap(lambda x: x).collect()
    print("Invalid stock names are removed. List of stocks of the portfolio: ")
    print(stk_list)
    num_shares_list = []
    min_date = datetime.datetime(1000, 1, 1)
    max_date = datetime.datetime.now()
    for i in range(len(list_stock)):

        num_shares = int(input("Please, input the number of shares of " + list_stock[i] + ": "))
        num_shares_list.append(num_shares)

        stock_df = read_stock_data(spark, list_stock[i], list_period[i], list_interval[i])

        min_date_aux = stock_df.select("DateTime").rdd.min()[0]
        max_date_aux = stock_df.select("DateTime").rdd.max()[0]

        if min_date_aux > min_date:
            min_date = min_date_aux

        if max_date_aux < max_date:
            max_date = max_date_aux

    write_portfolio_list(spark, stk_list, num_shares_list, min_date, max_date)
    read_portfolio_list(spark).show(truncate=False)
