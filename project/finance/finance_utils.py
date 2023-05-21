import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import scipy.stats as stats
import datetime
import numpy as np
from io_stockdata.io_stockdata import read_stock_data, write_portfolio_list, read_portfolio_list
import pyspark.sql.functions as f
from statistics import mean
from pyspark.sql.functions import year, month, dayofmonth


def avg_daily_stock_return(df_stock):
    avg_return = mean(df_stock.select("Performance").filter(f.col("Performance").isNotNull())
                      .rdd.flatMap(lambda x: x).collect())
    print(avg_return)


def avg_stock_return(df_stock, date_from, date_to):
    return1 = df_stock.filter((year(df_stock.DateTime) == date_from.year) &
                                 (month(df_stock.DateTime) == date_from.month) &
                                 (dayofmonth(df_stock.DateTime) >= date_from.day))\
        .select("Close").rdd.flatMap(lambda x: x).collect()[0]

    return2 = df_stock.filter((year(df_stock.DateTime) == date_to.year) &
                              (month(df_stock.DateTime) == date_to.month) &
                              (dayofmonth(df_stock.DateTime) <= date_to.day))\
        .select("Close").rdd.flatMap(lambda x: x).collect()[-1]

    return (return2 - return1) / return1


def covariance_matrix_portfolio(spark, df_portfolio, id_portfolio):
    stock_performance = []

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
        stock_performance.append(df_stock.select("Performance").rdd.flatMap(lambda x: x).collect())
    cov_matrix = np.cov(stock_performance, bias=False).round(decimals=5).tolist()

    for arr in cov_matrix:
        arr.insert(0, stock_list[cov_matrix.index(arr)])

    stock_list.insert(0, "Covariance")
    df = spark.createDataFrame(cov_matrix, stock_list)
    df.write.format("parquet").mode("overwrite").save("../data/portfolios/covariance_matrix/portfolio_" +
                                                      str(id_portfolio) + ".parquet")

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

    df.write.format("parquet").mode("overwrite").save("../data/portfolios/correlation_matrix/portfolio_" +
                                                      str(id_portfolio) + ".parquet")

    return df


def create_portfolio(spark, df_stock, stk_list, list_stock):
    stk_list = [i for i in stk_list if i in list_stock]
    df_stock = df_stock.filter(df_stock.Stock.isin(stk_list))
    list_stock = df_stock.select("Stock").rdd.flatMap(lambda x: x).collect()
    list_period = df_stock.select("Period").rdd.flatMap(lambda x: x).collect()
    list_interval = df_stock.select("Interval").rdd.flatMap(lambda x: x).collect()
    print("Invalid stock names are removed. List of stocks of the portfolio: ")
    print(list_stock)
    num_shares_list = []
    buy_dates_list = []
    performance_list = []
    min_date = datetime.datetime(1000, 1, 1)
    max_date = datetime.datetime.now()
    for i in range(len(list_stock)):

        num_shares = int(input("Please, input the number of shares of " + list_stock[i] + ": "))
        buy_date = input("Please, input the date (format YYYY-MM-DD) when you bought " + list_stock[i] + " shares: ")
        buy_date_split = buy_date.split("-")
        buy_date_datetime = datetime.datetime(int(buy_date_split[0]), int(buy_date_split[1]), int(buy_date_split[2]))
        num_shares_list.append(num_shares)
        buy_dates_list.append(buy_date_datetime)

        stock_df = read_stock_data(spark, list_stock[i], list_period[i], list_interval[i])

        min_date_aux = stock_df.select("DateTime").rdd.min()[0]
        max_date_aux = stock_df.select("DateTime").rdd.max()[0]

        if min_date_aux > min_date:
            min_date = min_date_aux

        if buy_date_datetime > min_date:
            min_date = buy_date_datetime

        if max_date_aux < max_date:
            max_date = max_date_aux

    for i in range(len(list_stock)):
        stock_df = read_stock_data(spark, list_stock[i], list_period[i], list_interval[i])
        performance_list.append(avg_stock_return(stock_df, min_date, max_date))

    write_portfolio_list(spark, list_stock, num_shares_list, buy_dates_list, performance_list, min_date, max_date)
    read_portfolio_list(spark).show(truncate=False)
