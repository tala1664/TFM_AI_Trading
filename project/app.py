import os
import sys
from io_stockdata.io_stockdata import download_stock_data, write_stock_data, \
    read_stock_data, read_stock_log, read_portfolio_list
from finance.finance_utils import correlation_matrix_portfolio, create_portfolio, covariance_matrix_portfolio
from display.display_utils import interactive_candlestick_graph, interactive_performance_graph
from pyspark.sql import SparkSession
from pyspark.sql.functions import year


def get_valid_period():
    periods = ["1d", "5d", "1mo",
               "3mo", "6mo", "1y",
               "2y", "5y", "10y",
               "ytd", "max"]  # Valid periods
    period = ""
    while period not in periods:
        period = str(input("Please, input a valid period: ") or "max")

    return period


def get_valid_interval():
    intervals = ["1m", "2m", "5m",
                 "15m", "30m", "60m",
                 "90m", "1h", "1d",
                 "5d", "1wk", "1mo",
                 "3mo"]  # Valid intervals
    interval = ""
    while interval not in intervals:
        interval = str(input("Please, input a valid interval: ") or "1d")

    return interval


def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("AI_Trading") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    option = -1

    while option != 0:

        option = int(input("Select a valid option: \n" +
                           "0. Exit\n" +
                           "1. Download stock data.\n" +
                           "2. Show Stock graph. \n" +
                           "3. Show Stock Performance graph. \n" +
                           "4. Show Stock Table. \n" +
                           "5. Update ALL Stocks. \n" +
                           "6. Create Portfolio. \n" +
                           "7. Show Portfolio Table. \n" +
                           "8. Calculate portfolio correlation matrix. \n" +
                           "9. Calculate portfolio covariance matrix. \n"))

        if option == 1:
            stk = input("Please, input a valid stock name: ").upper()

            try:
                period = get_valid_period()
                interval = get_valid_interval()
                df = download_stock_data(spark, stk, period, interval)
                write_stock_data(spark, df, stk, period, interval)

            except ValueError:
                print("Symbol " + stk + " not found")

        elif option == 2:
            try:
                read_stock_log(spark).orderBy("Stock").show()
                stk = input("Please, input a valid stock name: ").upper()
                period = get_valid_period()
                interval = get_valid_interval()
                df = read_stock_data(spark, stk, period, interval)
                interactive_candlestick_graph(df, stk)
            except (TypeError, AttributeError):
                print("Empty data, try option -> 1. Download stock data.\n")

        elif option == 3:
            try:
                read_stock_log(spark).orderBy("Stock").show()
                stk = input("Please, input a valid stock name: ").upper()
                period = get_valid_period()
                interval = get_valid_interval()
                df = read_stock_data(spark, stk, period, interval)
                interactive_performance_graph(df, stk)
            except (TypeError, AttributeError):
                print("Empty data, try option -> 1. Download stock data.\n")

        elif option == 4:
            try:
                read_stock_log(spark).orderBy("Stock").show(truncate=False)
                stk = input("Please, input a valid stock name: ").upper()
                period = get_valid_period()
                interval = get_valid_interval()
                df = read_stock_data(spark, stk, period, interval)
                df.show(truncate=False)
            except (TypeError, AttributeError):
                print("Empty data, try option -> 1. Download stock data.\n")

        elif option == 5:
            df_stock = read_stock_log(spark).orderBy("Stock")
            df_stock.show(truncate=False)
            list_stock = df_stock.select("Stock").rdd.flatMap(lambda x: x).collect()
            list_period = df_stock.select("Period").rdd.flatMap(lambda x: x).collect()
            list_interval = df_stock.select("Interval").rdd.flatMap(lambda x: x).collect()
            for i in range(len(list_stock)):
                df = download_stock_data(spark, list_stock[i], list_period[i], list_interval[i])
                write_stock_data(spark, df, list_stock[i], list_period[i], list_interval[i])

        elif option == 6:
            df_stock = read_stock_log(spark)
            df_stock.orderBy("Stock").filter("Period = 'max'").orderBy("Stock").show(truncate=False)
            list_stock = df_stock.select("Stock").rdd.flatMap(lambda x: x).collect()
            stk_list = input("Please, input a list separated by comma of stocks: ").upper().replace(" ", "").split(",")
            create_portfolio(spark, df_stock, stk_list, list_stock)

        elif option == 7:
            read_portfolio_list(spark).orderBy("ID").show(truncate=False)

        elif option == 8:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            correlation_matrix_portfolio(spark, df_portfolio, id_portfolio).show()

        elif option == 9:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            covariance_matrix_portfolio(spark, df_portfolio, id_portfolio).show()


if __name__ == "__main__":
    main()
