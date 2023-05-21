import datetime
import os
import sys
from io_stockdata.io_stockdata import download_stock_data, write_stock_data, \
    read_stock_data, read_stock_log, read_portfolio_list
from finance.finance_utils import correlation_matrix_portfolio, create_portfolio, covariance_matrix_portfolio, \
    avg_stock_return
from display.display_utils import interactive_candlestick_graph, interactive_performance_graph
from pyspark.sql import SparkSession
from pyspark.sql.functions import year
from ia_predict.ia_predict import predict_portfolio, train_portfolio
from ia_predict.ia_simulate import simulate_portfolio, train_portfolio_sim
import tensorflow as tf
import pandas as pd


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

    spark.sparkContext.setLogLevel('ERROR')
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    tf.get_logger().setLevel('ERROR')
    pd.options.mode.chained_assignment = None

    option = -1

    while option != 0:
        try:
            option = int(input("Select a valid option: \n" +
                               "0. Exit\n" +
                               "1. Download stock data.\n" +
                               "2. Update ALL Stocks. \n" +
                               "3. Show Stock graph. \n" +
                               "4. Show Stock Performance graph. \n" +
                               "5. Create Portfolio. \n" +
                               "6. Calculate portfolio correlation matrix. \n" +
                               "7. Calculate portfolio covariance matrix. \n" +
                               "8. Train model with portfolio. \n" +
                               "9. Get portfolio predictions. \n" +
                               "10. Train Model Portfolio MonteCarlo Simulation. \n" +
                               "11. Portfolio MonteCarlo Simulation. \n" +
                               "12. Show Stocks Table. \n" +
                               "13. Show Portfolio Table. \n" +
                               "14. Show Stock Table. \n"))
        except:
            option = -1

        if option == 1:
            stk = input("Please, input a valid stock name: ").upper()

            try:
                period = get_valid_period()
                interval = get_valid_interval()
                df = download_stock_data(spark, stk, period, interval)
                write_stock_data(spark, df, stk, period, interval)

            except ValueError:
                print("Symbol " + stk + " not found")

        elif option == 3:
            try:
                read_stock_log(spark).orderBy("Stock").show()
                stk = input("Please, input a valid stock name: ").upper()
                period = get_valid_period()
                interval = get_valid_interval()
                df = read_stock_data(spark, stk, period, interval)
                interactive_candlestick_graph(df, stk)
            except (TypeError, AttributeError):
                print("Empty data, try option -> 1. Download stock data.\n")

        elif option == 4:
            try:
                read_stock_log(spark).orderBy("Stock").show()
                stk = input("Please, input a valid stock name: ").upper()
                period = get_valid_period()
                interval = get_valid_interval()
                df = read_stock_data(spark, stk, period, interval)
                interactive_performance_graph(df, stk)
            except (TypeError, AttributeError):
                print("Empty data, try option -> 1. Download stock data.\n")

        elif option == 14:
            try:
                read_stock_log(spark).orderBy("Stock").show(truncate=False)
                stk = input("Please, input a valid stock name: ").upper()
                period = get_valid_period()
                interval = get_valid_interval()
                df = read_stock_data(spark, stk, period, interval)
                df.show(100, truncate=False)
            except (TypeError, AttributeError):
                print("Empty data, try option -> 1. Download stock data.\n")

        elif option == 2:
            df_stock = read_stock_log(spark).orderBy("Stock")
            df_stock.show(truncate=False)
            list_stock = df_stock.select("Stock").rdd.flatMap(lambda x: x).collect()
            list_period = df_stock.select("Period").rdd.flatMap(lambda x: x).collect()
            list_interval = df_stock.select("Interval").rdd.flatMap(lambda x: x).collect()
            for i in range(len(list_stock)):
                df = download_stock_data(spark, list_stock[i], list_period[i], list_interval[i])
                write_stock_data(spark, df, list_stock[i], list_period[i], list_interval[i])

        elif option == 5:
            try:
                df_stock = read_stock_log(spark)
                df_stock.orderBy("Stock").filter("Period = 'max'").orderBy("Stock").show(truncate=False)
                list_stock = df_stock.select("Stock").rdd.flatMap(lambda x: x).collect()
                stk_list = input("Please, input a list separated by comma of stocks: ").upper().replace(" ", "").split(
                    ",")
                create_portfolio(spark, df_stock, stk_list, list_stock)
            except:
                print("ERROR. Please try again")

        elif option == 13:
            try:
                read_portfolio_list(spark).orderBy("ID").show(truncate=False)
            except:
                print("No porftolio founded, please create one with option 6.")

        elif option == 6:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            correlation_matrix_portfolio(spark, df_portfolio, id_portfolio).show()

        elif option == 7:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            covariance_matrix_portfolio(spark, df_portfolio, id_portfolio).show()

        elif option == 8:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            train_portfolio(spark, df_portfolio, id_portfolio)

        elif option == 9:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            predict_portfolio(spark, df_portfolio, id_portfolio)

        elif option == 10:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            train_portfolio_sim(spark, df_portfolio, id_portfolio)

        elif option == 11:
            df_portfolio = read_portfolio_list(spark)
            df_portfolio.orderBy("ID").show(truncate=False)
            id_portfolio = int(input("Please, input a portfolio ID: "))
            num_days = int(input("Please, enter the number of days: "))
            num_traces = int(input("Please, enter the number of traces: "))
            simulate_portfolio(spark, df_portfolio, id_portfolio, num_days, num_traces)

        elif option == 12:
            try:
                read_stock_log(spark).orderBy("Stock").show()
            except (TypeError, AttributeError):
                print("Empty data, try option -> 1. Download stock data.\n")


if __name__ == "__main__":
    main()
