import os
import sys
from io_stockdata.io_stockdata import download_stock_data, write_stock_data, read_stock_data, read_stock_log
from display.display_utils import display_graph
from pyspark.sql import SparkSession


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
                           "2. Show stock graph. \n"))

        if option == 1:

            stk = input("Please, input a valid stock name: ").upper()
            period = get_valid_period()
            interval = get_valid_interval()
            df = download_stock_data(spark, stk, period, interval)
            write_stock_data(spark, df, stk, period, interval)

        elif option == 2:
            try:
                read_stock_log(spark).show()
                stk = input("Please, input a valid stock name: ").upper()
                period = get_valid_period()
                interval = get_valid_interval()
                df = read_stock_data(spark, stk, period, interval)
                display_graph(df, "DateTime", "Close", stk)
            except:
                print("Empty data, try option -> 1. Download stock data.\n")


if __name__ == "__main__":
    main()
