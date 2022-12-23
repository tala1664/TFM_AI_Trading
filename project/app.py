import os
import sys
from io_stockdata.io_stockdata import download_stock_data, write_stock_data, read_stock_data
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

    option = int(input("Select a valid option: \n" +
                       "1. Download stock data.\n" +
                       "2. Show stock graph. \n"))

    if option == 1:

        stk = input("Please, input a valid stock name: ")
        period = get_valid_period()
        interval = get_valid_interval()
        df = download_stock_data(spark, stk, period, interval)
        write_stock_data(df, stk, period, interval)

    elif option == 2:

        stk = input("Please, input a valid stock name: ")
        period = get_valid_period()
        interval = get_valid_interval()
        df = read_stock_data(spark, stk, period, interval)
        display_graph(df, "DateTime", "Close")


if __name__ == "__main__":
    main()
