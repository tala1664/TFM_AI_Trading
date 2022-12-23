import os
import sys
from io_stockdata.io_stockdata import download_stock_data, write_stock_data, read_stock_data
from pyspark.sql import SparkSession


def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("AI_Trading") \
        .getOrCreate()

    stk = input("Please, input a valid stock name: ")

    periods = ["1d", "5d", "1mo",
               "3mo", "6mo", "1y",
               "2y", "5y", "10y",
               "ytd", "max"]  # Valid periods
    period = ""
    while period not in periods:
        period = str(input("Please, input a valid period: ") or "max")

    intervals = ["1m", "2m", "5m",
                 "15m", "30m", "60m",
                 "90m", "1h", "1d",
                 "5d", "1wk", "1mo",
                 "3mo"]  # Valid intervals
    interval = ""
    while interval not in intervals:
        interval = str(input("Please, input a valid interval: ") or "1d")

    df = download_stock_data(spark, stk, period, interval)
    write_stock_data(df, stk, period, interval)
    df = read_stock_data(spark, stk, period, interval)
    df.show(10, False)


if __name__ == "__main__":
    main()
