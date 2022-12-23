import yfinance as yf
import os
import sys
from pyspark.sql import SparkSession


# https://pypi.org/project/yfinance/


def download_market_data(spark, stock, period, interval):
    print("Downloading " + stock +
          " market data. Period: " + period +
          ". Interval: " + interval + ".")
    try:
        data = yf.download(stock, period=period, interval=interval)
        data['DateTime'] = data.index

        dataframe = spark.createDataFrame(data)

    except Exception:
        raise Exception("Symbol " + stock + " not found")

    return dataframe


def write_market_data(dataframe, stock, period, interval):
    dataframe.write.format("parquet").mode("overwrite").save("../data/" + stock +
                                                             "_period=" + period +
                                                             "_interval=" + interval +
                                                             ".parquet")


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spk = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

stk_array = [["AAPL", "max", "1d"],
             ["GOOGL", "max", "1d"],
             ["SSNLF", "max", "1d"],
             ["MSFT", "max", "1d"],
             ["TSLA", "max", "1d"],
             ["DELL", "max", "1d"],
             ["META", "max", "1d"]]

for stk in stk_array:
    df = download_market_data(spk, stk[0], stk[1], stk[2])
    write_market_data(df, stk[0], stk[1], stk[2])
