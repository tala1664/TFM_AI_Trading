import yfinance as yf
from pyspark.sql import SparkSession


# https://pypi.org/project/yfinance/


def download_market_data(spark, stock, period, interval):
    data = yf.download(stock, period=period, interval=interval)

    data['DateTime'] = data.index

    dataframe = spark.createDataFrame(data)
    return dataframe


def write_market_data(dataframe, stock, period, interval):
    dataframe.write.format("parquet").mode("overwrite").save("../data/" + stock +
                                                             "_period=" + period +
                                                             "_interval=" + interval +
                                                             ".parquet")


spk = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

stk_array = ["AAPL", "GOOGL", "SSNLF", "MSFT", "TSLA", "DELL", "META"]
for stk in stk_array:
    df = download_market_data(spk, stk, "max", "1d")
    write_market_data(df, stk, "max", "1d")
