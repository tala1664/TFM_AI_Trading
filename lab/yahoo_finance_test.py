import yfinance as yf
from pyspark.sql import SparkSession


# https://pypi.org/project/yfinance/

def download_market_data(stock, period, interval):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()

    data = yf.download(stock, period="max", interval="1m")

    data['DateTime'] = data.index

    df = spark.createDataFrame(data)
    return df


def write_market_data(stock, period, interval):
    df.write.format("parquet").mode("overwrite").save("../data/" + stock +
                                                      "_period=" + period +
                                                      "_interval=" + interval +
                                                      ".parquet")


df = download_market_data("GOOGL", "max", "1d")
write_market_data("GOOGL", "max", "1d")
