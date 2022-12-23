import yfinance as yf


def download_stock_data(spark, stock, period, interval):
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


def write_stock_data(dataframe, stock, period, interval):
    dataframe.write.format("parquet").mode("overwrite").save("../data/" + stock +
                                                             "_period=" + period +
                                                             "_interval=" + interval +
                                                             ".parquet")


def read_stock_data(spark, stock, period, interval):
    return spark.read.load("../data/" + stock +
                           "_period=" + period +
                           "_interval=" + interval +
                           ".parquet")
