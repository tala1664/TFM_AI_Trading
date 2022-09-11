import pandas
import yfinance as yf
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

data = yf.download("SPY AAPL", period="1d", interval="1m")
print(data)
df = pandas.DataFrame(data=data)

df2 = spark.createDataFrame(df)
df2.show(10, False)