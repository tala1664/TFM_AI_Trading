import yfinance as yf
from pyspark.sql import SparkSession

stock = "GOOGL"

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

data = yf.download(stock, period="1d", interval="1m")

data['DateTime'] = data.index

df = spark.createDataFrame(data)
df.show(10, False)

df.write.format("parquet").save("../data/" + stock + ".parquet")
