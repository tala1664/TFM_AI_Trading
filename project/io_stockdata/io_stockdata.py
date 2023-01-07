import yfinance as yf
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import numpy


def download_stock_data(spark, stock, period, interval):
    print("Downloading " + stock +
          " stock data. Period: " + period +
          ". Interval: " + interval + ".")

    data = yf.download(stock, period=period, interval=interval)
    data['DateTime'] = data.index

    dataframe = spark.createDataFrame(data)

    dataframe = dataframe.withColumn("ID", f.monotonically_increasing_id())

    dataframe = dataframe.withColumn("ID2", f.when(f.col("ID") % 2 > 0, f.col("ID") - 1).otherwise(f.col("ID")))
    dataframe = dataframe.withColumn("ID3", f.when(f.col("ID") % 2 > 0, f.col("ID") + 1).otherwise(f.col("ID")))

    w1 = Window().partitionBy("ID2").orderBy("ID")

    dataframe = dataframe.withColumn("Performance1",
                                     (f.col("close") - f.lag("close", 1).over(w1)) / f.lag("close", 1).over(w1))

    w2 = Window().partitionBy("ID3").orderBy("ID")

    dataframe = dataframe.withColumn("Performance2",
                                     (f.col("close") - f.lag("close", 1).over(w2)) / f.lag("close", 1).over(w2))

    dataframe = dataframe.withColumn("Performance", f.when(f.col("Performance1").isNotNull(),
                                                           f.col("Performance1")).otherwise(f.col("Performance2")))

    dataframe = dataframe.withColumn("Performance", f.round("Performance", 5))

    columns_to_drop = ["ID2", "ID3", "Performance1", "Performance2"]
    dataframe = dataframe.drop(*columns_to_drop)

    return dataframe


def write_stock_data(spark, dataframe, stock, period, interval):
    dataframe.write.format("parquet").mode("overwrite").save("../data/stocks/" + stock +
                                                             "_period=" + period +
                                                             "_interval=" + interval +
                                                             ".parquet")
    write_stock_log(spark, stock, period, interval)


def read_stock_data(spark, stock, period, interval):
    return spark.read.load("../data/stocks/" + stock +
                           "_period=" + period +
                           "_interval=" + interval +
                           ".parquet")


def write_stock_log(spark, stock, period, interval):
    try:
        df = spark.read.load("../data/stock_inventory.parquet")
        if df.filter(df.Stock == stock).count() > 0:
            df_to_write = df.withColumn("Last_Update",
                                        f.when(df.Stock == stock,
                                               f.date_format(f.current_timestamp(),
                                                             "dd/MM/yyyy HH:mm")).otherwise(
                                            df.Last_Update))
            print("Log Updated " + stock +
                  "_period=" + period +
                  "_interval=" + interval + "\n")

        else:
            new_row = spark.createDataFrame(
                [[stock, period, interval]],
                ["Stock", "Period", "Interval"])
            new_row = new_row.withColumn("Last_Update", f.date_format(f.current_timestamp(), "dd/MM/yyyy HH:mm"))
            df_to_write = df.union(new_row)

            print("Log Created " + stock +
                  "_period=" + period +
                  "_interval=" + interval + "\n")

    except:
        df_to_write = spark.createDataFrame(
            [[stock, period, interval]],
            ["Stock", "Period", "Interval"])

        df_to_write = df_to_write.withColumn("Last_Update", f.date_format(f.current_timestamp(), "dd/MM/yyyy HH:mm"))
        print("Log Created")

    df_to_write.write.format("parquet").mode("overwrite").save("../data/temp/stock_inventory.parquet")
    df = spark.read.load("../data/temp/stock_inventory.parquet")
    df.write.format("parquet").mode("overwrite").save("../data/stock_inventory.parquet")


def read_stock_log(spark):
    return spark.read.load("../data/stock_inventory.parquet")


def write_portfolio_list(spark, stock_list, num_shares_list, min_date, max_date):
    try:
        df = spark.read.load("../data/portfolio_inventory.parquet")
        id_count = df.select("ID").rdd.max()[0] + 1
        new_row = spark.createDataFrame(
            [[id_count, stock_list, num_shares_list, min_date, max_date]],
            ["ID", "Stock_List", "Number_Shares_List", "Min_Date", "Max_Date"])
        df_to_write = df.union(new_row)

    except:
        df_to_write = spark.createDataFrame(
            [[1, stock_list, num_shares_list, min_date, max_date]],
            ["ID", "Stock_List", "Number_Shares_List", "Min_Date", "Max_Date"])

    df_to_write.write.format("parquet").mode("overwrite").save("../data/temp/portfolio_inventory.parquet")
    df = spark.read.load("../data/temp/portfolio_inventory.parquet")
    df.write.format("parquet").mode("overwrite").save("../data/portfolio_inventory.parquet")


def read_portfolio_list(spark):
    return spark.read.load("../data/portfolio_inventory.parquet")
