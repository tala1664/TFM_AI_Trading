import yfinance as yf
import pyspark.sql.functions as f
import numpy


def download_stock_data(spark, stock, period, interval):
    print("Downloading " + stock +
          " stock data. Period: " + period +
          ". Interval: " + interval + ".")

    data = yf.download(stock, period=period, interval=interval)
    data['DateTime'] = data.index

    dataframe = spark.createDataFrame(data)

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
                                                             "dd/MM/yyyy hh:mm")).otherwise(
                                            df.Last_Update))
            print("Log Updated " + stock +
                  "_period=" + period +
                  "_interval=" + interval)

        else:
            new_row = spark.createDataFrame(
                [[stock, period, interval]],
                ["Stock", "Period", "Interval"])
            new_row = new_row.withColumn("Last_Update", f.date_format(f.current_timestamp(), "dd/MM/yyyy hh:mm"))
            df_to_write = df.union(new_row)

            print("Log Created " + stock +
                  "_period=" + period +
                  "_interval=" + interval)

    except:
        df_to_write = spark.createDataFrame(
            [[stock, period, interval]],
            ["Stock", "Period", "Interval"])

        df_to_write = df_to_write.withColumn("Last_Update", f.date_format(f.current_timestamp(), "dd/MM/yyyy hh:mm"))
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
