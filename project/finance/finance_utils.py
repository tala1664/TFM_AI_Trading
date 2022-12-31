def correlation_matrix_portfolio(spark, stock_list, min_date, max_date):
    matrix = []


    for stock in stock_list:
        matrix.append([stock])
        for stock2 in stock_list:
            print(stock + " " + stock2)
    print(matrix)

    # stock_list.insert(0, "Correlations")
    # df = spark.createDataFrame([], stock_list)
