def main():
    # start Daft Connect (spark-connect compatible server)
    handle = connect_start("sc://0.0.0.0:15002")

    # create a Spark Connect client connected to the Daft Connect server
    spark = SparkSession.builder \
        .remote(f"sc://localhost:15002") \
        .appName("DaftPySparkExample") \
        .getOrCreate()

    print()

    data = [[2021, "test", "Albany", "M", 42]]
    columns = ["Year", "First_Name", "County", "Sex", "Count"]

    df1 = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
    df1.show()

    # shutdown the daft-connect server
    handle.shutdown()


if __name__ == "__main__":
    import daft
    from daft import col
    from daft.daft import connect_start

    # Connect PySpark to the Daft Connect server
    from pyspark.sql import SparkSession
    main()
