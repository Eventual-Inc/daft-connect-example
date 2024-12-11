def main():
    # start Daft Connect (spark-connect compatible server)
    handle = connect_start("sc://0.0.0.0:15002")

    # create a Spark Connect client connected to the Daft Connect server
    spark = SparkSession.builder \
        .remote(f"sc://localhost:15002") \
        .appName("DaftPySparkExample") \
        .getOrCreate()

    print()

    # Create a simple Spark DataFrame with a range
    df = spark.range(1, 5)


    # Show the DataFrame
    print("Original DataFrame:")
    print(df)

    print()

    # Show the frame as a pandas DataFrame
    pandas_df = df.toPandas()
    print("Pandas DataFrame:")
    print(pandas_df)

    # shutdown the daft-connect server
    handle.shutdown()


if __name__ == "__main__":
    import daft
    from daft import col
    from daft.daft import connect_start

    # Connect PySpark to the Daft Connect server
    from pyspark.sql import SparkSession
    main()
