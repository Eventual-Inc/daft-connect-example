def main():
    # start Daft Connect (spark-connect compatible server)
    handle = connect_start("sc://0.0.0.0:15002")

    # create a Spark Connect client connected to the Daft Connect server
    spark = SparkSession.builder \
        .remote(f"sc://localhost:15002") \
        .appName("DaftPySparkExample") \
        .getOrCreate()

    print()

    # Read mvp.parquet from the local filesystem
    df = spark.read.parquet("mvp.parquet")

    # Show the DataFrame
    print("Original DataFrame:")
    print(df)

    print()
    
    # Show some basic operations on the DataFrame
    print("Sum of column 'a':", df.agg({"a": "sum"}).collect()[0][0])

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
