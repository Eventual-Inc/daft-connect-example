def main():
    # start Daft Connect (spark connect compatible server)
    handle = connect_start("sc://0.0.0.0:15002")

    # The port the daft connect server is running on
    port = handle.port()
    assert port == 15002

    spark = SparkSession.builder \
        .remote(f"sc://localhost:15002") \
        .appName("DaftPySparkExample") \
        .getOrCreate()


    # Create a simple Spark DataFrame with a range
    df = spark.range(1, 5)
    
    # Show the DataFrame
    print("Original DataFrame:")
    print(df.toPandas())


if __name__ == "__main__":
    import daft
    from daft import col
    from daft.daft import connect_start

    # Connect PySpark to the Daft Connect server
    from pyspark.sql import SparkSession
    main()
