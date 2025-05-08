from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, struct, to_json, collect_list, when, to_utc_timestamp, lit, date_format, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType

host = "localhost"
port = 9092
checkpoint_base = "/tmp/kafka-checkpoint"

def create_spark_session(app_name) -> SparkSession:
    """Create a Spark session with Kafka configuration"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .config("spark.sql.session.timeZone", "UTC")\
        .getOrCreate()
    
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    return spark

if __name__ == "__main__":
    spark = create_spark_session("zscore")

    # Schema for data from the topic 'btc-price-moving'
    stats_schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("windows", ArrayType(StructType([
            StructField("window", StringType(), False),
            StructField("avg_price", StringType(), False),
            StructField("std_price", StringType(), False)
        ])), False)
    ])
    stats_stream = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", f"{host}:{port}") \
            .option("failOnDataLoss", "false") \
            .option("subscribe", "btc-price-moving") \
            .load()
    df_stats = stats_stream\
            .selectExpr("CAST(value AS STRING) as json")\
            .withColumn("value", from_json("json", stats_schema))\
            .select(
                col("value.timestamp").alias("stats_timestamp"),
                col("value.symbol").alias("stats_symbol"),
                "value.windows")\
            .withColumn("windows", expr("""
                transform(windows, x -> struct(
                    x.window as window,
                    cast(x.avg_price as double) as avg_price,
                    cast(x.std_price as double) as std_price
                ))"""))\
            .withColumn("stats_timestamp", to_utc_timestamp(col("stats_timestamp"), "UTC"))

    # Schema for data from the topic 'btc-price'
    data_schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("price", StringType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    data_stream = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", f"{host}:{port}") \
            .option("failOnDataLoss", "false") \
            .option("subscribe", "btc-price") \
            .load()
    df_data = data_stream\
            .selectExpr("CAST(value AS STRING) as json")\
            .withColumn("value", from_json("json", data_schema))\
            .select(
                col("value.timestamp").alias("data_timestamp"),
                col("value.symbol").alias("data_symbol"),
                col("value.price")
            )\
            .withColumn("price", col("price").cast(DoubleType()))\
            .withColumn("data_timestamp", to_utc_timestamp(col("data_timestamp"), "UTC"))

    # # Apply watermark to handle late data
    df_data = df_data.withWatermark("data_timestamp", "10 seconds")
    df_stats = df_stats.withWatermark("stats_timestamp", "10 seconds")

    # Explode the windows array first to access each window's configuration
    df_stats_exploded = df_stats.withColumn("window_item", explode(col("windows")))

    # # Join the two streams with more flexible time-based join conditions
    df_joined = df_data.join(
        df_stats_exploded,
        (df_data.data_symbol == df_stats_exploded.stats_symbol) &
        expr("""
            CASE 
                WHEN window_item.window = '30s' THEN 
                    data_timestamp >= stats_timestamp AND data_timestamp <= stats_timestamp + interval 30 seconds
                WHEN window_item.window = '1m' THEN 
                    data_timestamp >= stats_timestamp AND data_timestamp <= stats_timestamp + interval 1 minute
                WHEN window_item.window = '5m' THEN 
                    data_timestamp >= stats_timestamp AND data_timestamp <= stats_timestamp + interval 5 minutes
                WHEN window_item.window = '15m' THEN 
                    data_timestamp >= stats_timestamp AND data_timestamp <= stats_timestamp + interval 15 minutes
                WHEN window_item.window = '30m' THEN 
                    data_timestamp >= stats_timestamp AND data_timestamp <= stats_timestamp + interval 30 minutes
                WHEN window_item.window = '1h' THEN 
                    data_timestamp >= stats_timestamp AND data_timestamp <= stats_timestamp + interval 1 hour
                ELSE FALSE
            END
        """),
        "inner"
    )
    
    # Compute Z-score with the already exploded window data
    df_with_zscore = df_joined.select(
            col("data_timestamp").alias("timestamp"),
            col("data_symbol").alias("symbol"),
            col("price"),
            col("window_item.window").alias("window"),
            col("window_item.avg_price").alias("avg_price"),
            col("window_item.std_price").alias("std_price")
        ) \
        .withColumn("zscore_price",
                    when(col("std_price") != 0, (col("price") - col("avg_price")) / col("std_price"))
                    .otherwise(0.0))

    # Drop duplicates based on timestamp, symbol, and window due to the threshold can be less than 1 minute
    df_with_zscore = df_with_zscore.dropDuplicates(["timestamp", "symbol", "window"])

    # Group by timestamp and symbol
    output_df = df_with_zscore.groupBy("timestamp", "symbol").agg(
        collect_list(struct("window", "zscore_price")).alias("zscores")
    ).select(
        to_json(struct(
            date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
            col("symbol"),
            "zscores"
        )).alias("value")
    )

    # Write to the topic 'btc-price-zscore'
    writer = output_df.writeStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", f"{host}:{port}")\
            .option("topic", "btc-price-zscore")\
            .option("checkpointLocation", f"{checkpoint_base}/btc-price-zscore-checkpoint")\
            .outputMode("append")\
            .start()

    # Uncomment for debugging
    # writer = output_df.writeStream\
    #             .format("console")\
    #             .outputMode("append")\
    #             .start()\
    #             .awaitTermination()
    try:
        writer.awaitTermination()
    except KeyboardInterrupt:
        print("\nShutting down...")
