from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, lit, struct, collect_list, to_json, to_utc_timestamp, date_format, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from functools import reduce
import threading
import time

host = "localhost"
port = 9092
checkpoint_base = "/tmp/kafka-checkpoint"

def create_spark_session(app_name):
    """Create a Spark session with Kafka configuration"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .config("spark.sql.session.timeZone", "UTC")\
        .getOrCreate()
    
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    return spark

def run_pre_moving_stream():
    """First streaming job: Process raw BTC prices and compute statistics for different time windows"""
    spark = create_spark_session("btc_pre_moving_Streaming")
    
    # Read from Kafka - raw BTC price data
    dataStream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host}:{port}") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "btc-price") \
        .load()

    # Schema of incoming JSON
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("price", StringType(), False),
        StructField("timestamp", TimestampType(), False)
    ])

    # Parse JSON and cast price
    raw_df = (dataStream
        .selectExpr("CAST(value AS STRING) as json")
        .withColumn("value", from_json("json", schema))
        .select("value.*")
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("timestamp", to_utc_timestamp(col("timestamp"), "UTC"))
    )

    # Watermark late data
    df = raw_df.withWatermark("timestamp", "10 seconds")

    # Define window lengths
    window_configs = {
        "30s": "30 seconds",
        "1m": "1 minute",
        "5m": "5 minutes",
        "15m": "15 minutes",
        "30m": "30 minutes",
        "1h": "1 hour"
    }

    # Collect each windowed aggregation into a list
    windowed_dfs = []
    for name, duration in window_configs.items():
        wdf = (df
            .groupBy(
                col("symbol"),
                window(col("timestamp"), duration).alias("window")
            )
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("std_price")
            )
            # Tag which window this row came from
            .withColumn("std_price", when(col("std_price").isNull(), lit(0.0)).otherwise(col("std_price")))
            .withColumn("window_interval", lit(name))
        )
        # Flatten the window struct for easier downstream processing
        wdf = wdf.select(
            "symbol",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("window_interval").alias("window"),
            "avg_price",
            "std_price"
        )
        windowed_dfs.append(wdf)

    # Union them all into one streaming DataFrame
    union_df = reduce(lambda dfa, dfb: dfa.unionByName(dfb), windowed_dfs)
    union_df = union_df.dropDuplicates(["symbol", "window_start", "window"])
    output_df = union_df.select(
        to_json(struct(
            "symbol",
            date_format(col("window_start"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("window_start"),
            date_format(col("window_end"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("window_end"),
            "window",
            "avg_price",
            "std_price"
        )).alias("value")
    )

    # Write to intermediate Kafka topic
    writer = (output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", f"{host}:{port}")
        .option("topic", "btc-price-pre-moving")
        .option("checkpointLocation", f"{checkpoint_base}/btc-price-pre-moving-checkpoint")
        .outputMode("append")
        .start())


    return writer

def run_moving_stream():
    """Second streaming job: Group pre-processed data by window and symbol"""
    spark = create_spark_session("btc_moving_Streaming")
    
    # Read from intermediate Kafka topic
    dataStream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host}:{port}") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "btc-price-pre-moving") \
        .load()

    # Schema of incoming JSON from first stream
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("window", StringType(), False),
        StructField("avg_price", StringType(), False),
        StructField("std_price", StringType(), False)
    ])

    # Parse JSON
    raw_df = (dataStream
        .selectExpr("CAST(value AS STRING) as json")
        .withColumn("value", from_json("json", schema))
        .select("value.*")
        .withColumn("window_start", to_utc_timestamp(col("window_start"), "UTC"))
        .withColumn("window_end", to_utc_timestamp(col("window_end"), "UTC"))
    )

    # Watermark late data for second stream
    df = raw_df.withWatermark("window_start", "10 seconds")

    # Group by symbol and window_end, collecting window statistics
    grouped_df = df.filter(col('std_price').isNotNull()).groupBy("symbol", "window_start").agg(
        collect_list(struct("window", "avg_price", "std_price")).alias("windows")
    )

    # Convert the list of structs to a JSON string
    output_df = grouped_df.select(
        to_json(struct(
            date_format(col("window_start"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("timestamp"),
            "symbol",
            "windows"
        )).alias("value")
    )


    # Write the output to final Kafka topic
    writer = output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host}:{port}") \
        .option("topic", "btc-price-moving") \
        .option("checkpointLocation", f"{checkpoint_base}/btc-price-moving-checkpoint") \
        .outputMode("append") \
        .start()
    
    return writer

if __name__ == "__main__":
    print("Starting BTC price streaming pipeline with two-phase processing...")
    
    # Start first stream (pre-moving)
    pre_moving_query = run_pre_moving_stream()
    print("Pre-moving stream started")
    
    # Give the first stream a moment to initialize
    time.sleep(5)
    
    # Start second stream (moving)
    moving_query = run_moving_stream()
    print("Moving stream started")
    
    # Wait for both streams to terminate
    try:
        pre_moving_query.awaitTermination()
        moving_query.awaitTermination()
    except KeyboardInterrupt:
        print("\nShutting down streaming pipeline...")
        # Both streams will terminate when the process is interrupted