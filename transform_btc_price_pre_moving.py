from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, lit, struct, collect_list, to_json, equal_null
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from functools import reduce

host = "localhost"
port = 9092

spark = SparkSession.builder \
    .appName("btc_pre_moving_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

# read from Kafka
dataStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host}:{port}") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "btc-price") \
    .load()

# schema of incoming JSON
schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])

# parse JSON and cast price
raw_df = (dataStream
    .selectExpr("CAST(value AS STRING) as json")
    .withColumn("value", from_json("json", schema))
    .select("value.*")
    .withColumn("price", col("price").cast(DoubleType()))
)

# watermark late data
df = raw_df.withWatermark("timestamp", "10 seconds")

# define window lengths
window_configs = {
    "30s": "30 seconds",
    "1m": "1 minute",
    #"5m": "5 minutes",
    #"15m": "15 minutes",
    #"30m": "30 minutes",
    #"1h": "1 hour"
}

# collect each windowed aggregation into a list
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
        # tag which window this row came from
        .withColumn("window_interval", lit(name))
    )
    # flatten the window struct for easier downstream processing
    wdf = wdf.select(
        "symbol",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("window_interval").alias("window"),
        "avg_price",
        "std_price"
    )
    windowed_dfs.append(wdf)

# union them all into one streaming DataFrame
union_df = reduce(lambda dfa, dfb: dfa.unionByName(dfb), windowed_dfs)
output_df = union_df.select( 
    to_json(struct("symbol", "window_start", "window_end", "window", "avg_price", "std_price")).alias("value")
)

writer = (output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "btc-price-pre-moving")
    .option("checkpointLocation", "/tmp/btc-price-moving-checkpoint")
    .outputMode("append")
    .start())

writer.awaitTermination()