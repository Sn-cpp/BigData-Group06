from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, lit, struct, collect_list, to_json, equal_null
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from functools import reduce

host = "localhost"
port = 9092

spark = SparkSession.builder \
    .appName("btc_moving_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

# read from Kafka
dataStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host}:{port}") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "btc-price-pre-moving") \
    .load()

# schema of incoming JSON
#"symbol", "window_start", "window_end", "window", "avg_price", "std_price"
schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("window", StringType(), False),
    StructField("avg_price", StringType(), False),
    StructField("std_price", StringType(), False)
])

# parse JSON and cast price
raw_df = (dataStream
    .selectExpr("CAST(value AS STRING) as json")
    .withColumn("value", from_json("json", schema))
    .select("value.*")
)

# watermark late data
df = raw_df.withWatermark("window_end", "10 seconds")

grouped_df = df.filter(col('std_price').isNotNull()).groupBy("symbol", "window_end").agg(
    collect_list(struct("window", "avg_price", "std_price")).alias("windows")
)

# Convert the list of structs to a JSON string
output_df = grouped_df.select(
    to_json(struct(
        col("window_end").alias("timestamp"),
        "symbol",
        "windows"
    )).alias("value")
)

# Write the output to Kafka
query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host}:{port}") \
    .option("topic", "btc-price-moving") \
    .option("checkpointLocation", "/tmp/kafka-to-kafka-checkpoint") \
    .outputMode("append") \
    .start()
query.awaitTermination()