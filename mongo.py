from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, lit, struct, collect_list, to_json, collect_set,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from functools import reduce

host = "localhost"
port = 9092
checkpoint_base = "/tmp/kafka-checkpoint"



spark = SparkSession.Builder()\
            .appName("kafka_2_mongodb")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1")\
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")\
            .config("spark.mongodb.connection.uri", "mongodb+srv://ductindongthap:123124@sparkstream.blgujkw.mongodb.net/?retryWrites=true&w=majority&appName=SparkStream").getOrCreate()


data_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("windows", ArrayType(StructType([
        StructField("window", StringType(), False),
        StructField("zscore_price", StringType(), False)
    ])), False),
])


data_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", f"{host}:{port}") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "btc-price") \
        .load()

df_data = data_stream\
        .selectExpr("CAST(value AS STRING) as json")\
        .withColumn("value", from_json("json", data_schema))\
        .select("value.*")\
        


df = df_data\
            .withColumn("wd", explode(col("windows")))\
            .selectExpr(
                            "timestamp",
                            "symbol",
                            "wd.window AS window",
                            "wd.zscore_price AS zscore_price"
                        )

windows = [
    '30s', '1m', '5m', '15m', '30m', '1h'
]



df_by_window = [
    df.filter(df.window==window_key) for window_key in windows
]


def create_writer(df, window):
    return (df.writeStream
    .format("mongodb")
    .option("checkpointLocation", f"/tmp/kafka-checkpoint/{window}")
    .option("forceDeleteTempCheckpointLocation", "true")
    .option("spark.mongodb.database", "sparklab04")
    .option("spark.mongodb.collection", f"btc-price-zscore-{window}")
    .outputMode("append")
    ).start().awaitTermination()
    

writers = { window_key : create_writer(df_, window_key) for window_key, df_ in zip(windows, df_by_window)}

