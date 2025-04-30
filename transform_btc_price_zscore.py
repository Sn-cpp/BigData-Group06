from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, lit, struct, collect_list, to_json, collect_set
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from functools import reduce

host = "localhost"
port = 9092

def create_spark_session(app_name) -> SparkSession:
    """Create a Spark session with Kafka configuration"""
    spark = SparkSession.Builder() \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    return spark





if __name__ == "__main__":
    spark = create_spark_session("zscore")


    #----------------------------------------------------------------------------------
    stats_schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("windows", StringType(), False)
    ])
    stats_stream = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", f"{host}:{port}") \
            .option("failOnDataLoss", "false") \
            .option("subscribe", "btc-price-moving") \
            .load()
    df_stats = stats_stream\
            .selectExpr("CAST(value AS STRING) as json")\
            .withColumn("value", from_json("json", stats_schema))\
            .select("value.*")
    


    #----------------------------------------------------------------------------------
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
            .select("value.*")\
            .withColumn("price", col("price").cast(DoubleType()))
    #----------------------------------------------------------------------------------

    # df_data = df_data.withWatermark("timestamp", "10 minutes")
    # df_stats = df_stats.withWatermark("timestamp", "10 minutes")




    df_joined = df_data.join(df_stats, on= [df_data.timestamp==df_stats.timestamp, df_data.symbol==df_stats.symbol])





    writer = df_stats.writeStream\
            .format("console")\
            .outputMode("append")\
            .option("truncate", "false")\
            .start()




    try:
        writer.awaitTermination()
    except KeyboardInterrupt:
        print("\nShutting down...")



