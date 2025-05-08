from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, avg, stddev, lit, struct, collect_list, to_json, collect_set, explode, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from functools import reduce

host = "localhost"
port = 9092
checkpoint_base = "/tmp/kafka-checkpoint"

testers = {
    'ndtdt' : "mongodb+srv://ductindongthap:123124@sparkstream.blgujkw.mongodb.net/sparklab04?retryWrites=true&w=majority&appName=SparkStream"
}

uri = testers['ndtdt']

if __name__ == "__main__":
    # Spark initialization with MongoDB connector
    spark = SparkSession.Builder()\
                .appName("kafka_2_mongodb")\
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1")\
                .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")\
                .config("spark.mongodb.write.connection.uri", uri)\
                .config("spark.sql.session.timeZone", "UTC")\
                .getOrCreate()

    # Schema definition for incoming data from btc-price-zscore topic
    data_schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("zscores", ArrayType(StructType([
            StructField("window", StringType(), False),
            StructField("zscore_price", StringType(), False)
        ])), False),
    ])

    # Create a stream listener for the btc-price-zscore Kafka topic
    data_stream = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", f"{host}:{port}") \
            .option("failOnDataLoss", "false") \
            .option("subscribe", "btc-price-zscore") \
            .load()

    # Cast binary data to string and parse JSON according to schema
    df_data = data_stream\
            .selectExpr("CAST(value AS STRING) as json")\
            .withColumn("value", from_json("json", data_schema))\
            .select("value.*")

    # Flatten the array of zscores windows for easier processing
    df = df_data\
                .withColumn("wd", explode(col("zscores")))\
                .selectExpr(
                                "timestamp",
                                "symbol",
                                "wd.window AS window",
                                "wd.zscore_price AS zscore_price"
                        )

    # Format timestamp to the exact format required for MongoDB
    df_formatted = df.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # Batch processing function for MongoDB writes
    def write_mongo(df: DataFrame, epoch_id):
        # Get unique window names present in the current batch
        windows = df.select("window").distinct().collect()
        
        # Process each window type separately
        for row in windows:
            window = row["window"]
            
            # Filter data for current window type
            df_by_window = df.filter(df["window"] == window)

            # Write data to MongoDB with UTC timestamp preservation
            df_by_window.write\
                .format("mongodb")\
                .mode("append")\
                .option("database", "sparklab04")\
                .option("spark.mongodb.collection", f"btc-price-zscore-{window}")\
                .option("spark.mongodb.timeZone", "UTC")\
                .save()

    # Start the streaming pipeline to write data to MongoDB
    writer = df_formatted.writeStream\
                .foreachBatch(write_mongo)\
                .outputMode("append")\
                .start()\
                .awaitTermination()

    # Debug section - uncomment for console output instead of MongoDB storage
    # writer = df_formatted.writeStream\
    #             .format("console")\
    #             .outputMode("append")\
    #             .option("truncate", "false")\
    #             .start()\
    #             .awaitTermination()
