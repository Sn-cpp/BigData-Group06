from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

host = "localhost"
port = 9092

#Spark init
spark = SparkSession.Builder()\
            .appName("btcStreaming")\
            .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")\
            .getOrCreate()
            # .config("spark.driver.memory", "4g")\
            # .config("spark.executor.memory", "4g")\


#Create and connect subscriber to 'btc-price' topic
dataStream = spark.readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", f"{host}:{port}")\
            .option("failOnDataLoss", "false")\
            .option("subscribe", "btc-price")\
            .load()

#Format of incoming JSON object
schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])

#Parse bytes to JSON object
df_json = dataStream.selectExpr('CAST(value AS STRING)')

#Parse JSON object to columns
raw_df = df_json.withColumn("value", from_json(df_json["value"], schema))\
            .select("value.*")\
            .withColumn("price", col("price").cast(DoubleType()))\

#Handle late data        
df = raw_df.withWatermark("timestamp", "10 seconds")\





window30s = df.groupBy(window(col("timestamp"), "30 seconds"))\
              .agg(
                    avg(col("price")).alias("avg_price"),
                    stddev(col("price")).alias("std_price")
              )\
              .selectExpr(" '30s' AS window", "avg_price", "std_price")


writer = window30s.writeStream\
            .outputMode("complete")\
            .format("console")\
            .option("truncate", "false")\
            .start()

writer.awaitTermination()

#python Lab04/main.py