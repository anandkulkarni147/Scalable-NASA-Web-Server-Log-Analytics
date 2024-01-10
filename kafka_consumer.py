from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Kafka consumer configuration file settings
broker = 'localhost:9092'
topic = 'log_topic1'
kafka_group_id = 'spark-streaming-consumer-group'

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaConsumerSparkStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# set up consumer config settings
kafka_options = {
    "kafka.bootstrap.servers": broker,
    "subscribe": topic,
    "startingOffsets": "earliest",
    "failOnDataLoss": False
}

# Read the Kafka stream using spark.readStream
df = spark.readStream.format("kafka").options(**kafka_options).load()

# Convert the binary message value to string
df = df.withColumn("value", col("value").cast("string"))

# Extract the desired fields from the log line
df = df.withColumn(
    "ip_address",
    regexp_extract(col("value"), r'^([\d.]+)', 1)
).withColumn(
    "timestamp",
    regexp_extract(col("value"), r'\[([^:]+)', 1)
).withColumn(
    "year",
    regexp_extract(col("timestamp"), r'(\d{4})', 1)
).withColumn(
    "month",
    regexp_extract(col("timestamp"), r'\w{3}', 0)
).withColumn(
    "day",
    regexp_extract(col("timestamp"), r'(\d{2})', 1)
).withColumn(
    "hour",
    regexp_extract(col("timestamp"), r'(\d{2}):(\d{2}):(\d{2})', 1)
).withColumn(
    "minute",
    regexp_extract(col("timestamp"), r'(\d{2}):(\d{2}):(\d{2})', 2)
).withColumn(
    "method",
    regexp_extract(col("value"), r'\"(\w+)', 1)
).withColumn(
    "endpoint",
    regexp_extract(col("value"), r'\"(?:[A-Z]+\s)?(\S+)', 1)
).withColumn(
    "http_version",
    regexp_extract(col("value"), r'HTTP/(\d+\.\d+)', 1)
).withColumn(
    "response_code",
    regexp_extract(col("value"), r'\s(\d{3})\s', 1)
).withColumn(
    "bytes",
    regexp_extract(col("value"), r'\s(\d+)$', 1)
)

# Select the desired columns
parsed_df = df.select("ip_address", "year", "month", "day", "method", "endpoint", "http_version", "response_code",
                      "bytes")

output_path = 'D:/Big Data/Log Analysis/output'
checkpoint_path = 'D:/Big Data/Log Analysis/checkpoint'

# Process the parquet files by writing to above paths and push to hdfs
query = parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

# 10 seconds interval
start_time = datetime.now()

while query.isActive:
    elapsed_time = datetime.now() - start_time
    if elapsed_time > timedelta(seconds=10):
        query.stop()
query.awaitTermination()