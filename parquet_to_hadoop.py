from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ParquetFileHDFS').getOrCreate()
logs_df = spark.read.parquet('*.parquet')
logs_df.write.parquet('hdfs://localhost:9000/*.parquet')
