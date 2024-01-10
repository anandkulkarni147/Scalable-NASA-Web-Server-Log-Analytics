## Readme

Running the Spark code for Word Frequency

Seection A

Run as Maven build on pom.xml using Eclipse IDE
spark-submit --class 'WordFreq' --master local\[2\] '/Users/pralhad/eclipse-workspace/SparkWordFreq/target/SparkWordFreq-0.0.1-SNAPSHOT.jar'

Section B
1)NASA Log Analytics
	run log_analysis_NASA notebook using colab please keep the NASA_LOGS dataset in the same directory as the file
	(this has the code from the article as well as 3.1 and 3.2 implemented)

2) Kafka Producer Consumer 

	Start the zookeeper, kafka server and spark using below commands (go into respective installation directories)
	
	i) .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
	ii) .\bin\windows\kafka-server-start.bat .\config\server.properties
	iii) spark-shell
	iv) Start HDFS to push parquet files

	a) run python kafka_producer.py using python interpreter keep the dataset extracted in the same directory
	change the topic name if required
	
	b) run bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 
	"D:\Big Data\Log Analysis\kafka_consumer.py"
	
	c) Perform EDA and Clustering on parquet files from consumer by running log_analysis_parquet
	
	d) run parquet_to_hadoop.py to push parquet files after EDA

