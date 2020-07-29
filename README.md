# Udacity Data Streaming Nanodegree Project

## Project Objective

In this project a stream processing application is built which sources data from San Francisco crime incidents and provides data analysis using Spark Structured Streaming. 

## Steps to Execute the Project

### 1. Start Zookeeper and Kafka Server
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### 2. Install requirements
```
./start.sh
```
### 3. Build Kafka Producer
```
python producer_server.py
```
and 
```
python kafka_server.py
```

### 4. Build Kafka Consumer

In order to build a Kafka Consumer in the console
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:<your-port-number> --topic <your-topic-name> --from-beginning
```

The expected output is

![Image of producer-output](https://github.com/MRazaKazmi/kafka-spark-streaming-san-francisco-crime-stats/blob/master/console-consumer-output.png)

Inorder to implement a Kafka consumer server
```
python consumer_server.py
```
The expected output is 

![Image of producer-output](https://github.com/MRazaKazmi/kafka-spark-streaming-san-francisco-crime-stats/blob/master/consumer-output.png)


### 5. Run Spark Streaming Application 
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py
```
The expected Spark UI is 

![Image of producer-output](https://github.com/MRazaKazmi/kafka-spark-streaming-san-francisco-crime-stats/blob/master/spark-ui.png)

The expected progress report is 

![Image of producer-output](https://github.com/MRazaKazmi/kafka-spark-streaming-san-francisco-crime-stats/blob/master/progress-report.png)

## Additional Questions

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Changing the SparkSession property parameters affected the data's throughput and latency. Increasing, for example, the value of 'maxOffsetsPerTrigger' increased the rows processed per second. However, it increased latency as well. 

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal

'maxOffsetsPerTrigger' provides best performance boost with a value of 200 followed by 'maxRatePerPartition' with a value of 10. 'spark.default.parallelism' also provides a fair amount of performance boost with a recommended value of 20 and 'spark.sql.shuffle.partitions' with a value of 9.

